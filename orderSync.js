// orderSync.js
const Shopify = require('shopify-api-node');
const soap = require('strong-soap').soap;
const config = require('./config.json');
const {
  mainLogger,
  orderSyncLogger,
  withRetry,
  sendLogEmail
} = require('./utils');

// Configuration from config.json
const SMIFFYS_API_KEY = config.smiffys.apiKey;
const SMIFFYS_CLIENT_ID = config.smiffys.clientId;
const SMIFFYS_ORDERS_URL = config.smiffys.ordersUrl;
const SHOPIFY_ACCESS_TOKEN = config.shopify.accessToken;
const SHOPIFY_STORE_NAME = config.shopify.storeName;

// Validate configuration
if (!SMIFFYS_ORDERS_URL) {
  throw new Error('SMIFFYS_ORDERS_URL is not defined in config.json. Please set smiffys.ordersUrl to the Smiffys orders API WSDL URL (e.g., "https://webservices.smiffys.com/services/orders.asmx?wsdl").');
}

// Timestamp for log filenames
const timestamp = new Date().toISOString().replace(/:/g, '-').split('.')[0];

// Shopify API setup
const shopify = new Shopify({
  shopName: SHOPIFY_STORE_NAME,
  accessToken: SHOPIFY_ACCESS_TOKEN,
  apiVersion: '2025-01',
  autoLimit: { calls: 1, interval: 1000, bucketSize: 40 }
});

// Singleton for Smiffys orders SOAP client
let smiffysOrdersClient = null;

async function getSmiffysOrdersClient() {
  if (!smiffysOrdersClient) {
    smiffysOrdersClient = await new Promise((resolve, reject) => {
      soap.createClient(SMIFFYS_ORDERS_URL, {}, (err, client) => {
        if (err) {
          orderSyncLogger.error(`Failed to create SOAP client for Smiffys orders API at ${SMIFFYS_ORDERS_URL}: ${err.message}`);
          return reject(err);
        }
        orderSyncLogger.info('Successfully created SOAP client for Smiffys orders API');
        resolve(client);
      });
    });
  }
  return smiffysOrdersClient;
}

// Fetch Shopify orders that need to be sent to Smiffys
async function fetchShopifyOrders() {
  const start = Date.now();
  try {
    const orders = await withRetry(() =>
      shopify.order.list({
        status: 'open',
        fulfillment_status: 'unfulfilled',
        limit: 50,
        fields: 'id,order_number,customer,line_items,shipping_address,billing_address,total_price,created_at,tags'
      })
    );
    const unsentOrders = orders.filter(order => !order.tags.includes('sent_to_smiffys'));
    orderSyncLogger.info(`Fetched ${unsentOrders.length} unfulfilled orders from Shopify (out of ${orders.length} total), Took: ${(Date.now() - start) / 1000}s`);
    return unsentOrders;
  } catch (error) {
    orderSyncLogger.error(`Failed to fetch Shopify orders: ${error.message}`);
    throw error;
  }
}

// Transform Shopify order to Smiffys XML format
function transformOrderToSmiffys(shopifyOrder) {
  const shippingAddress = shopifyOrder.shipping_address || {};

  const defaultAddress = {
    address1: shippingAddress.address1 || '123 Test Street',
    address2: shippingAddress.address2 || 'Apartment 4B',
    city: shippingAddress.city || 'Test Town',
    zip: shippingAddress.zip || 'TE1 1ST',
    province: shippingAddress.province || 'TEST',
    country: shippingAddress.country || 'United Kingdom',
    country_code: shippingAddress.country_code || 'GB'
  };

  const recipientName = `${shopifyOrder.customer?.first_name || 'Unknown'} ${shopifyOrder.customer?.last_name || 'Unknown'}`.trim();
  const customerEmail = shopifyOrder.customer?.email || 'no-email@provided.com';

  const orderXml = `
    <orderXml>
      <Order>
        <YourOrderNumber>${shopifyOrder.order_number}</YourOrderNumber>
        <Recipient>
          <Email>${customerEmail}</Email>
          <Telephone>${shopifyOrder.customer?.phone || ''}</Telephone>
          <Recipient>${recipientName}</Recipient>
          <AddressLine>${defaultAddress.address1}</AddressLine>
          <AddressLine>${defaultAddress.address2}</AddressLine>
          <City>${defaultAddress.city}</City>
          <PostCode>${defaultAddress.zip}</PostCode>
          <County>${defaultAddress.province}</County>
          <CountryCode>${defaultAddress.country_code}</CountryCode>
        </Recipient>
        <DeliveryCode>ZZRML_1SGN</DeliveryCode>
        <Lines>
          ${shopifyOrder.line_items.map(item => `
            <Line>
              <ProductCode>${item.sku || ''}</ProductCode>
              <ProductQuantity>${item.quantity}</ProductQuantity>
            </Line>
          `).join('')}
        </Lines>
      </Order>
    </orderXml>
  `;

  const cleanOrderXml = orderXml.replace(/\s+/g, ' ').trim();
  orderSyncLogger.info(`Transformed order ${shopifyOrder.order_number} to Smiffys XML: ${cleanOrderXml}`);
  return cleanOrderXml;
}

// Submit order to Smiffys via SOAP API
async function submitOrderToSmiffys(orderXml) {
  const start = Date.now();
  try {
    const client = await getSmiffysOrdersClient();

    const args = {
      apiKey: SMIFFYS_API_KEY,
      clientID: SMIFFYS_CLIENT_ID,
      orderXml: orderXml
    };

    orderSyncLogger.info(`Submitting order to Smiffys with args: ${JSON.stringify({ apiKey: '****', clientID: SMIFFYS_CLIENT_ID, orderXml })}`);

    const result = await new Promise((resolve, reject) => {
      client.SubmitOrder(args, (err, result, rawResponse, soapHeader, rawRequest) => {
        if (err) {
          orderSyncLogger.error(`SOAP Fault: ${JSON.stringify(err.root?.Envelope?.Body?.Fault || err.message)}`);
          orderSyncLogger.debug(`Raw SOAP Request: ${rawRequest}`);
          orderSyncLogger.debug(`Raw SOAP Response: ${rawResponse}`);
          return reject(new Error(`SOAP Error: ${err.message}`));
        }
        orderSyncLogger.debug(`Raw SOAP Request: ${rawRequest}`);
        orderSyncLogger.debug(`Raw SOAP Response: ${rawResponse}`);
        resolve(result);
      });
    });

    const returnValue = result?.SubmitOrderResult?.ReturnValue;
    if (returnValue !== 'Success' && returnValue !== 'NotEnabled') {
      throw new Error(`Smiffys API returned an error: ${returnValue} (ReturnCode: ${result.SubmitOrderResult.ReturnCode})`);
    }

    orderSyncLogger.info(`Successfully submitted order to Smiffys (or NotEnabled in test environment). Result: ${JSON.stringify(result)}, Took: ${(Date.now() - start) / 1000}s`);
    return result;
  } catch (error) {
    orderSyncLogger.error(`Failed to submit order to Smiffys: ${error.message}`);
    throw error;
  }
}

// Update Shopify order after successful submission to Smiffys
async function updateShopifyOrder(orderId) {
  try {
    const order = await shopify.order.get(orderId, { fields: 'tags' });
    const currentTags = order.tags ? order.tags.split(',').map(tag => tag.trim()) : [];
    if (!currentTags.includes('sent_to_smiffys')) {
      currentTags.push('sent_to_smiffys');
      await withRetry(() =>
        shopify.order.update(orderId, {
          tags: currentTags.join(', ')
        })
      );
      orderSyncLogger.info(`Updated Shopify order ${orderId} with tag 'sent_to_smiffys'`);
    } else {
      orderSyncLogger.info(`Shopify order ${orderId} already has 'sent_to_smiffys' tag`);
    }
  } catch (error) {
    orderSyncLogger.error(`Failed to update Shopify order ${orderId}: ${error.message}`);
    throw error;
  }
}

// Mark Shopify order as fulfilled using the modern FulfillmentOrder API
async function fulfillShopifyOrder(orderId, lineItems) {
  try {
    const fulfillmentOrders = await withRetry(() =>
      shopify.fulfillmentOrder.list(orderId)
    );

    const fulfillmentOrder = fulfillmentOrders.find(fo => fo.status === 'open');
    if (!fulfillmentOrder) {
      throw new Error(`No open fulfillment orders found for order ${orderId}`);
    }

    const fulfillmentData = {
      fulfillment: {
        line_items_by_fulfillment_order: [
          {
            fulfillment_order_id: fulfillmentOrder.id,
            fulfillment_order_line_items: lineItems.map(item => {
              const fulfillmentOrderLineItem = fulfillmentOrder.line_items.find(li => li.sku === item.sku);
              if (!fulfillmentOrderLineItem) {
                throw new Error(`Line item with SKU ${item.sku} not found in fulfillment order ${fulfillmentOrder.id}`);
              }
              return {
                id: fulfillmentOrderLineItem.id,
                quantity: item.quantity
              };
            })
          }
        ],
        notify_customer: false,
        status: 'success'
      }
    };

    const result = await withRetry(() =>
      shopify.fulfillment.createV2(fulfillmentData)
    );
    orderSyncLogger.info(`Marked Shopify order ${orderId} as fulfilled: ${JSON.stringify(result)}`);
  } catch (error) {
    orderSyncLogger.error(`Failed to mark Shopify order ${orderId} as fulfilled: ${error.message}`);
    throw error;
  }
}

// Main order sync function
async function orderSync() {
  mainLogger.info('Starting Shopify to Smiffys order sync');
  const startTime = Date.now();
  let syncError = null;

  try {
    const shopifyOrders = await fetchShopifyOrders();
    if (shopifyOrders.length > 0) {
      mainLogger.info(`Processing ${shopifyOrders.length} orders to send to Smiffys`);
      for (const order of shopifyOrders) {
        try {
          orderSyncLogger.info(`Order details for ${order.order_number} (ID: ${order.id}): ${JSON.stringify(order, null, 2)}`);
          orderSyncLogger.info(`Processing order ${order.order_number} (ID: ${order.id})`);
          const orderXml = transformOrderToSmiffys(order);
          await submitOrderToSmiffys(orderXml);
          await updateShopifyOrder(order.id);
          await fulfillShopifyOrder(order.id, order.line_items);
          orderSyncLogger.info(`Successfully processed order ${order.order_number}`);
        } catch (error) {
          orderSyncLogger.error(`Failed to process order ${order.order_number}: ${error.message}`);
        }
      }
      mainLogger.info(`Completed order sync. Processed ${shopifyOrders.length} orders`);
    } else {
      mainLogger.info('No orders to sync to Smiffys');
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    mainLogger.info(`Order sync completed in ${duration} seconds`);
    await sendLogEmail(timestamp, duration, syncError, 'order');
  } catch (error) {
    syncError = error;
    mainLogger.error(`Order sync failed: ${error.message}`);
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    await sendLogEmail(timestamp, duration, syncError, 'order');
    throw error;
  }
}

// Run the order sync immediately
orderSync().catch(err => {
  mainLogger.error(`Unhandled error in order sync: ${err.message}`);
  process.exit(1);
});