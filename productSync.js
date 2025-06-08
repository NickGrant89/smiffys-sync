const Shopify = require('shopify-api-node');
const winston = require('winston');
const soap = require('strong-soap').soap;
const fs = require('fs').promises;
const path = require('path');
const nodemailer = require('nodemailer');
const { MongoClient } = require('mongodb');
const config = require('./config.json');
const { promisify } = require('util');
const setTimeoutPromise = promisify(setTimeout);
const Promise = require('bluebird');

// Configuration from config.json
const SMIFFYS_API_KEY = config.smiffys.apiKey;
const SMIFFYS_CLIENT_ID = config.smiffys.clientId;
const SMIFFYS_PRODUCTS_URL = config.smiffys.productsUrl;
const SMIFFYS_ORDERS_URL = config.smiffys.ordersUrl;
const SHOPIFY_ACCESS_TOKEN = config.shopify.accessToken;
const SHOPIFY_STORE_NAME = config.shopify.storeName;
const IMAGE_DIR = config.imageDir;
const MONGODB_CONNECTION_STRING = config.mongodb.connectionString;

// Email configuration from config.json
const SMTP_HOST = config.email.smtpHost;
const SMTP_PORT = config.email.smtpPort;
const SENDER_EMAIL = config.email.senderEmail;
const SENDER_PASSWORD = config.email.senderPassword;
const RECIPIENT_EMAIL = config.email.recipientEmail;
const EMAIL_SUBJECT_TEMPLATE = config.email.subject;

let collectionIds = {};

// Validate configuration
if (!SMIFFYS_ORDERS_URL) {
  throw new Error('SMIFFYS_ORDERS_URL is not defined in config.json.');
}

if (!SMTP_HOST || !SMTP_PORT || !SENDER_EMAIL || !SENDER_PASSWORD || !RECIPIENT_EMAIL || !EMAIL_SUBJECT_TEMPLATE) {
  throw new Error('Email configuration is incomplete in config.json.');
}

if (!MONGODB_CONNECTION_STRING) {
  throw new Error('MongoDB connection string is not defined in config.json.');
}

// Constants
const FORCE_CREATE = process.argv.includes('--force-create');
const RUN_PRODUCTS = process.argv.includes('--products');
const RUN_ORDERS = process.argv.includes('--orders');
const PRODUCT_SYNC_CONCURRENCY = 5;
const REST_RATE_LIMIT_DELAY = 100;
const MONGODB_WRITE_BATCH_INTERVAL = 10;
const FETCH_CONCURRENCY = 10;
const FETCH_BATCH_SIZE = 100;
const BULK_FETCH_SIZE = 250;

// Create logs directory if it doesn't exist
const LOGS_DIR = path.join(__dirname, 'logs');
(async () => {
  try {
    await fs.mkdir(LOGS_DIR, { recursive: true });
  } catch (error) {
    console.error(`Failed to create logs directory: ${error.message}`);
    process.exit(1);
  }
})();

// Ensure cache directory exists
const CACHE_DIR = path.join(__dirname, 'cache');
(async () => {
  try {
    await fs.mkdir(CACHE_DIR, { recursive: true });
  } catch (error) {
    console.error(`Failed to create cache directory: ${error.message}`);
    process.exit(1);
  }
})();

// Timestamp for log filenames
const timestamp = new Date().toISOString().replace(/:/g, '-').split('.')[0];

// Logger setup with separate log files
const loggerConfig = {
  level: 'debug', // Set to 'debug' for detailed logs
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} - ${level}: ${message}`)
  )
};

// Main logger for general messages
const mainLogger = winston.createLogger({
  ...loggerConfig,
  transports: [
    new winston.transports.File({
      filename: path.join(LOGS_DIR, `main_sync_${timestamp}.log`),
      handleExceptions: true,
      handleRejections: true
    }),
    new winston.transports.Console()
  ]
});

// Logger for product sync
const productSyncLogger = winston.createLogger({
  ...loggerConfig,
  transports: [
    new winston.transports.File({
      filename: path.join(LOGS_DIR, `product_sync_${timestamp}.log`),
      handleExceptions: true,
      handleRejections: true
    }),
    new winston.transports.Console()
  ]
});

// Logger for order sync
const orderSyncLogger = winston.createLogger({
  ...loggerConfig,
  transports: [
    new winston.transports.File({
      filename: path.join(LOGS_DIR, `order_sync_${timestamp}.log`),
      handleExceptions: true,
      handleRejections: true
    }),
    new winston.transports.Console()
  ]
});

// Shopify API setup with increased rate limit
const shopify = new Shopify({
  shopName: SHOPIFY_STORE_NAME,
  accessToken: SHOPIFY_ACCESS_TOKEN,
  apiVersion: '2025-01',
  autoLimit: { calls: 2, interval: 1000, bucketSize: 40 },
  timeout: 120000
});

// MongoDB setup
let mongoClient;
let productMappingCollection;

// Track changed mappings for MongoDB writes
let changedMappings = {};

async function connectToMongoDB() {
  try {
    if (!mongoClient) {
      mongoClient = new MongoClient(MONGODB_CONNECTION_STRING);
      await mongoClient.connect();
      const db = mongoClient.db('smiffys_shopify');
      productMappingCollection = db.collection('product_mapping');
      mainLogger.info('Connected to MongoDB Atlas');
    }
  } catch (error) {
    mainLogger.error(`Failed to connect to MongoDB Atlas: ${error.message}`);
    throw error;
  }
}

// Load product mapping from MongoDB (include last known stock levels, prices, and image filenames)
async function loadProductMapping() {
  try {
    const mappingDocs = await productMappingCollection.find({}).toArray();
    const mapping = {};
    mappingDocs.forEach(doc => {
      mapping[doc.genericCode] = {
        shopifyProductId: doc.shopifyProductId,
        lastStock: doc.lastStock || {},
        lastPrices: doc.lastPrices || {},
        lastImageFilenames: doc.lastImageFilenames || [], // Load cached image filenames
      };
    });
    productSyncLogger.info(`Loaded product mapping from MongoDB with ${Object.keys(mapping).length} entries`);
    return mapping;
  } catch (error) {
    productSyncLogger.error(`Failed to load product mapping from MongoDB: ${error.message}`);
    throw error;
  }
}

// Save product mapping to MongoDB (optimized to save only changes, include stock levels, prices, and image filenames)
async function saveProductMapping(mapping, batchNumber, totalBatches) {
  try {
    if (Object.keys(changedMappings).length === 0) {
      productSyncLogger.info('No changed mappings to save');
      return;
    }
    if (batchNumber % MONGODB_WRITE_BATCH_INTERVAL !== 0 && batchNumber !== totalBatches) return;

    const bulkOps = Object.entries(changedMappings).map(([genericCode, data]) => ({
      replaceOne: {
        filter: { genericCode },
        replacement: {
          genericCode,
          shopifyProductId: data.shopifyProductId,
          lastStock: data.lastStock || {},
          lastPrices: data.lastPrices || {},
          lastImageFilenames: data.lastImageFilenames || [], // Save updated image filenames
          updatedAt: new Date(),
        },
        upsert: true,
      },
    }));
    if (bulkOps.length > 0) {
      const result = await productMappingCollection.bulkWrite(bulkOps, { ordered: false });
      productSyncLogger.info(`Saved ${result.modifiedCount} modified and ${result.upsertedCount} upserted mappings`);
    }
    changedMappings = {};
  } catch (error) {
    productSyncLogger.error(`Failed to save mappings: ${error.message}`);
    throw error;
  }
}

// Singleton for Smiffys orders SOAP client
let smiffysOrdersClient = null;

async function getSmiffysOrdersClient() {
  if (!smiffysOrdersClient) {
    smiffysOrdersClient = await new Promise((resolve, reject) => {
      soap.createClient(SMIFFYS_ORDERS_URL, {}, (err, client) => {
        if (err) {
          orderSyncLogger.error(`Failed to create SOAP client for Smiffys orders API: ${err.message}`);
          return reject(err);
        }
        orderSyncLogger.info('Successfully created SOAP client for Smiffys orders API');
        resolve(client);
      });
    });
  }
  return smiffysOrdersClient;
}

// Test Shopify connection and get location ID
async function testShopifyConnection() {
  const start = Date.now();
  try {
    const products = await shopify.product.list({ limit: 1 });
    let locationId = null;
    try {
      const locations = await shopify.location.list();
      locationId = locations[0]?.id;
      mainLogger.info(`Successfully accessed locations endpoint. Locations: ${locations.length}, Location ID: ${locationId}`);
    } catch (error) {
      mainLogger.warn(`Failed to access locations endpoint: ${error.message}`);
    }
    mainLogger.info(`Shopify connection test completed. Found product: ${products[0]?.title || 'None'}, Location ID: ${locationId || 'Not available'}, Took: ${(Date.now() - start) / 1000}s`);
    return locationId;
  } catch (error) {
    mainLogger.error(`Shopify connection failed: ${error.message}`);
    throw error;
  }
}

// Fetch local images for a product (gallery support) only if needed
async function getLocalImages(product, forceLoad = false) {
  if (!product || !product.ProductCode) {
    productSyncLogger.warn(`No valid product data provided to getLocalImages for ${product?.ProductCode || 'unknown'}`);
    return [];
  }
  const imageFields = [
    { filename: product.FrontShot, position: 1 },
    { filename: product.SideShot, position: 2 },
    { filename: product.BackShot, position: 3 }
  ];
  if (!forceLoad) {
    productSyncLogger.debug(`Skipping image load for ${product.ProductCode}, using filenames: ${imageFields.filter(f => f.filename).map(f => f.filename).join(', ')}`);
    return imageFields.filter(f => f.filename).map(f => ({ filename: f.filename, position: f.position }));
  }
  const imagePromises = imageFields.map(async ({ filename, position }) => {
    if (!filename) return null;
    const imagePath = path.join(IMAGE_DIR, filename);
    try {
      await fs.access(imagePath);
      const buffer = await fs.readFile(imagePath);
      return { attachment: buffer.toString('base64'), position, filename };
    } catch (error) {
      productSyncLogger.warn(`Failed to load image ${filename} for product ${product.ProductCode}: ${error.message}`);
      return null;
    }
  });
  const images = (await Promise.all(imagePromises)).filter(img => img !== null);
  if (images.length > 0) {
    productSyncLogger.info(`Loaded ${images.length} images for product ${product.ProductCode}: ${imageFields.map(f => f.filename).join(', ')}`);
  } else {
    productSyncLogger.warn(`No images loaded for product ${product.ProductCode}`);
  }
  return images;
}

// Sanitize tags to be Shopify-compatible
function sanitizeTag(tag) {
  if (!tag) return null;
  return tag.trim().replace(/[,;:]/g, '');
}

// Round price to the nearest .99
function roundToNearest99(price) {
  const rounded = Math.ceil(price);
  return (rounded - 0.01).toFixed(2);
}

// Fetch Smiffys products from SOAP API or file-based cache
async function fetchSmiffysProducts() {
  const start = Date.now();
  const cacheFile = path.join(CACHE_DIR, 'smiffys_cache.json');
  let products = [];

  productSyncLogger.info('Fetching products from Smiffys API');

  const maxRetries = 3;
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      products = await new Promise((resolve, reject) => {
        soap.createClient(SMIFFYS_PRODUCTS_URL, {}, (err, client) => {
          if (err) {
            productSyncLogger.error(`Failed to create SOAP client for Smiffys products API: ${err.message}`);
            return reject(err);
          }
          client.GetFullDataSet({ apiKey: SMIFFYS_API_KEY, clientID: SMIFFYS_CLIENT_ID }, async (err, result) => {
            if (err) {
              productSyncLogger.error(`Failed to fetch products from Smiffys API: ${err.message}`);
              return reject(err);
            }
            products = result?.GetFullDataSetResult?.ProductList?.Product || [];
            try {
              await fs.writeFile(cacheFile, JSON.stringify(products, null, 2));
              productSyncLogger.info(`Fetched and cached ${products.length} products from Smiffys, Took: ${(Date.now() - start) / 1000}s`);
            } catch (writeError) {
              productSyncLogger.warn(`Failed to cache Smiffys products: ${writeError.message}`);
            }
            resolve(products);
          });
        });
      });
      return products;
    } catch (error) {
      lastError = error;
      productSyncLogger.warn(`Attempt ${attempt}/${maxRetries} failed to fetch Smiffys products: ${error.message}`);
      if (attempt < maxRetries) {
        const delay = 2000 * attempt;
        productSyncLogger.info(`Retrying in ${delay / 1000}s...`);
        await setTimeoutPromise(delay);
      }
    }
  }

  productSyncLogger.error(`Failed to fetch Smiffys products after ${maxRetries} attempts: ${lastError.message}`);
  throw lastError;
}

// Helper function to determine audience
function determineAudience(rawAudience) {
  const audienceUpper = (rawAudience || '').trim().toUpperCase();
  if (['CHILD', 'KID', 'KIDS', 'YOUTH', 'JUNIOR', 'CHILDREN'].includes(audienceUpper)) {
    return 'Kids';
  } else if (['ADULT', 'ADULTS'].includes(audienceUpper)) {
    return 'Adult';
  }
  return 'Unspecified';
}

// Transform Smiffys products to Shopify format with image gallery
async function transformSmiffysToShopify(smiffysProducts) {
  const start = Date.now();
  const productMap = new Map();
  const handledSkus = new Set();
  const collectionNames = new Set(['Mens', 'Womens', 'Girls', 'Boys', 'All Products']);

  async function fetchOrCreateCollectionIds(collectionNames) {
    const start = Date.now();
    const collections = {};

    let pageInfo = null;
    const limit = 250;

    try {
      do {
        const params = { limit };
        if (pageInfo) params.page_info = pageInfo;
        const customCollections = await withRetry(() => shopify.customCollection.list(params));
        customCollections.forEach(collection => {
          collections[collection.title] = collection.id;
        });
        const linkHeader = customCollections.headers?.link;
        pageInfo = null;
        if (linkHeader) {
          const match = linkHeader.match(/<[^>]+page_info=([^>]+)>; rel="next"/);
          if (match) pageInfo = match[1];
        }
      } while (pageInfo);

      for (const collectionName of collectionNames) {
        if (!collections[collectionName]) {
          try {
            const newCollection = await withRetry(() =>
              shopify.customCollection.create({
                title: collectionName,
                published: true,
              })
            );
            collections[collectionName] = newCollection.id;
            productSyncLogger.info(`Created collection ${collectionName} with ID ${newCollection.id}`);
          } catch (error) {
            productSyncLogger.error(`Failed to create collection ${collectionName}: ${error.message}`);
            throw error;
          }
        }
      }

      productSyncLogger.info(`Fetched or created ${Object.keys(collections).length} collection IDs, Took: ${(Date.now() - start) / 1000}s`);
      return collections;
    } catch (error) {
      productSyncLogger.error(`Error in fetchOrCreateCollectionIds: ${error.message}`);
      throw error;
    }
  }

  for (const product of smiffysProducts) {
    const fullSku = product.ProductCode || '';
    if (!fullSku) {
      productSyncLogger.warn(`Skipping product with empty ProductCode: ${JSON.stringify(product)}`);
      continue;
    }
    const groupingCode = fullSku.match(/^\d+/)?.[0] || fullSku.replace(/[^a-zA-Z0-9]/g, '');
    if (!groupingCode) {
      productSyncLogger.warn(`Invalid groupingCode for SKU ${fullSku}, skipping`);
      continue;
    }
    const size = fullSku.replace(groupingCode, '') || 'Default';

    if (handledSkus.has(fullSku)) {
      productSyncLogger.info(`Skipping duplicate SKU ${fullSku}`);
      continue;
    }
    handledSkus.add(fullSku);

    let stock = 0;
    const rawStockQty = product.StockQty || '';
    if (rawStockQty === '') {
      productSyncLogger.warn(`Empty StockQty for SKU ${fullSku}, setting stock to 0`);
    } else {
      stock = parseInt(rawStockQty, 10);
      if (isNaN(stock) || stock < 0) {
        productSyncLogger.warn(`Invalid StockQty for SKU ${fullSku}: ${rawStockQty}, setting stock to 0`);
        stock = 0;
      }
    }

    const basePrice = parseFloat(product.stdPrice1 || 0);

    if (!productMap.has(groupingCode)) {
      const catalogueName = (product.CatalogueName || 'Costumes').trim();
      const rawGender = (product.Gender || '').trim();
      const rawAudience = (product.Audience || '').trim();
      const audience = determineAudience(rawAudience);
      const collections = new Set([catalogueName, 'All Products']);

      if (rawGender.toUpperCase() === 'UNISEX') {
        if (catalogueName.toUpperCase() === 'COSTUMES' && audience === 'Adult') {
          collections.add('Mens');
          collections.add('Womens');
        } else if (catalogueName.toUpperCase() === 'COSTUMES' && audience === 'Kids') {
          collections.add('Boys');
          collections.add('Girls');
        }
      } else if (catalogueName.toUpperCase() === 'COSTUMES') {
        if (rawGender.toUpperCase() === 'MALE' && audience === 'Adult') collections.add('Mens');
        else if (rawGender.toUpperCase() === 'FEMALE' && audience === 'Adult') collections.add('Womens');
        else if (rawGender.toUpperCase() === 'FEMALE' && audience === 'Kids') collections.add('Girls');
        else if (rawGender.toUpperCase() === 'MALE' && audience === 'Kids') collections.add('Boys');
      }

      const title = (product.ProductName || 'Unnamed Product').replace(/[^a-zA-Z0-9\s]/g, '');
      const description = (product.WebDescription || product.BrochureDescription || 'No description available').replace(/[^a-zA-Z0-9\s.,-]/g, '').substring(0, 10000);
      const finalTitle = audience === 'Adult' ? title : `${audience} ${title}`;

      // Load images with full data initially to populate cache
      const images = await getLocalImages(product, true); // Force full load on first pass
      const imageFilenames = images.map(img => img.filename);

      productMap.set(groupingCode, {
        title: finalTitle,
        body_html: description,
        vendor: 'Smiffys',
        product_type: catalogueName,
        tags: `smiffys_generic_code:${groupingCode}`,
        collections: Array.from(collections),
        variants: [],
        genericCode: groupingCode,
        totalStock: 0,
        rawAudience,
        rawGender,
        stockMap: {},
        priceMap: {},
        images: images, // Attach initial image data
        originalProduct: product, // Store original product data for getLocalImages
      });

      collectionNames.add(catalogueName);
    }

    let price = basePrice;
    const audienceUpper = (product.Audience || '').trim().toUpperCase();
    if (['CHILD', 'KID', 'KIDS', 'YOUTH', 'JUNIOR', 'CHILDREN'].includes(audienceUpper)) {
      price = basePrice * 1.7;
    } else if (['ADULT', 'ADULTS'].includes(audienceUpper)) {
      price = basePrice * 2.25;
    }
    price = roundToNearest99(price);

    const variant = {
      sku: fullSku,
      barcode: product.BarCode || '',
      price: price,
      inventory_quantity: stock,
      inventory_management: 'shopify',
      weight: parseFloat(product.unit_weight || 0) || 0.02,
      weight_unit: 'kg',
      option1: size === 'Default' ? 'Default Title' : size,
    };
    const productData = productMap.get(groupingCode);
    productData.variants.push(variant);
    productData.totalStock += stock;
    productData.status = 'active';
    productData.stockMap[fullSku] = stock;
    productData.priceMap[fullSku] = price;
  }

  collectionIds = await fetchOrCreateCollectionIds([...collectionNames]);

  const filteredProductMap = new Map();
  for (const [groupingCode, productData] of productMap) {
    if (productData.totalStock <= 0) {
      productSyncLogger.info(`Skipping product ${groupingCode} (${productData.title}) due to zero total stock: ${productData.totalStock}`);
      continue;
    }
    filteredProductMap.set(groupingCode, productData);
  }

  const shopifyProducts = [];
  const sortedEntries = Array.from(filteredProductMap.entries()).sort((a, b) => a[1].title.localeCompare(b[1].title));
  const processedProducts = new Map();

  for (let i = 0; i < sortedEntries.length; i++) {
    const [groupingCode, productData] = sortedEntries[i];
    const duplicates = [];
    for (let j = i + 1; j < sortedEntries.length && sortedEntries[j][1].title === productData.title; j++) {
      duplicates.push(sortedEntries[j]);
    }
    if (duplicates.length > 0) {
      const allDups = [[groupingCode, productData], ...duplicates];
      allDups.forEach(([dupCode, dupData], index) => {
        const audience = determineAudience(dupData.rawAudience);
        dupData.title = `${audience} ${dupData.title}${index > 0 ? ` (${index + 1})` : ''}`.trim();
        processedProducts.set(dupCode, dupData);
      });
      i += duplicates.length;
    } else {
      processedProducts.set(groupingCode, productData);
    }
  }

  const finalEntries = Array.from(processedProducts.entries());
  for (const [groupingCode, productData] of finalEntries) {
    if (productData.variants.length > 1) {
      productData.options = [{ name: 'Size', position: 1, values: productData.variants.map(v => v.option1) }];
    } else {
      productData.options = [{ name: 'Title', position: 1, values: ['Default Title'] }];
    }
    shopifyProducts.push(productData);
  }

  productSyncLogger.info(`Prepared ${shopifyProducts.length} products after filtering, Took: ${(Date.now() - start) / 1000}s`);
  return shopifyProducts;
}

// Retry logic for Shopify API calls with exponential backoff
async function withRetry(fn, retries = 5, baseDelay = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const result = await fn();
      return result;
    } catch (error) {
      if (error.response && [429, 403, 502].includes(error.response.status)) {
        const retryAfter = error.response.headers['retry-after'] || 'unknown';
        mainLogger.warn(`Retry ${i + 1}/${retries} after ${error.message}. Retry-After: ${retryAfter}s`);
      } else if (error.message.includes('Timeout') || error.message.includes('ECONNRESET') || error.message.includes('EPIPE')) {
        mainLogger.warn(`Retry ${i + 1}/${retries} after network error: ${error.message}`);
      }
      if (i === retries - 1) {
        mainLogger.error(`Retry failed after ${retries} attempts: ${error.message}`);
        throw error;
      }
      const delay = baseDelay * Math.pow(2, i);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

// Clean up duplicates in Shopify based on smiffys_generic_code tags
async function cleanUpDuplicates() {
  const start = Date.now();
  productSyncLogger.info('Starting duplicate cleanup');

  const allProducts = [];
  let pageInfo = null;
  const limit = 250;

  do {
    const params = { limit, fields: 'id,title,tags,variants' };
    if (pageInfo) params.page_info = pageInfo;
    const products = await withRetry(() => shopify.product.list(params));
    allProducts.push(...products);
    const linkHeader = products.headers?.link;
    pageInfo = null;
    if (linkHeader) {
      const match = linkHeader.match(/<[^>]+page_info=([^>]+)>; rel="next"/);
      if (match) pageInfo = match[1];
    }
  } while (pageInfo);

  const productMap = new Map();
  for (const product of allProducts) {
    const tags = product.tags ? product.tags.split(',').map(tag => tag.trim()) : [];
    const genericCodeTag = tags.find(tag => tag.startsWith('smiffys_generic_code:'));
    if (genericCodeTag) {
      const genericCode = genericCodeTag.split(':')[1];
      if (productMap.has(genericCode)) {
        const existing = productMap.get(genericCode);
        productSyncLogger.warn(`Duplicate for ${genericCode}: ID ${existing.id} (${existing.title}), ID ${product.id} (${product.title})`);
        const existingVariants = existing.variants.length;
        const newVariants = product.variants.length;
        if (newVariants > existingVariants || (newVariants === existingVariants && product.id > existing.id)) {
          productMap.set(genericCode, product);
          await withRetry(() => shopify.product.delete(existing.id));
          productSyncLogger.info(`Kept ID ${product.id}, deleted ID ${existing.id} for ${genericCode}`);
        } else {
          await withRetry(() => shopify.product.delete(product.id));
          productSyncLogger.info(`Kept ID ${existing.id}, deleted ID ${product.id} for ${genericCode}`);
        }
      } else {
        productMap.set(genericCode, product);
      }
    }
  }

  const productMapping = {};
  for (const [genericCode, product] of productMap) {
    productMapping[genericCode] = { 
      shopifyProductId: product.id, 
      lastStock: {},
      lastPrices: {},
    };
    changedMappings[genericCode] = productMapping[genericCode];
  }
  await saveProductMapping(productMapping);

  productSyncLogger.info(`Duplicate cleanup completed, processed ${allProducts.length} products in ${(Date.now() - start) / 1000}s`);
}

// Remove products no longer in Smiffys feed
async function removeDiscontinuedProducts(currentGenericCodes) {
  const start = Date.now();
  productSyncLogger.info('Starting discontinued products cleanup');

  const globalProductMapping = await loadProductMapping();
  const productsToRemove = Object.keys(globalProductMapping).filter(
    code => !currentGenericCodes.has(code)
  );

  productSyncLogger.info(`Found ${productsToRemove.length} products to remove`);

  for (const genericCode of productsToRemove) {
    try {
      const productId = globalProductMapping[genericCode].shopifyProductId;
      await withRetry(() => shopify.product.delete(productId));
      productSyncLogger.info(`Removed discontinued product ${genericCode} (Shopify ID: ${productId})`);
      
      // Remove from MongoDB mapping
      await productMappingCollection.deleteOne({ genericCode });
      delete globalProductMapping[genericCode];
      delete changedMappings[genericCode];
      global.syncStats.deleted++;
    } catch (error) {
      productSyncLogger.error(`Failed to remove product ${genericCode}: ${error.message}`);
    }
  }

  await saveProductMapping(globalProductMapping);
  productSyncLogger.info(`Discontinued products cleanup completed in ${(Date.now() - start) / 1000}s`);
}

// Fetch existing Shopify products using the product mapping and tags (optimized with bulk fetching)
async function fetchExistingProducts(productsToSync) {
  const start = Date.now();
  const genericCodes = new Set(productsToSync.map(p => p.genericCode));
  const existingProducts = new Map();

  let globalProductMapping = await loadProductMapping();
  const unmappedGenericCodes = new Set(genericCodes);

  // Clean up PENDING entries in MongoDB
  const pendingEntries = Object.entries(globalProductMapping).filter(
    ([_, mapping]) => mapping.shopifyProductId === 'PENDING'
  );
  for (const [genericCode, _] of pendingEntries) {
    productSyncLogger.info(`Removing PENDING mapping for genericCode ${genericCode} from MongoDB`);
    await productMappingCollection.deleteOne({ genericCode });
    delete globalProductMapping[genericCode];
  }
  if (pendingEntries.length > 0) {
    productSyncLogger.info(`Removed ${pendingEntries.length} PENDING mappings from MongoDB`);
  }

  // Step 1: Bulk fetch products by ID from the mapping
  const mappedGenericCodes = Array.from(genericCodes).filter(code => globalProductMapping[code]?.shopifyProductId && globalProductMapping[code].shopifyProductId !== 'PENDING');
  for (let i = 0; i < mappedGenericCodes.length; i += BULK_FETCH_SIZE) {
    const batchCodes = mappedGenericCodes.slice(i, i + BULK_FETCH_SIZE);
    const productIds = batchCodes.map(code => globalProductMapping[code].shopifyProductId).join(',');
    try {
      const products = await withRetry(() =>
        shopify.product.list({
          ids: productIds,
          limit: BULK_FETCH_SIZE,
          fields: 'id,title,variants,vendor,product_type,tags,status,images'
        })
      );
      for (const product of products) {
        const tags = product.tags ? product.tags.split(',').map(tag => tag.trim()) : [];
        const genericCodeTag = tags.find(tag => tag.startsWith('smiffys_generic_code:'));
        if (genericCodeTag) {
          const genericCode = genericCodeTag.split(':')[1];
          existingProducts.set(genericCode, product);
          unmappedGenericCodes.delete(genericCode);
          productSyncLogger.info(`Found product for genericCode ${genericCode} in mapping: Shopify ID ${product.id}, Title: ${product.title}, Tags: ${product.tags}, Status: ${product.status}, Images: ${product.images?.length || 0}`);
        }
      }
      // Remove invalid mappings
      batchCodes.forEach(code => {
        if (!existingProducts.has(code)) {
          productSyncLogger.warn(`Product ID ${globalProductMapping[code].shopifyProductId} from mapping for genericCode ${code} not found in Shopify`);
          delete globalProductMapping[code];
        }
      });
    } catch (error) {
      productSyncLogger.error(`Error fetching products by IDs: ${error.message}`);
    }
  }

  // Step 2: Fetch unmapped products using tag search
  if (unmappedGenericCodes.size > 0) {
    productSyncLogger.info(`Searching Shopify for ${unmappedGenericCodes.size} unmapped generic codes`);
    const genericCodeTags = Array.from(unmappedGenericCodes).map(code => `smiffys_generic_code:${code}`);
    const batches = [];
    for (let i = 0; i < genericCodeTags.length; i += FETCH_BATCH_SIZE) {
      batches.push(genericCodeTags.slice(i, i + FETCH_BATCH_SIZE));
    }

    await Promise.map(batches, async batchTags => {
      const query = batchTags.join(' OR ');
      try {
        const products = await withRetry(() =>
          shopify.product.list({
            query: query,
            limit: 250,
            fields: 'id,title,variants,vendor,product_type,tags,status,images'
          })
        );
        for (const product of products) {
          const tags = product.tags ? product.tags.split(',').map(tag => tag.trim()) : [];
          const genericCodeTag = tags.find(tag => tag.startsWith('smiffys_generic_code:'));
          if (genericCodeTag) {
            const genericCode = genericCodeTag.split(':')[1];
            if (unmappedGenericCodes.has(genericCode)) {
              existingProducts.set(genericCode, product);
              globalProductMapping[genericCode] = { 
                shopifyProductId: product.id, 
                lastStock: {},
                lastPrices: {},
              };
              changedMappings[genericCode] = globalProductMapping[genericCode];
              productSyncLogger.info(`Found product for genericCode ${genericCode} via tag search: Shopify ID ${product.id}, Title: ${product.title}, Tags: ${product.tags}, Status: ${product.status}, Images: ${product.images?.length || 0}`);
              unmappedGenericCodes.delete(genericCode);
            }
          }
        }
      } catch (error) {
        productSyncLogger.error(`Error searching for products with tags ${batchTags.join(', ')}: ${error.message}`);
      }
    }, { concurrency: FETCH_CONCURRENCY });
  }

  await saveProductMapping(globalProductMapping);
  productSyncLogger.info(`Fetched ${existingProducts.size} existing products, Took: ${(Date.now() - start) / 1000}s`);
  return { existingProducts, globalProductMapping };
}

// Sync a single product to Shopify (upload images only on creation, update stock thereafter)
async function syncProduct(productData, existingProducts, globalProductMapping, failureLogger) {
  const start = Date.now();
  if (!productData) {
    productSyncLogger.error(`Sync failed for undefined product data`);
    return null;
  }
  const baseSku = productData.genericCode;
  productSyncLogger.info(`Attempting to sync ${baseSku} with ${productData.variants.length} variants`);

  if (!global.syncStats) {
    global.syncStats = { created: 0, updated: 0, skipped: 0, failed: 0, deleted: 0 };
  }

  const totalStock = productData.variants.reduce((sum, variant) => sum + (variant.inventory_quantity || 0), 0);
  if (totalStock <= 0) {
    productSyncLogger.warn(`Product ${baseSku} (${productData.title}) has zero total stock (${totalStock}) at sync stage, skipping`);
    global.syncStats.skipped++;
    return null;
  }

  try {
    productData.tags = `smiffys_generic_code:${baseSku}`;

    let existingProduct = existingProducts.get(baseSku);
    let syncedProduct;

    const cachedMapping = globalProductMapping[baseSku];
    if (!cachedMapping) {
      productSyncLogger.warn(`No MongoDB mapping found for product ${baseSku}`);
    } else {
      productSyncLogger.info(`MongoDB mapping for ${baseSku}: Shopify ID ${cachedMapping.shopifyProductId}, Last Stock: ${JSON.stringify(cachedMapping.lastStock)}, Last Image Filenames: ${cachedMapping.lastImageFilenames.join(', ')}`);
    }

    let stockChanged = false;
    let priceChanged = false;
    let inventoryUpdates = []; // Store inventory updates to apply after product update

    if (existingProduct && !FORCE_CREATE) {
      // For existing products, skip image updates and focus on stock/price
      const existingVariants = existingProduct.variants || [];
      const existingVariantMap = new Map();
      existingVariants.forEach(variant => {
        existingVariantMap.set(variant.sku, {
          inventory_quantity: variant.inventory_quantity,
          price: parseFloat(variant.price),
          id: variant.id,
          barcode: variant.barcode,
          weight: variant.weight,
          weight_unit: variant.weight_unit,
          option1: variant.option1,
          inventory_item_id: variant.inventory_item_id // Needed for inventory updates
        });
      });

      const updateData = {
        vendor: 'Smiffys',
        status: 'active',
        tags: `smiffys_generic_code:${baseSku}`,
        variants: [],
      };

      // Preserve existing variants and update stock/price
      for (const existingVariant of existingVariants) {
        const variant = {
          id: existingVariant.id,
          sku: existingVariant.sku,
          price: parseFloat(existingVariant.price),
          barcode: existingVariant.barcode,
          weight: existingVariant.weight,
          weight_unit: existingVariant.weight_unit,
          option1: existingVariant.option1,
        };
        updateData.variants.push(variant);
      }

      for (const newVariant of productData.variants) {
        const existingVariant = existingVariantMap.get(newVariant.sku);
        const newStock = newVariant.inventory_quantity || 0;
        const newPrice = parseFloat(newVariant.price) || 0;

        if (existingVariant) {
          const shopifyStock = existingVariant.inventory_quantity || 0;
          const shopifyPrice = existingVariant.price || 0;

          if (shopifyStock !== newStock) {
            stockChanged = true;
            productSyncLogger.info(`Detected stock change for product ${baseSku} variant ${newVariant.sku}: previous stock ${shopifyStock}, new stock ${newStock}`);
            inventoryUpdates.push({
              inventory_item_id: existingVariant.inventory_item_id,
              available: newStock,
              sku: newVariant.sku
            });
          }

          if (shopifyPrice !== newPrice) {
            priceChanged = true;
            const variantIndex = updateData.variants.findIndex(v => v.sku === newVariant.sku);
            if (variantIndex !== -1) {
              updateData.variants[variantIndex].price = newPrice;
            }
          }
        } else {
          // New variant, include all details except inventory_quantity
          productSyncLogger.info(`Added new variant for product ${baseSku} variant ${newVariant.sku}: initial stock ${newStock}`);
          updateData.variants.push({
            sku: newVariant.sku,
            price: newPrice,
            barcode: newVariant.barcode,
            weight: newVariant.weight,
            weight_unit: newVariant.weight_unit,
            option1: newVariant.option1,
          });
          stockChanged = true;
          priceChanged = true;
        }
      }

      const vendorChanged = existingProduct.vendor !== 'Smiffys';
      const statusChanged = existingProduct.status !== 'active';
      const tagsChanged = existingProduct.tags !== `smiffys_generic_code:${baseSku}`;

      if (!stockChanged && !priceChanged && !vendorChanged && !statusChanged && !tagsChanged) {
        productSyncLogger.info(`No changes detected for product ${baseSku} (Shopify ID ${existingProduct.id}), skipping update`);
        global.syncStats.skipped++;
        return existingProduct.id;
      }

      productSyncLogger.info(`Product ${baseSku} already exists, updating Shopify ID ${existingProduct.id} (Stock Changed: ${stockChanged}, Price Changed: ${priceChanged}, Vendor Changed: ${vendorChanged}, Status Changed: ${statusChanged}, Tags Changed: ${tagsChanged})...`);
      syncedProduct = await withRetry(() => shopify.product.update(existingProduct.id, updateData));
      productSyncLogger.info(`Updated product ${baseSku}: Vendor: ${syncedProduct.vendor}, Status: ${syncedProduct.status}, Tags: ${syncedProduct.tags}, Images: ${syncedProduct.images?.length || 0}`);
      productSyncLogger.info(`Skipped collection and image updates for existing product ${baseSku} (Shopify ID ${existingProduct.id})`);
      global.syncStats.updated++;

      // Apply inventory updates using the inventory_levels endpoint
      if (inventoryUpdates.length > 0) {
        const locationId = await testShopifyConnection();
        if (!locationId) {
          throw new Error(`No location ID available for inventory updates for product ${baseSku}`);
        }

        for (const update of inventoryUpdates) {
          try {
            await withRetry(() =>
              shopify.inventoryLevel.set({
                location_id: locationId,
                inventory_item_id: update.inventory_item_id,
                available: update.available
              })
            );
            productSyncLogger.info(`Updated inventory for product ${baseSku} variant ${update.sku}: set stock to ${update.available} at location ${locationId}`);
          } catch (error) {
            productSyncLogger.error(`Failed to update inventory for product ${baseSku} variant ${update.sku}: ${error.message}`);
            failureLogger.info(`Failed to update inventory: genericCode=${baseSku}, sku=${update.sku}, error=${error.message}`);
          }
        }
      }

      const stockMap = {};
      const priceMap = {};
      productData.variants.forEach(variant => {
        stockMap[variant.sku] = variant.inventory_quantity;
        priceMap[variant.sku] = parseFloat(variant.price);
      });
      globalProductMapping[baseSku].lastStock = stockMap;
      globalProductMapping[baseSku].lastPrices = priceMap;
      changedMappings[baseSku] = globalProductMapping[baseSku];
    } else {
      if (!productData.title || productData.title.length > 255) {
        productSyncLogger.warn(`Invalid title for ${baseSku}: "${productData.title}", skipping creation`);
        global.syncStats.failed++;
        failureLogger.info(`Failed to create in Shopify: genericCode=${baseSku}, title=${productData.title}, error=Invalid title`);
        return null;
      }

      const variantSkus = new Set();
      for (const variant of productData.variants) {
        if (!variant.sku || variantSkus.has(variant.sku)) {
          productSyncLogger.warn(`Invalid or duplicate SKU for ${baseSku}: ${variant.sku}, skipping creation`);
          global.syncStats.failed++;
          failureLogger.info(`Failed to create in Shopify: genericCode=${baseSku}, title=${productData.title}, error=Invalid or duplicate SKU ${variant.sku}`);
          return null;
        }
        variantSkus.add(variant.sku);
        if (variant.price <= 0) {
          productSyncLogger.warn(`Invalid price for ${baseSku} variant ${variant.sku}: ${variant.price}, skipping creation`);
          global.syncStats.failed++;
          failureLogger.info(`Failed to create in Shopify: genericCode=${baseSku}, title=${productData.title}, error=Invalid price for variant ${variant.sku}`);
          return null;
        }
      }

      // Save generic code to MongoDB before creating in Shopify
      try {
        const desiredCollectionIds = productData.collections
          .map(name => collectionIds[name])
          .filter(Boolean)
          .sort();
        await productMappingCollection.updateOne(
          { genericCode: baseSku },
          { $set: { genericCode: baseSku, shopifyProductId: 'PENDING', lastStock: {}, lastPrices: {}, lastImageFilenames: [], createdAt: new Date() } },
          { upsert: true }
        );
        productSyncLogger.info(`Saved genericCode ${baseSku} to MongoDB with PENDING status before Shopify creation`);
      } catch (error) {
        productSyncLogger.error(`Failed to save genericCode ${baseSku} to MongoDB before creation: ${error.message}`);
        failureLogger.info(`Failed to save to MongoDB: genericCode=${baseSku}, error=${error.message}`);
        global.syncStats.failed++;
        return null;
      }

      productSyncLogger.info(`No existing product for ${baseSku}, creating new product...`);
      productData.vendor = 'Smiffys';
      productData.status = 'active';
      productData.images = await getLocalImages(productData.originalProduct, true); // Upload images on creation
      if (productData.images.length > 0) {
        globalProductMapping[baseSku] = { 
          shopifyProductId: 'PENDING', 
          lastStock: {},
          lastPrices: {},
          lastImageFilenames: productData.images.map(img => img.filename)
        };
        changedMappings[baseSku] = globalProductMapping[baseSku];
      } else {
        productSyncLogger.warn(`No images loaded for new product ${baseSku}, proceeding without images`);
        globalProductMapping[baseSku] = { 
          shopifyProductId: 'PENDING', 
          lastStock: {},
          lastPrices: {},
          lastImageFilenames: []
        };
        changedMappings[baseSku] = globalProductMapping[baseSku];
      }
      // Remove inventory_quantity from creation payload
      productData.variants.forEach(variant => {
        delete variant.inventory_quantity;
      });
      try {
        syncedProduct = await withRetry(() => shopify.product.create(productData));
        productSyncLogger.info(`Created ${baseSku}: ${productData.title} (Type: ${productData.product_type}, Tags: ${productData.tags}, Vendor: ${productData.vendor}, Status: ${syncedProduct.status}, Images: ${syncedProduct.images?.length || 0})`);
        const stockMap = {};
        const priceMap = {};
        productData.variants.forEach(variant => {
          stockMap[variant.sku] = variant.inventory_quantity;
          priceMap[variant.sku] = parseFloat(variant.price);
        });

        // Fetch the created product to get inventory_item_id for each variant
        const createdProduct = await withRetry(() => shopify.product.get(syncedProduct.id));
        const locationId = await testShopifyConnection();
        if (!locationId) {
          throw new Error(`No location ID available for inventory updates for new product ${baseSku}`);
        }

        // Set initial inventory for new variants
        for (const variant of createdProduct.variants) {
          const matchingVariant = productData.variants.find(v => v.sku === variant.sku);
          if (matchingVariant) {
            const initialStock = matchingVariant.inventory_quantity || 0;
            try {
              await withRetry(() =>
                shopify.inventoryLevel.set({
                  location_id: locationId,
                  inventory_item_id: variant.inventory_item_id,
                  available: initialStock
                })
              );
              productSyncLogger.info(`Set initial stock for new product ${baseSku} variant ${variant.sku}: stock ${initialStock} at location ${locationId}`);
            } catch (error) {
              productSyncLogger.error(`Failed to set initial stock for new product ${baseSku} variant ${variant.sku}: ${error.message}`);
              failureLogger.info(`Failed to set initial stock: genericCode=${baseSku}, sku=${variant.sku}, error=${error.message}`);
            }
          }
        }

        globalProductMapping[baseSku].shopifyProductId = syncedProduct.id;
        globalProductMapping[baseSku].lastStock = stockMap;
        globalProductMapping[baseSku].lastPrices = priceMap;
        changedMappings[baseSku] = globalProductMapping[baseSku];
        global.syncStats.created++;

        // Update MongoDB with the Shopify product ID
        await productMappingCollection.updateOne(
          { genericCode: baseSku },
          { $set: { shopifyProductId: syncedProduct.id, lastStock: stockMap, lastPrices: priceMap, lastImageFilenames: productData.images.map(img => img.filename), updatedAt: new Date() } }
        );
        productSyncLogger.info(`Updated MongoDB mapping for genericCode ${baseSku} with Shopify ID ${syncedProduct.id}`);

        if (productData.collections && productData.collections.length > 0) {
          await updateProductCollections(syncedProduct.id, productData.collections);
        }
      } catch (error) {
        productSyncLogger.error(`Failed to create ${baseSku} in Shopify: ${error.message}`);
        failureLogger.info(`Failed to create in Shopify: genericCode=${baseSku}, title=${productData.title}, error=${error.message}`);
        global.syncStats.failed++;
        return null;
      }
    }

    productSyncLogger.info(`Synced ${baseSku} in ${(Date.now() - start) / 1000}s`);
    return syncedProduct.id;
  } catch (error) {
    productSyncLogger.error(`Failed to sync ${baseSku}: ${error.message}`);
    failureLogger.info(`Failed to sync: genericCode=${baseSku}, title=${productData?.title || 'Unknown'}, error=${error.message}`);
    global.syncStats.failed++;
    return null;
  }
}

// Fetch Shopify orders
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

// Submit order to Smiffys
async function submitOrderToSmiffys(orderXml) {
  const start = Date.now();
  try {
    const client = await getSmiffysOrdersClient();
    const args = {
      apiKey: SMIFFYS_API_KEY,
      clientID: SMIFFYS_CLIENT_ID,
      orderXml: orderXml
    };
    const result = await new Promise((resolve, reject) => {
      client.SubmitOrder(args, (err, result) => {
        if (err) return reject(new Error(`SOAP Error: ${err.message}`));
        resolve(result);
      });
    });

    const returnValue = result?.SubmitOrderResult?.ReturnValue;
    if (returnValue !== 'Success' && returnValue !== 'NotEnabled') {
      throw new Error(`Smiffys API returned an error: ${returnValue}`);
    }

    orderSyncLogger.info(`Successfully submitted order to Smiffys, Took: ${(Date.now() - start) / 1000}s`);
    return result;
  } catch (error) {
    orderSyncLogger.error(`Failed to submit order to Smiffys: ${error.message}`);
    throw error;
  }
}

// Update Shopify order
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
    }
  } catch (error) {
    orderSyncLogger.error(`Failed to update Shopify order ${orderId}: ${error.message}`);
    throw error;
  }
}

// Mark Shopify order as fulfilled
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

// Send email with log files and delete them afterward
async function sendLogEmail(timestamp, duration, syncError) {
  mainLogger.info('Preparing to send log email');

  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: false,
    auth: {
      user: SENDER_EMAIL,
      pass: SENDER_PASSWORD
    }
  });

  const logFiles = [
    path.join(LOGS_DIR, `main_sync_${timestamp}.log`),
    path.join(LOGS_DIR, `product_sync_${timestamp}.log`),
    path.join(LOGS_DIR, `order_sync_${timestamp}.log`),
    path.join(LOGS_DIR, `failed_product_uploads_${timestamp}.log`)
  ];

  const attachments = [];
  for (const logFile of logFiles) {
    try {
      await fs.access(logFile);
      attachments.push({ path: logFile });
    } catch (error) {
      mainLogger.warn(`Log file ${logFile} not found, skipping attachment: ${error.message}`);
    }
  }

  const emailSubject = EMAIL_SUBJECT_TEMPLATE.replace('{{timestamp}}', timestamp);
  const emailBody = `
    <h2>Smiffys to Shopify Sync Completed</h2>
    <p><strong>Timestamp:</strong> ${timestamp}</p>
    <p><strong>Duration:</strong> ${duration} seconds</p>
    <p><strong>Status:</strong> ${syncError ? 'Failed' : 'Success'}</p>
    ${syncError ? `<p><strong>Error:</strong> ${syncError.message}</p>` : ''}
    <p>Please find the log files attached for more details.</p>
  `;

  const mailOptions = {
    from: SENDER_EMAIL,
    to: RECIPIENT_EMAIL,
    subject: emailSubject,
    html: emailBody,
    attachments: attachments
  };

  try {
    await transporter.sendMail(mailOptions);
    mainLogger.info(`Log email sent successfully to ${RECIPIENT_EMAIL}`);

    // Delete log files after successful email send
    for (const logFile of logFiles) {
      try {
        await fs.access(logFile); // Check if file exists
        await fs.unlink(logFile);
        mainLogger.info(`Deleted log file: ${logFile}`);
      } catch (error) {
        if (error.code === 'ENOENT') {
          mainLogger.info(`Log file ${logFile} already deleted or does not exist`);
        } else {
          mainLogger.warn(`Failed to delete log file ${logFile}: ${error.message}`);
        }
      }
    }
  } catch (error) {
    mainLogger.error(`Failed to send log email: ${error.message}`);
    // Log files are not deleted if email sending fails
  }
}

// Update product collections (only called during product creation)
async function updateProductCollections(shopifyProductId, collections) {
  try {
    productSyncLogger.info(`Assigning collections for new product ID: ${shopifyProductId}`);
    const desiredCollectionIds = new Set(
      collections
        .map(name => {
          if (!collectionIds[name]) {
            productSyncLogger.warn(`Collection ${name} not found in collectionIds map, skipping assignment for product ID: ${shopifyProductId}`);
            return null;
          }
          return collectionIds[name];
        })
        .filter(Boolean)
    );

    for (const collectionId of desiredCollectionIds) {
      await withRetry(() => shopify.collect.create({ product_id: shopifyProductId, collection_id: collectionId }));
      const collectionName = Object.keys(collectionIds).find(key => collectionIds[key] === collectionId);
      productSyncLogger.info(`Added product ID: ${shopifyProductId} to collection: ${collectionName} (ID: ${collectionId})`);
    }
  } catch (error) {
    productSyncLogger.error(`Failed to assign collections for product ID: ${shopifyProductId}: ${error.message}`);
  }
}

// Product sync function
async function syncProducts() {
  const startTime = Date.now();
  let syncError = null;

  // Initialize syncStats at the start
  global.syncStats = { created: 0, updated: 0, skipped: 0, failed: 0, deleted: 0 };

  // Initialize failure log file
  const failureLogFile = path.join(LOGS_DIR, `failed_product_uploads_${timestamp}.log`);
  const failureLogger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf(({ timestamp, level, message }) => `${timestamp} - ${level}: ${message}`)
    ),
    transports: [
      new winston.transports.File({ filename: failureLogFile }),
      new winston.transports.Console()
    ],
  });

  try {
    await connectToMongoDB();
    const locationId = await testShopifyConnection();
    if (!locationId) {
      throw new Error('No location ID available; cannot proceed with inventory updates');
    }

    mainLogger.info('Starting product sync from Smiffys to Shopify');
    const smiffysProducts = await fetchSmiffysProducts();
    const shopifyProducts = await transformSmiffysToShopify(smiffysProducts);

    // Create a set of current generic codes for discontinued product check
    const currentGenericCodes = new Set(shopifyProducts.map(p => p.genericCode));

    await cleanUpDuplicates();

    const { existingProducts, globalProductMapping } = await fetchExistingProducts(shopifyProducts);
    mainLogger.info(`Found ${existingProducts.size} existing products for ${shopifyProducts.length} products to sync`);

    // Remove discontinued products
    await removeDiscontinuedProducts(currentGenericCodes);

    if (shopifyProducts.length > 0) {
      mainLogger.info(`Syncing ${shopifyProducts.length} products to Shopify`);
      const productIds = [];
      const totalBatches = Math.ceil(shopifyProducts.length / PRODUCT_SYNC_CONCURRENCY);
      for (let i = 0; i < shopifyProducts.length; i += PRODUCT_SYNC_CONCURRENCY) {
        const batch = shopifyProducts.slice(i, i + PRODUCT_SYNC_CONCURRENCY);
        const batchNumber = i / PRODUCT_SYNC_CONCURRENCY + 1;
        const batchPromises = batch.map(product => syncProduct(product, existingProducts, globalProductMapping, failureLogger));
        const batchResults = await Promise.all(batchPromises);
        productIds.push(...batchResults.filter(id => id));
        mainLogger.info(`Synced batch ${batchNumber}/${totalBatches}: ${batchResults.length} products`);
        await saveProductMapping(globalProductMapping, batchNumber, totalBatches);
        await setTimeoutPromise(REST_RATE_LIMIT_DELAY);
      }

      // Calculate and log the total number of products on the site
      const totalProductsOnSite = global.syncStats.created + global.syncStats.updated + global.syncStats.skipped - global.syncStats.deleted;
      mainLogger.info(`Shopify product sync completed. Total products on site: ${totalProductsOnSite} (Created: ${global.syncStats.created}, Updated: ${global.syncStats.updated}, Skipped: ${global.syncStats.skipped}, Failed: ${global.syncStats.failed}, Deleted: ${global.syncStats.deleted})`);
      productSyncLogger.info(`Total products on site after sync: ${totalProductsOnSite}`);
    } else {
      // If no products to sync, report existing products minus deleted
      const totalProductsOnSite = existingProducts.size - global.syncStats.deleted;
      mainLogger.info(`No products to sync to Shopify. Total products on site: ${totalProductsOnSite} (Deleted: ${global.syncStats.deleted})`);
      productSyncLogger.info(`Total products on site after sync: ${totalProductsOnSite}`);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    mainLogger.info(`Product sync completed in ${duration} seconds`);
    await sendLogEmail(timestamp, duration, syncError);
  } catch (error) {
    syncError = error;
    mainLogger.error(`Product sync failed: ${error.message}`);
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    await sendLogEmail(timestamp, duration, syncError);
    throw error;
  }
}

// Order sync function
async function syncOrders() {
  const startTime = Date.now();
  let syncError = null;

  try {
    await connectToMongoDB();
    const locationId = await testShopifyConnection();
    if (!locationId) {
      throw new Error('No location ID available; cannot proceed with order sync');
    }

    mainLogger.info('Starting order sync from Shopify to Smiffys');
    const shopifyOrders = await fetchShopifyOrders();
    if (shopifyOrders.length > 0) {
      mainLogger.info(`Processing ${shopifyOrders.length} orders to send to Smiffys`);
      for (const order of shopifyOrders) {
        try {
          const orderXml = transformOrderToSmiffys(order);
          await submitOrderToSmiffys(orderXml);
          await updateShopifyOrder(order.id);
          await fulfillShopifyOrder(order.id, order.line_items);
          orderSyncLogger.info(`Successfully processed order ${order.order_number}`);
        } catch (error) {
          orderSyncLogger.error(`Failed to process order ${order.order_number}: ${error.message}`);
          // Continue processing the next order instead of failing the entire sync
        }
      }
      mainLogger.info(`Completed order sync. Processed ${shopifyOrders.length} orders`);
    } else {
      mainLogger.info('No orders to sync to Smiffys');
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    mainLogger.info(`Order sync completed in ${duration} seconds`);
    await sendLogEmail(timestamp, duration, syncError);
  } catch (error) {
    syncError = error;
    mainLogger.error(`Order sync failed: ${error.message}`);
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    await sendLogEmail(timestamp, duration, syncError);
    throw error;
  }
}

// Main function to run based on arguments
async function main() {
  mainLogger.info('Starting Smiffys to Shopify sync');
  const startTime = Date.now();

  if (FORCE_CREATE) {
    mainLogger.warn('FORCE_CREATE flag is enabled. This will create new products even if they already exist, potentially causing duplicates.');
  }

  try {
    if (!RUN_PRODUCTS && !RUN_ORDERS) {
      await syncProducts();
      await syncOrders();
    } else {
      if (RUN_PRODUCTS) await syncProducts();
      if (RUN_ORDERS) await syncOrders();
    }
  } catch (error) {
    mainLogger.error(`Main function failed: ${error.message}`);
    process.exit(1);
  } finally {
    if (mongoClient) {
      await mongoClient.close();
      mainLogger.info('Closed MongoDB connection');
    }
  }
}

// Run the script
main().catch(err => {
  mainLogger.error(`Unhandled error in main: ${err.message}`);
  process.exit(1);
});