// utils.js
const winston = require('winston');
const path = require('path');
const fs = require('fs').promises;
const nodemailer = require('nodemailer');
const { promisify } = require('util');
const setTimeoutPromise = promisify(setTimeout);
const config = require('./config.json');

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

// Timestamp for log filenames
const timestamp = new Date().toISOString().replace(/:/g, '-').split('.')[0];

// Logger setup with separate log files
const loggerConfig = {
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} - ${level}: ${message}`)
  )
};

// Main logger for general messages
const mainLogger = winston.createLogger({
  ...loggerConfig,
  transports: [
    new winston.transports.File({ filename: path.join(LOGS_DIR, `main_sync_${timestamp}.log`) }),
    new winston.transports.Console()
  ]
});

// Logger for product sync
const productSyncLogger = winston.createLogger({
  ...loggerConfig,
  transports: [
    new winston.transports.File({ filename: path.join(LOGS_DIR, `product_sync_${timestamp}.log`) }),
    new winston.transports.Console()
  ]
});

// Logger for order sync
const orderSyncLogger = winston.createLogger({
  ...loggerConfig,
  transports: [
    new winston.transports.File({ filename: path.join(LOGS_DIR, `order_sync_${timestamp}.log`) }),
    new winston.transports.Console()
  ]
});

// Retry logic for Shopify API calls
async function withRetry(fn, retries = 5, delay = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const result = await fn();
      return result;
    } catch (error) {
      if (error.response && [429, 403].includes(error.response.status)) {
        const retryAfter = error.response.headers['retry-after'] || 'unknown';
        mainLogger.warn(`Retry ${i + 1}/${retries} after ${error.message}. Retry-After: ${retryAfter}s`);
      }
      if (i === retries - 1 || !error.response || ![429, 403].includes(error.response.status)) {
        mainLogger.error(`Retry failed after ${retries} attempts: ${error.message}`);
        throw error;
      }
      await new Promise(resolve => setTimeout(resolve, delay * (i + 1)));
    }
  }
}

// Function to send email with log files
async function sendLogEmail(timestamp, duration, syncError, type = 'product') {
  mainLogger.info(`Preparing to send log email for ${type} sync`);

  // Create a transporter using SMTP
  const transporter = nodemailer.createTransport({
    host: config.email.smtpHost,
    port: config.email.smtpPort,
    secure: false, // Use TLS
    auth: {
      user: config.email.senderEmail,
      pass: config.email.senderPassword
    }
  });

  // Define log files to attach based on type
  const logFiles = [
    path.join(LOGS_DIR, `main_sync_${timestamp}.log`),
    type === 'product'
      ? path.join(LOGS_DIR, `product_sync_${timestamp}.log`)
      : path.join(LOGS_DIR, `order_sync_${timestamp}.log`)
  ];

  // Prepare attachments
  const attachments = [];
  for (const logFile of logFiles) {
    try {
      await fs.access(logFile); // Check if file exists
      attachments.push({
        path: logFile
      });
    } catch (error) {
      mainLogger.warn(`Log file ${logFile} not found, skipping attachment: ${error.message}`);
    }
  }

  // Prepare email subject with timestamp
  const emailSubject = config.email.subject.replace('{{timestamp}}', timestamp);

  // Prepare email body
  const emailBody = `
    <h2>Smiffys to Shopify ${type.charAt(0).toUpperCase() + type.slice(1)} Sync Completed</h2>
    <p><strong>Timestamp:</strong> ${timestamp}</p>
    <p><strong>Duration:</strong> ${duration} seconds</p>
    <p><strong>Status:</strong> ${syncError ? 'Failed' : 'Success'}</p>
    ${syncError ? `<p><strong>Error:</strong> ${syncError.message}</p>` : ''}
    <p>Please find the log files attached for more details.</p>
  `;

  // Email options
  const mailOptions = {
    from: config.email.senderEmail,
    to: config.email.recipientEmail,
    subject: emailSubject,
    html: emailBody,
    attachments: attachments
  };

  // Send the email
  try {
    await transporter.sendMail(mailOptions);
    mainLogger.info(`Log email for ${type} sync sent successfully to ${config.email.recipientEmail}`);
  } catch (error) {
    mainLogger.error(`Failed to send log email for ${type} sync: ${error.message}`);
  }
}

module.exports = {
  mainLogger,
  productSyncLogger,
  orderSyncLogger,
  withRetry,
  sendLogEmail,
  setTimeoutPromise
};