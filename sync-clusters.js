const { Client: OpenSearchClient } = require('@opensearch-project/opensearch');
const { Client: ElasticClient } = require('@elastic/elasticsearch');
const debug = require('debug');
require('dotenv').config();

// Constants
const BASE_DELAY = 10000; // Minimum delay (10 sec) between bulk operations
const MAX_DELAY = 60000; // Max delay if Elasticsearch is struggling (60 sec)
const INITIAL_BATCH_SIZE = 200; // Default batch size
const MAX_RETRIES = 5; // Max retry attempts for failed bulk inserts

// Setup debug loggers
const logInfo = (...args) => {
  debug('sync:info')(...args);
  console.log('[INFO]', new Date().toISOString(), ...args);
};
const logError = (...args) => {
  debug('sync:error')(...args);
  console.error('[ERROR]', new Date().toISOString(), ...args);
};
const logDebug = (...args) => {
  debug('sync:debug')(...args);
  console.debug('[DEBUG]', new Date().toISOString(), ...args);
};

// Validate required environment variables
const requiredEnvVars = [
  'AWS_OPENSEARCH_ENDPOINT',
  'AWS_OPENSEARCH_USERNAME',
  'AWS_OPENSEARCH_PASSWORD',
  'ELASTIC_CLOUD_ID',
  'ELASTIC_API_KEY_ID',
  'ELASTIC_API_KEY_SECRET',
  'SOURCE_INDEX_NAME',
  'TARGET_INDEX_NAME'
];

const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingEnvVars.length > 0) {
  logError('Missing required environment variables:', missingEnvVars.join(', '));
  process.exit(1);
}

// Configure OpenSearch Client
const sourceClient = new OpenSearchClient({
  node: `https://${process.env.AWS_OPENSEARCH_USERNAME}:${process.env.AWS_OPENSEARCH_PASSWORD}@${process.env.AWS_OPENSEARCH_ENDPOINT}`,
  ssl: { rejectUnauthorized: false },
  requestTimeout: 120000,
});

// Configure Elasticsearch Client
const targetClient = new ElasticClient({
  cloud: { id: process.env.ELASTIC_CLOUD_ID },
  auth: { apiKey: { id: process.env.ELASTIC_API_KEY_ID, api_key: process.env.ELASTIC_API_KEY_SECRET } },
  tls: { rejectUnauthorized: true },
  requestTimeout: 120000,
});

// Sync function
async function syncData(sourceIndex, targetIndex) {
  try {
    const startTime = Date.now();
    let batchSize = INITIAL_BATCH_SIZE;
    let delay = BASE_DELAY;

    logInfo(`Starting sync for ${sourceIndex} → ${targetIndex}`);

    // Check if the target index exists
    const indexExists = await targetClient.indices.exists({ index: targetIndex });
    if (!indexExists) {
      logInfo(`Target index ${targetIndex} does not exist. Creating...`);
      const sourceMapping = await sourceClient.indices.getMapping({ index: sourceIndex });

      if (!sourceMapping.body) throw new Error('Invalid mapping response from OpenSearch');

      await targetClient.indices.create({
        index: targetIndex,
        body: { mappings: sourceMapping.body[sourceIndex].mappings },
      });

      logInfo(`Target index ${targetIndex} created successfully`);
    }

    // Get total document count
    const countResponse = await sourceClient.count({ index: sourceIndex });
    if (!countResponse.body) throw new Error('Invalid count response from OpenSearch');

    const totalDocs = countResponse.body.count;
    logInfo(`Total documents to sync: ${totalDocs}`);

    let processedDocs = 0;
    let failedDocs = 0;
    let scrollResponse = await sourceClient.search({
      index: sourceIndex,
      scroll: '2m',
      size: batchSize,
      body: { query: { match_all: {} } },
    });

    let scrollId = scrollResponse.body._scroll_id;
    let documents = scrollResponse.body.hits.hits;

    while (documents.length > 0) {
      logInfo(`Processing batch of ${documents.length} docs (Total processed: ${processedDocs}/${totalDocs})`);

      // Prepare bulk request
      const operations = documents.flatMap(doc => {
        const source = { ...doc._source };

        if (source.jsonPayload && typeof source.jsonPayload === 'string') {
          try {
            source.jsonPayload = JSON.parse(source.jsonPayload);
          } catch (parseError) {
            logError(`Failed to parse jsonPayload for doc ${doc._id}:`, parseError);
            failedDocs++;
          }
        }

        return [{ index: { _index: targetIndex, _id: doc._id } }, source];
      });

      // Bulk Insert with Smart Retry
      let success = false;
      let retries = 0;
      while (!success && retries < MAX_RETRIES) {
        try {
          const bulkResponse = await targetClient.bulk({ refresh: true, body: operations, timeout: '2m' });

          if (bulkResponse.errors) {
            failedDocs += bulkResponse.items.filter(item => item.index?.error).length;
            logError('Bulk operation had errors:', bulkResponse.items.filter(item => item.index?.error));
          }

          success = true;
        } catch (bulkError) {
          retries++;
          logError(`Bulk insert failed (Attempt ${retries}/${MAX_RETRIES}). Retrying in ${delay / 1000}s...`, bulkError);
          await new Promise(resolve => setTimeout(resolve, delay));

          // Increase delay for next retry
          delay = Math.min(delay * 2, MAX_DELAY);
        }
      }

      processedDocs += documents.length;

      // Wait before fetching the next batch
      logInfo(`⏳ Waiting ${delay / 1000}s before fetching the next batch...`);
      await new Promise(resolve => setTimeout(resolve, delay));

      // Fetch next batch
      const scrollResult = await sourceClient.scroll({ scroll_id: scrollId, scroll: '2m' });
      scrollId = scrollResult.body._scroll_id;
      documents = scrollResult.body.hits.hits;
    }

    // Clear the scroll session
    await sourceClient.clearScroll({ body: { scroll_id: scrollId } });

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    logInfo(`Sync completed in ${duration}s`);
    logInfo(`Processed: ${processedDocs} |  Failed: ${failedDocs}`);

  } catch (error) {
    logError('Error during sync:', error);
  }
}

// Start sync job every 2 hours
async function main() {
  while (true) {
    await syncData(process.env.SOURCE_INDEX_NAME, process.env.TARGET_INDEX_NAME);
    logInfo('Waiting 2 hours before next sync...');
    await new Promise(resolve => setTimeout(resolve, 2 * 60 * 60 * 1000)); // 2 hours
  }
}

main();
