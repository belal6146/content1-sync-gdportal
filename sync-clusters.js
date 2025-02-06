const { Client: OpenSearchClient } = require('@opensearch-project/opensearch');
const { Client: ElasticClient } = require('@elastic/elasticsearch');
const debug = require('debug');
require('dotenv').config();

// Setup debug loggers
const logInfo = (...args) => {
  debug('sync:info')(...args);
  console.log('[INFO]', ...args);
};
const logError = (...args) => {
  debug('sync:error')(...args);
  console.error('[ERROR]', ...args);
};
const logDebug = (...args) => {
  debug('sync:debug')(...args);
  console.debug('[DEBUG]', ...args);
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
  console.error('Missing required environment variables:', missingEnvVars.join(', '));
  process.exit(1);
}

// Configure OpenSearch client (source)
const sourceClient = new OpenSearchClient({
  node: `https://${process.env.AWS_OPENSEARCH_USERNAME}:${process.env.AWS_OPENSEARCH_PASSWORD}@${process.env.AWS_OPENSEARCH_ENDPOINT}`,
  ssl: { rejectUnauthorized: false },
  requestTimeout: 120000
});

// Configure Elastic client (target)
const targetClient = new ElasticClient({
  cloud: { id: process.env.ELASTIC_CLOUD_ID },
  auth: {
    apiKey: {
      id: process.env.ELASTIC_API_KEY_ID,
      api_key: process.env.ELASTIC_API_KEY_SECRET
    }
  },
  tls: { rejectUnauthorized: true },
  requestTimeout: 120000
});

// Function to check if a document has changed (fixed logic)
async function hasDocumentChanged(index, docId, newData) {
  try {
    const response = await targetClient.get({ index, id: docId });

    if (response.body && response.body._source) {
      const existingData = response.body._source;

      // Compare full document JSON
      if (JSON.stringify(existingData) === JSON.stringify(newData)) {
        return false; // No update needed
      }
    }
  } catch (error) {
    if (error.meta && error.meta.statusCode === 404) {
      return true; // Document does not exist, needs to be created
    }
    logError(`Error checking document ${docId}:`, error.message);
  }
  return true; // Default to updating if there's an error
}

// Function to check the document count in Elasticsearch
async function getElasticsearchDocCount(targetIndex) {
  try {
    const response = await targetClient.count({ index: targetIndex });
    return response.body.count;
  } catch (error) {
    logError(`Error fetching Elasticsearch document count: ${error.message}`);
    return 0;
  }
}

// Function to sync data
async function syncData(sourceIndex, targetIndex, batchSize = 100) {
  while (true) {
    try {
      const startTime = Date.now();
      logInfo(`\n===== 🟢 New Sync Job Started at ${new Date().toLocaleString()} =====`);

      // Get total document count in OpenSearch
      const countResponse = await sourceClient.count({ index: sourceIndex });
      if (!countResponse.body) throw new Error('Invalid count response from OpenSearch');

      const totalDocs = countResponse.body.count;
      logInfo(`🔎 Total documents in OpenSearch: ${totalDocs}`);

      if (totalDocs === 0) {
        logInfo('📭 No documents to sync. Exiting...');
        return;
      }

      // Get Elasticsearch document count before sync
      const elasticBeforeCount = await getElasticsearchDocCount(targetIndex);
      logInfo(`📂 Documents in Elasticsearch before sync: ${elasticBeforeCount}`);

      // Initialize counters
      let processedDocs = 0;
      let updatedDocs = 0;
      let skippedDocs = 0;
      let failedDocs = 0;
      let newDocs = 0;
      let batchNumber = 0;

      // Start scroll query with increased timeout
      let scrollResponse = await sourceClient.search({
        index: sourceIndex,
        scroll: '5m',
        size: batchSize,
        body: { query: { match_all: {} } }
      });

      let scrollId = scrollResponse.body._scroll_id;
      let documents = scrollResponse.body.hits.hits;

      while (documents.length > 0) {
        batchNumber++;
        const batchStartTime = Date.now();
        processedDocs += documents.length;

        logInfo(`⚙️ Processing batch #${batchNumber} (${processedDocs}/${totalDocs})`);

        const operations = [];
        for (const doc of documents) {
          const source = { ...doc._source };

          // Ensure jsonPayload is parsed correctly
          if (source.jsonPayload && typeof source.jsonPayload === 'string') {
            try {
              source.jsonPayload = JSON.parse(source.jsonPayload);
            } catch (parseError) {
              logError(`❌ Skipping document ${doc._id} due to JSON parse error.`);
              failedDocs++;
              continue;
            }
          }

          // Check if document has changed
          const needsUpdate = await hasDocumentChanged(targetIndex, doc._id, source);
          if (!needsUpdate) {
            skippedDocs++;
            continue;
          }

          // Add to bulk update operations
          operations.push(
            { update: { _index: targetIndex, _id: doc._id } },
            { doc: source, doc_as_upsert: true }
          );
        }

        if (operations.length > 0) {
          try {
            const bulkResponse = await targetClient.bulk({ refresh: true, body: operations, timeout: '2m' });

            bulkResponse.items.forEach(item => {
              if (item.update && item.update.error) {
                failedDocs++;
              } else {
                updatedDocs++;
              }
            });

          } catch (bulkError) {
            logError(`❌ Bulk indexing error: ${bulkError.message}`);
            failedDocs += operations.length / 2;
          }
        }

        logInfo(`✅ Batch #${batchNumber} completed. Processed: ${processedDocs}, Updated: ${updatedDocs}, Skipped: ${skippedDocs}, Failed: ${failedDocs}`);
      }

      // Check final document count
      const elasticAfterCount = await getElasticsearchDocCount(targetIndex);
      newDocs = elasticAfterCount - elasticBeforeCount;

      const endTime = Date.now();
      const totalTime = ((endTime - startTime) / 60000).toFixed(2);
      logInfo(`✅ Sync Job Completed in ${totalTime} minutes.`);
      logInfo(`🔄 Final Elasticsearch Count: ${elasticAfterCount} (Added: ${newDocs}, Updated: ${updatedDocs}, Skipped: ${skippedDocs}, Failed: ${failedDocs})`);

    } catch (error) {
      logError('❌ Error during sync:', error.message);
    }

    logInfo('🔄 Restarting sync in 1 minute...\n');
    await new Promise(resolve => setTimeout(resolve, 60000));
  }
}

// Start continuous sync
syncData(process.env.SOURCE_INDEX_NAME, process.env.TARGET_INDEX_NAME);
