const { Client: OpenSearchClient } = require('@opensearch-project/opensearch');
const { Client: ElasticClient } = require('@elastic/elasticsearch');
const debug = require('debug');
require('dotenv').config();

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
const logRequest = (...args) => {
  debug('sync:request')(...args);
  console.debug('[REQUEST]', new Date().toISOString(), ...args);
};
const logResponse = (...args) => {
  debug('sync:response')(...args);
  console.debug('[RESPONSE]', new Date().toISOString(), ...args);
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

// Configure clients for both clusters
const sourceClient = new OpenSearchClient({
  node: `https://${process.env.AWS_OPENSEARCH_USERNAME}:${process.env.AWS_OPENSEARCH_PASSWORD}@${process.env.AWS_OPENSEARCH_ENDPOINT}`,
  ssl: { rejectUnauthorized: false },
  requestTimeout: 120000,
});

const targetClient = new ElasticClient({
  cloud: { id: process.env.ELASTIC_CLOUD_ID },
  auth: { apiKey: { id: process.env.ELASTIC_API_KEY_ID, api_key: process.env.ELASTIC_API_KEY_SECRET } },
  tls: { rejectUnauthorized: true },
  requestTimeout: 120000,
});

async function syncData(sourceIndex, targetIndex, batchSize = 200) {
  try {
    const startTime = new Date(); // Start time for logging

    // Check if target index exists
    const indexExists = await targetClient.indices.exists({ index: targetIndex });

    if (!indexExists) {
      logInfo(`Target index ${targetIndex} does not exist. Creating...`);

      const sourceMapping = await sourceClient.indices.getMapping({ index: sourceIndex });

      if (!sourceMapping.body) throw new Error('Invalid mapping response from OpenSearch');

      // Modify mapping before creating
      sourceMapping.body[sourceIndex].mappings.properties.jsonPayload.type = "object";
      delete sourceMapping.body[sourceIndex].mappings.properties.jsonPayload.fields;
      delete sourceMapping.body[sourceIndex].mappings.properties.marketVariation.analyzer;
      delete sourceMapping.body[sourceIndex].mappings.properties.brand.analyzer;

      logInfo(`Creating target index ${targetIndex} with mapping: ${JSON.stringify(sourceMapping.body[sourceIndex].mappings)}`);

      await targetClient.indices.create({
        index: targetIndex,
        body: { mappings: sourceMapping.body[sourceIndex].mappings },
      });

      logInfo(`Created target index ${targetIndex}`);
    }

    // Get total number of documents in source index
    const countResponse = await sourceClient.count({ index: sourceIndex });

    if (!countResponse.body) throw new Error('Invalid count response from OpenSearch');

    const totalDocs = countResponse.body.count;
    logInfo(`Total documents to sync: ${totalDocs}`);
    logDebug(`Starting sync from ${sourceIndex} to ${targetIndex}`);

    let processedDocs = 0;
    let failedDocs = 0;

    let scrollResponse;
    try {
      scrollResponse = await sourceClient.search({
        index: sourceIndex,
        scroll: '1m',
        size: batchSize,
        body: { query: { match_all: {} } },
      });
    } catch (searchError) {
      logError('Error initializing scroll search:', searchError);
      throw searchError;
    }

    if (!scrollResponse.body || !scrollResponse.body.hits) throw new Error('Invalid scroll response from OpenSearch');

    let scrollId = scrollResponse.body._scroll_id;
    let documents = scrollResponse.body.hits.hits;

    while (documents.length > 0) {
      processedDocs += documents.length;
      logInfo(`Processing batch of ${documents.length} documents (${processedDocs}/${totalDocs})`);
      logDebug(`Current scroll ID: ${scrollId}`);

      const operations = documents.flatMap(doc => {
        const source = { ...doc._source };

        if (source.jsonPayload && typeof source.jsonPayload === 'string') {
          try {
            source.jsonPayload = JSON.parse(source.jsonPayload);
          } catch (parseError) {
            logError(`Failed to parse jsonPayload for document ${doc._id}:`, parseError);
            failedDocs++;
          }
        }

        return [{ index: { _index: targetIndex, _id: doc._id } }, source];
      });

      let retries = 3;
      while (retries > 0) {
        try {
          const bulkResponse = await targetClient.bulk({
            refresh: true,
            body: operations,
            timeout: '2m',
          });

          if (bulkResponse.errors) {
            failedDocs += bulkResponse.items.filter(item => item.index && item.index.error).length;
            logError('Bulk operation had errors:', bulkResponse.items.filter(item => item.index && item.index.error));
          }

          break;
        } catch (bulkError) {
          retries--;
          if (retries === 0) {
            logError('Error during bulk indexing (all retries failed):', bulkError);
            failedDocs += documents.length;
            throw bulkError;
          }
          logError(`Bulk indexing failed, retrying (${retries} attempts remaining):`, bulkError);
          await new Promise(resolve => setTimeout(resolve, (3 - retries) * 1000));
        }
      }

      try {
        const scrollResult = await sourceClient.scroll({
          scroll_id: scrollId,
          scroll: '1m',
        });

        if (!scrollResult.body || !scrollResult.body.hits) throw new Error('Invalid scroll result from OpenSearch');

        scrollId = scrollResult.body._scroll_id;
        documents = scrollResult.body.hits.hits;
        logDebug(`Retrieved next batch: ${documents.length} documents`);
      } catch (scrollError) {
        logError('Error during scroll:', scrollError);
        throw scrollError;
      }
    }

    try {
      await sourceClient.clearScroll({ body: { scroll_id: scrollId } });
    } catch (clearScrollError) {
      logError('Error clearing scroll:', clearScrollError);
    }

    const endTime = new Date();
    const duration = (endTime - startTime) / 1000;
    logInfo(`First sync cycle completed in ${duration.toFixed(2)} seconds.`);
    logInfo(`Total records processed: ${processedDocs}`);
    logInfo(`Total records failed: ${failedDocs}`);

    logInfo('Sync cycle completed successfully. Restarting process...');
    setTimeout(() => syncData(sourceIndex, targetIndex, batchSize), 5000); // Restart after 5s

  } catch (error) {
    logError('Error during sync:', error);
    throw error;
  }
}

// Start the sync process in a loop
async function main() {
  while (true) {
    try {
      await syncData(process.env.SOURCE_INDEX_NAME, process.env.TARGET_INDEX_NAME);
    } catch (error) {
      console.error('Sync failed:', error);
    }
    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait before restarting
  }
}

main();
