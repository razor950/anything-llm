const { QdrantClient } = require("@qdrant/js-client-rest");
const { TextSplitter } = require("../../TextSplitter");
const { SystemSettings } = require("../../../models/systemSettings");
const { storeVectorResult, cachedVectorInformation } = require("../../files");
const { v4: uuidv4 } = require("uuid");
const { toChunks, getEmbeddingEngineSelection } = require("../../helpers");
const { sourceIdentifier } = require("../../chats");
const { NativeEmbeddingReranker } = require("../../EmbeddingRerankers/native");

const QDrant = {
  name: "QDrant",

  /**
   * Enhanced connection method with better error handling and configuration
   */
  connect: async function () {
    if (process.env.VECTOR_DB !== "qdrant")
      throw new Error("QDrant::Invalid ENV settings");

    const config = {
      url: process.env.QDRANT_ENDPOINT,
      timeout: parseInt(process.env.QDRANT_TIMEOUT) || 300_000, // 5 minutes default
      maxConnections: parseInt(process.env.QDRANT_MAX_CONNECTIONS) || 25,
      ...(process.env.QDRANT_API_KEY
        ? { apiKey: process.env.QDRANT_API_KEY }
        : {}),
    };

    // Add custom headers if specified
    if (process.env.QDRANT_HEADERS) {
      try {
        config.headers = JSON.parse(process.env.QDRANT_HEADERS);
      } catch (e) {
        console.warn("QDrant::Invalid headers format, ignoring:", e.message);
      }
    }

    const client = new QdrantClient(config);

    const isAlive = (await client.api("cluster")?.clusterStatus())?.ok || false;
    if (!isAlive)
      throw new Error(
        "QDrant::Invalid Heartbeat received - is the instance online?"
      );

    return { client };
  },

  heartbeat: async function () {
    await this.connect();
    return { heartbeat: Number(new Date()) };
  },

  totalVectors: async function () {
    const { client } = await this.connect();
    const { collections } = await client.getCollections();
    let totalVectors = 0;

    // Use Promise.all for parallel processing
    const counts = await Promise.all(
      collections.map(async (collection) => {
        if (!collection?.name) return 0;
        try {
          const namespace = await this.namespace(client, collection.name);
          return namespace?.vectorCount || 0;
        } catch (error) {
          console.warn(
            `QDrant::Failed to get count for ${collection.name}:`,
            error.message
          );
          return 0;
        }
      })
    );

    totalVectors = counts.reduce((sum, count) => sum + count, 0);
    return totalVectors;
  },

  namespaceCount: async function (_namespace = null) {
    const { client } = await this.connect();
    const namespace = await this.namespace(client, _namespace);
    return namespace?.vectorCount || 0;
  },

  /**
   * Enhanced similarity search with support for multiple search strategies
   */
  performSimilaritySearch: async function ({
    namespace = null,
    input = "",
    LLMConnector = null,
    similarityThreshold = 0.25,
    topN = 4,
    filterIdentifiers = [],
    rerank = true,
    searchStrategy = "vector",
    contextPairs = [],
  }) {
    if (!namespace || !input || !LLMConnector)
      throw new Error("Invalid request to performSimilaritySearch.");

    const { client } = await this.connect();
    if (!(await this.namespaceExists(client, namespace))) {
      return {
        contextTexts: [],
        sources: [],
        message: "Invalid query - no documents found for workspace!",
      };
    }

    const queryVector = await LLMConnector.embedTextInput(input);
    let searchResults;

    if (searchStrategy === "discovery" && contextPairs.length > 0) {
      searchResults = await this.performDiscoverySearch({
        client,
        namespace,
        queryVector,
        contextPairs,
        topN,
        similarityThreshold,
        filterIdentifiers,
      });
    } else if (searchStrategy === "hybrid") {
      searchResults = await this.performHybridSearch({
        client,
        namespace,
        queryVector,
        input,
        topN,
        similarityThreshold,
        filterIdentifiers,
      });
    } else {
      searchResults = await this.similarityResponse({
        client,
        namespace,
        queryVector,
        similarityThreshold,
        topN,
        filterIdentifiers,
      });
    }

    // Apply reranking if requested
    if (rerank) {
      const reranker = new NativeEmbeddingReranker();
      const documents = searchResults.contextTexts.map((text, i) => ({
        text,
        ...searchResults.sourceDocuments[i],
      }));

      try {
        const rerankedResults = await reranker.rerank(input, documents, {
          topK: topN,
        });

        return {
          contextTexts: rerankedResults.map((doc) => doc.text),
          sources: this.curateSources(rerankedResults),
          message: false,
        };
      } catch (error) {
        console.error("Reranking failed, using original results:", error);
      }
    }

    return {
      contextTexts: searchResults.contextTexts,
      sources: this.curateSources(
        searchResults.sourceDocuments.map((metadata, i) => ({
          ...metadata,
          text: searchResults.contextTexts[i],
        }))
      ),
      message: false,
    };
  },

  /**
   * Discovery search using context pairs for better relevance
   */
  performDiscoverySearch: async function ({
    client,
    namespace,
    queryVector,
    contextPairs,
    topN,
    similarityThreshold,
    filterIdentifiers,
  }) {
    try {
      const responses = await client.discoverPoints(namespace, {
        target: queryVector,
        context: contextPairs,
        limit: topN,
        with_payload: true,
        score_threshold: similarityThreshold,
      });

      const result = {
        contextTexts: [],
        sourceDocuments: [],
        scores: [],
      };

      responses.forEach((response) => {
        if (filterIdentifiers.includes(sourceIdentifier(response?.payload))) {
          console.log(
            "QDrant: A source was filtered from context as it's parent document is pinned."
          );
          return;
        }

        result.contextTexts.push(response?.payload?.text || "");
        result.sourceDocuments.push({
          ...(response?.payload || {}),
          id: response.id,
        });
        result.scores.push(response.score);
      });

      return result;
    } catch (error) {
      console.warn(
        "QDrant::Discovery search failed, falling back to vector search:",
        error.message
      );
      return this.similarityResponse({
        client,
        namespace,
        queryVector,
        similarityThreshold,
        topN,
        filterIdentifiers,
      });
    }
  },

  /**
   * Hybrid search using the new query API
   */
  performHybridSearch: async function ({
    client,
    namespace,
    queryVector,
    input,
    topN,
    similarityThreshold,
    filterIdentifiers,
  }) {
    try {
      const responses = await client.query(namespace, {
        query: queryVector,
        limit: topN,
        with_payload: true,
        score_threshold: similarityThreshold,
        // Add text search as prefetch for hybrid results
        prefetch: [
          {
            query: {
              text: input,
            },
            limit: topN * 2,
          },
        ],
      });

      const result = {
        contextTexts: [],
        sourceDocuments: [],
        scores: [],
      };

      responses.points.forEach((response) => {
        if (filterIdentifiers.includes(sourceIdentifier(response?.payload))) {
          console.log(
            "QDrant: A source was filtered from context as it's parent document is pinned."
          );
          return;
        }

        result.contextTexts.push(response?.payload?.text || "");
        result.sourceDocuments.push({
          ...(response?.payload || {}),
          id: response.id,
        });
        result.scores.push(response.score);
      });

      return result;
    } catch (error) {
      console.warn(
        "QDrant::Hybrid search failed, falling back to vector search:",
        error.message
      );
      return this.similarityResponse({
        client,
        namespace,
        queryVector,
        similarityThreshold,
        topN,
        filterIdentifiers,
      });
    }
  },

  /**
   * Enhanced similarity response using the query API with fallback to search
   */
  similarityResponse: async function ({
    client,
    namespace,
    queryVector,
    similarityThreshold = 0.25,
    topN = 4,
    filterIdentifiers = [],
  }) {
    const result = {
      contextTexts: [],
      sourceDocuments: [],
      scores: [],
    };

    try {
      // Try to use the query API first (newer QDrant versions)
      let responses;
      try {
        const queryResult = await client.query(namespace, {
          query: queryVector,
          limit: topN,
          with_payload: true,
          score_threshold: similarityThreshold,
        });

        // Handle query API response format
        responses = queryResult.points || queryResult;
      } catch (queryError) {
        // Fallback to search API for older QDrant versions
        console.log(
          "QDrant::Query API not available, falling back to search API"
        );
        responses = await client.search(namespace, {
          vector: queryVector,
          limit: topN,
          with_payload: true,
          score_threshold: similarityThreshold,
        });
      }

      responses.forEach((response) => {
        if (response.score < similarityThreshold) return;
        if (filterIdentifiers.includes(sourceIdentifier(response?.payload))) {
          console.log(
            "QDrant: A source was filtered from context as it's parent document is pinned."
          );
          return;
        }

        result.contextTexts.push(response?.payload?.text || "");
        result.sourceDocuments.push({
          ...(response?.payload || {}),
          id: response.id,
        });
        result.scores.push(response.score);
      });

      return result;
    } catch (error) {
      console.error("QDrant::Similarity search failed:", error.message);
      throw error;
    }
  },

  namespace: async function (client, namespace = null) {
    if (!namespace) throw new Error("No namespace value provided.");

    try {
      const collection = await client.getCollection(namespace);
      if (!collection) return null;

      const countResult = await client.count(namespace, { exact: true });

      return {
        name: namespace,
        ...collection,
        vectorCount: countResult.count,
      };
    } catch (error) {
      console.error(`QDrant::namespace error for ${namespace}:`, error.message);
      return null;
    }
  },

  hasNamespace: async function (namespace = null) {
    if (!namespace) return false;
    const { client } = await this.connect();
    return await this.namespaceExists(client, namespace);
  },

  namespaceExists: async function (client, namespace = null) {
    if (!namespace) throw new Error("No namespace value provided.");

    try {
      const exists = await client.collectionExists(namespace);
      return exists.exists;
    } catch (error) {
      console.error("QDrant::namespaceExists", error.message);
      return false;
    }
  },

  deleteVectorsInNamespace: async function (client, namespace = null) {
    try {
      await client.deleteCollection(namespace);
      return true;
    } catch (error) {
      console.error(
        `QDrant::Failed to delete collection ${namespace}:`,
        error.message
      );
      throw error;
    }
  },

  /**
   * Enhanced collection creation with better configuration options
   */
  getOrCreateCollection: async function (client, namespace, dimensions = null) {
    if (await this.namespaceExists(client, namespace)) {
      return await client.getCollection(namespace);
    }

    if (!dimensions) {
      throw new Error(
        `Qdrant:getOrCreateCollection Unable to infer vector dimension from input. Open an issue on GitHub for support.`
      );
    }

    // Enhanced collection configuration
    const collectionConfig = {
      vectors: {
        size: dimensions,
        distance: "Cosine",
      },
      // Optimize for performance
      optimizers_config: {
        max_segment_size: 20000,
        memmap_threshold: 50000,
        indexing_threshold: 20000,
        flush_interval_sec: 5,
        max_optimization_threads: 1,
      },
      // Configure HNSW for better performance
      hnsw_config: {
        m: 16,
        ef_construct: 100,
        full_scan_threshold: 10000,
        max_indexing_threads: 0,
        on_disk: false,
      },
      // Enable WAL for durability
      wal_config: {
        wal_capacity_mb: 32,
        wal_segments_ahead: 0,
      },
    };

    await client.createCollection(namespace, collectionConfig);
    return await client.getCollection(namespace);
  },

  /**
   * Enhanced document addition with batch processing and better error handling
   */
  addDocumentToNamespace: async function (
    namespace,
    documentData = {},
    fullFilePath = null,
    skipCache = false
  ) {
    const { DocumentVectors } = require("../../../models/vectors");
    try {
      let vectorDimension = null;
      const { pageContent, docId, ...metadata } = documentData;
      if (!pageContent || pageContent.length == 0) return false;

      console.log("Adding new vectorized document into namespace", namespace);

      // Handle cached results with improved error handling
      if (skipCache) {
        const cacheResult = await cachedVectorInformation(fullFilePath);
        if (cacheResult.exists) {
          return await this.processCachedVectors(cacheResult, namespace, docId);
        }
      }

      // Process new document with enhanced chunking
      const EmbedderEngine = getEmbeddingEngineSelection();
      const textSplitter = new TextSplitter({
        chunkSize: TextSplitter.determineMaxChunkSize(
          await SystemSettings.getValueOrFallback({
            label: "text_splitter_chunk_size",
          }),
          EmbedderEngine?.embeddingMaxChunkLength
        ),
        chunkOverlap: await SystemSettings.getValueOrFallback(
          { label: "text_splitter_chunk_overlap" },
          20
        ),
        chunkHeaderMeta: TextSplitter.buildHeaderMeta(metadata),
      });

      const textChunks = await textSplitter.splitText(pageContent);
      console.log("Chunks created from document:", textChunks.length);

      // Process embeddings in batches for better performance
      const batchSize = 50; // Configurable batch size
      const documentVectors = [];
      const allVectors = [];

      for (let i = 0; i < textChunks.length; i += batchSize) {
        const batch = textChunks.slice(i, i + batchSize);
        const vectorValues = await EmbedderEngine.embedChunks(batch);

        if (!vectorValues || vectorValues.length === 0) {
          throw new Error(
            `Failed to embed batch ${Math.floor(i / batchSize) + 1}`
          );
        }

        for (let j = 0; j < vectorValues.length; j++) {
          const vector = vectorValues[j];
          if (!vectorDimension) vectorDimension = vector.length;

          const vectorRecord = {
            id: uuidv4(),
            vector: vector,
            payload: { ...metadata, text: batch[j] },
          };

          allVectors.push(vectorRecord);
          documentVectors.push({ docId, vectorId: vectorRecord.id });
        }
      }

      // Create collection and insert vectors
      const { client } = await this.connect();
      const collection = await this.getOrCreateCollection(
        client,
        namespace,
        vectorDimension
      );

      if (!collection) {
        throw new Error("Failed to create new QDrant collection!", {
          namespace,
        });
      }

      // Insert vectors in batches
      await this.batchInsertVectors(client, namespace, allVectors);

      // Store cache and database records
      if (fullFilePath) {
        await storeVectorResult(
          allVectors.map((v) => [{ vector: v.vector, payload: v.payload }]),
          fullFilePath
        );
      }

      await DocumentVectors.bulkInsert(documentVectors);
      return { vectorized: true, error: null };
    } catch (error) {
      console.error("addDocumentToNamespace", error.message);
      return { vectorized: false, error: error.message };
    }
  },

  /**
   * Process cached vectors with enhanced error handling
   */
  processCachedVectors: async function (cacheResult, namespace, docId) {
    const { DocumentVectors } = require("../../../models/vectors");
    const { client } = await this.connect();
    const { chunks } = cacheResult;
    const documentVectors = [];

    if (!chunks || chunks.length === 0) {
      throw new Error("Invalid cache result: no chunks found");
    }

    const vectorDimension =
      chunks[0][0]?.vector?.length ?? chunks[0][0]?.values?.length ?? null;

    const collection = await this.getOrCreateCollection(
      client,
      namespace,
      vectorDimension
    );
    if (!collection) {
      throw new Error("Failed to create new QDrant collection!", { namespace });
    }

    // Process chunks in batches
    for (const chunk of chunks) {
      const vectors = [];

      chunk.forEach((chunkItem) => {
        const id = uuidv4();
        if (chunkItem?.payload?.hasOwnProperty("id")) {
          const { id: _id, ...payload } = chunkItem.payload;
          documentVectors.push({ docId, vectorId: id });
          vectors.push({
            id,
            vector: chunkItem.vector,
            payload,
          });
        } else {
          console.error(
            "The 'id' property is not defined in chunk.payload - skipping chunk"
          );
        }
      });

      if (vectors.length > 0) {
        await this.batchInsertVectors(client, namespace, vectors);
      }
    }

    await DocumentVectors.bulkInsert(documentVectors);
    return { vectorized: true, error: null };
  },

  /**
   * Enhanced batch vector insertion with retry logic
   */
  batchInsertVectors: async function (client, namespace, vectors, retries = 3) {
    const batchSize = 100; // Optimal batch size for QDrant

    for (let i = 0; i < vectors.length; i += batchSize) {
      const batch = vectors.slice(i, i + batchSize);
      const submission = {
        points: batch.map((v) => ({
          id: v.id,
          vector: v.vector,
          payload: v.payload,
        })),
      };

      for (let attempt = 0; attempt < retries; attempt++) {
        try {
          const result = await client.upsert(namespace, submission);
          if (result.status !== "completed") {
            throw new Error(`Upsert failed: ${result.status}`);
          }
          break; // Success, exit retry loop
        } catch (error) {
          if (attempt === retries - 1) {
            throw new Error(
              `Failed to insert batch after ${retries} attempts: ${error.message}`
            );
          }

          // Wait before retry (exponential backoff)
          const delay = Math.pow(2, attempt) * 1000;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }
  },

  /**
   * Enhanced document deletion with better error handling
   */
  deleteDocumentFromNamespace: async function (namespace, docId) {
    const { DocumentVectors } = require("../../../models/vectors");

    try {
      const { client } = await this.connect();
      if (!(await this.namespaceExists(client, namespace))) return true;

      const knownDocuments = await DocumentVectors.where({ docId });
      if (knownDocuments.length === 0) return true;

      const vectorIds = knownDocuments.map((doc) => doc.vectorId);

      // Delete in batches to avoid large request payloads
      const batchSize = 100;
      for (let i = 0; i < vectorIds.length; i += batchSize) {
        const batch = vectorIds.slice(i, i + batchSize);
        await client.delete(namespace, {
          points: batch,
          wait: true,
        });
      }

      const indexes = knownDocuments.map((doc) => doc.id);
      await DocumentVectors.deleteIds(indexes);
      return true;
    } catch (error) {
      console.error(
        `QDrant::Failed to delete document ${docId}:`,
        error.message
      );
      throw error;
    }
  },

  /**
   * Enhanced namespace statistics with additional metrics
   */
  "namespace-stats": async function (reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error("namespace required");

    const { client } = await this.connect();
    if (!(await this.namespaceExists(client, namespace))) {
      throw new Error("Namespace by that name does not exist.");
    }

    try {
      const [stats, collection] = await Promise.all([
        this.namespace(client, namespace),
        client.getCollection(namespace),
      ]);

      return {
        ...stats,
        collection_info: {
          config: collection.config,
          status: collection.status,
          optimizer_status: collection.optimizer_status,
        },
        performance_metrics: {
          indexed_vectors_count: collection.indexed_vectors_count || 0,
          points_count: collection.points_count || 0,
        },
      };
    } catch (error) {
      console.error(
        `QDrant::Failed to get namespace stats for ${namespace}:`,
        error.message
      );
      return {
        message: "No stats were able to be fetched from DB for namespace",
        error: error.message,
      };
    }
  },

  /**
   * Enhanced namespace deletion with confirmation
   */
  "delete-namespace": async function (reqBody = {}) {
    const { namespace = null } = reqBody;
    if (!namespace) throw new Error("namespace required");

    const { client } = await this.connect();
    if (!(await this.namespaceExists(client, namespace))) {
      throw new Error("Namespace by that name does not exist.");
    }

    try {
      const details = await this.namespace(client, namespace);
      await this.deleteVectorsInNamespace(client, namespace);

      return {
        message: `Namespace ${namespace} was deleted along with ${details?.vectorCount || 0} vectors.`,
        deleted_vectors: details?.vectorCount || 0,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      console.error(
        `QDrant::Failed to delete namespace ${namespace}:`,
        error.message
      );
      throw error;
    }
  },

  /**
   * Enhanced reset with better cleanup
   */
  reset: async function () {
    const { client } = await this.connect();

    try {
      const response = await client.getCollections();
      const deletePromises = response.collections.map((collection) =>
        client
          .deleteCollection(collection.name)
          .catch((err) =>
            console.warn(
              `Failed to delete collection ${collection.name}:`,
              err.message
            )
          )
      );

      await Promise.all(deletePromises);
      return {
        reset: true,
        deleted_collections: response.collections.length,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      console.error("QDrant::Reset failed:", error.message);
      throw error;
    }
  },

  curateSources: function (sources = []) {
    const documents = [];
    for (const source of sources) {
      if (Object.keys(source).length > 0) {
        const metadata = source.hasOwnProperty("metadata")
          ? source.metadata
          : source;
        documents.push({ ...metadata });
      }
    }
    return documents;
  },
};

module.exports.QDrant = QDrant;
