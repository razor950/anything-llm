/**
 * @typedef {object} DocumentMetadata
 * @property {string} id - eg; "123e4567-e89b-12d3-a456-426614174000"
 * @property {string} url - eg; "file://example.com/index.html"
 * @property {string} title - eg; "example.com/index.html"
 * @property {string} docAuthor - eg; "no author found"
 * @property {string} description - eg; "No description found."
 * @property {string} docSource - eg; "URL link uploaded by the user."
 * @property {string} chunkSource - eg; link://https://example.com
 * @property {string} published - ISO 8601 date string
 * @property {number} wordCount - Number of words in the document
 * @property {string} pageContent - The raw text content of the document
 * @property {number} token_count_estimate - Number of tokens in the document
 */

const path = require("path");

function isNullOrNaN(value) {
  if (value === null) return true;
  return isNaN(value);
}

class TextSplitter {
  #splitter;
  constructor(config = {}) {
    /*
      config can be a ton of things depending on what is required or optional by the specific splitter.
      Non-splitter related keys
      {
        splitByFilename: string,
        strategy: 'recursive' | 'semantic' | 'sentence' | 'markdown',
        preserveStructure: boolean,
        semanticChunking: boolean,
        minChunkSize: number,
        contentType: string,
      }
      ------
      Default: "RecursiveCharacterTextSplitter"
      Config: {
        chunkSize: number,
        chunkOverlap: number,
        chunkHeaderMeta: object | null, // Gets appended to top of each chunk as metadata
      }
      ------
    */
    this.config = {
      strategy: "semantic",
      preserveStructure: true,
      semanticChunking: true,
      minChunkSize: 100,
      ...config
    };
    this.#splitter = this.#setSplitter(this.config);
  }

  log(text, ...args) {
    console.log(`\x1b[35m[TextSplitter]\x1b[0m ${text}`, ...args);
  }

  /**
   *  Does a quick check to determine the text chunk length limit.
   * Embedder models have hard-set limits that cannot be exceeded, just like an LLM context
   * so here we want to allow override of the default 1000, but up to the models maximum, which is
   * sometimes user defined.
   */
  static determineMaxChunkSize(preferred = null, embedderLimit = 1000) {
    const prefValue = isNullOrNaN(preferred)
      ? Number(embedderLimit)
      : Number(preferred);
    const limit = Number(embedderLimit);
    if (prefValue > limit)
      console.log(
        `\x1b[43m[WARN]\x1b[0m Text splitter chunk length of ${prefValue} exceeds embedder model max of ${embedderLimit}. Will use ${embedderLimit}.`
      );
    return prefValue > limit ? limit : prefValue;
  }

  /**
   * Determines optimal chunk size based on content analysis
   * @param {string} text - The text to analyze
   * @param {number} embedderLimit - Maximum chunk size allowed by embedder
   * @returns {number} Optimal chunk size
   */
  static determineOptimalChunkSize(text, embedderLimit = 1000) {
    // Analyze text characteristics
    const sentences = text.match(/[^.!?]+[.!?]+/g) || [];
    const avgSentenceLength = sentences.length > 0 
      ? sentences.reduce((sum, s) => sum + s.length, 0) / sentences.length 
      : 100;
    
    const paragraphs = text.split(/\n\n+/);
    const avgParagraphLength = paragraphs.length > 0
      ? paragraphs.reduce((sum, p) => sum + p.length, 0) / paragraphs.length
      : 500;

    // Determine optimal size based on content structure
    let optimal;
    if (avgParagraphLength < embedderLimit * 0.5) {
      // Short paragraphs - use larger chunks to maintain context
      optimal = Math.min(embedderLimit * 0.8, avgParagraphLength * 3);
    } else if (avgSentenceLength > 150) {
      // Long sentences (likely technical content) - use medium chunks
      optimal = Math.min(embedderLimit * 0.6, avgSentenceLength * 5);
    } else {
      // Normal content - use default sizing
      optimal = Math.min(embedderLimit * 0.7, 1000);
    }

    return Math.floor(optimal);
  }

  /**
   *  Creates a string of metadata to be prepended to each chunk.
   * @param {DocumentMetadata} metadata - Metadata to be prepended to each chunk.
   * @returns {{[key: ('title' | 'published' | 'source')]: string}} Object of metadata that will be prepended to each chunk.
   */
  static buildHeaderMeta(metadata = {}) {
    if (!metadata || Object.keys(metadata).length === 0) return null;
    const PLUCK_MAP = {
      title: {
        as: "sourceDocument",
        pluck: (metadata) => {
          return metadata?.title || null;
        },
      },
      published: {
        as: "published",
        pluck: (metadata) => {
          return metadata?.published || null;
        },
      },
      chunkSource: {
        as: "source",
        pluck: (metadata) => {
          const validPrefixes = ["link://", "youtube://"];
          // If the chunkSource is a link or youtube link, we can add the URL
          // as its source in the metadata so the LLM can use it for context.
          // eg prompt: Where did you get this information? -> answer: "from https://example.com"
          if (
            !metadata?.chunkSource || // Exists
            !metadata?.chunkSource.length || // Is not empty
            typeof metadata.chunkSource !== "string" || // Is a string
            !validPrefixes.some(
              (prefix) => metadata.chunkSource.startsWith(prefix) // Has a valid prefix we respect
            )
          )
            return null;

          // We know a prefix is present, so we can split on it and return the rest.
          // If nothing is found, return null and it will not be added to the metadata.
          let source = null;
          for (const prefix of validPrefixes) {
            source = metadata.chunkSource.split(prefix)?.[1] || null;
            if (source) break;
          }

          return source;
        },
      },
    };

    const pluckedData = {};
    Object.entries(PLUCK_MAP).forEach(([key, value]) => {
      if (!(key in metadata)) return; // Skip if the metadata key is not present.
      const pluckedValue = value.pluck(metadata);
      if (!pluckedValue) return; // Skip if the plucked value is null/empty.
      pluckedData[value.as] = pluckedValue;
    });

    return pluckedData;
  }

  /**
   *  Creates a string of metadata to be prepended to each chunk.
   */
  stringifyHeader() {
    if (!this.config.chunkHeaderMeta) return null;
    let content = "";
    Object.entries(this.config.chunkHeaderMeta).map(([key, value]) => {
      if (!key || !value) return;
       content += `${key}: ${value}\n`;
    });

    if (!content) return null;
    return `<document_metadata>\n${content}</document_metadata>\n\n`;
  }

  /**
   * Detects content type from filename or content analysis
   * @param {string} filename - Optional filename to analyze
   * @param {string} content - Optional content to analyze
   * @returns {string} Detected content type
   */
  static detectContentType(filename = null, content = null) {
    if (filename) {
      const ext = path.extname(filename).toLowerCase();
      const typeMap = {
        '.md': 'markdown',
        '.py': 'code',
        '.js': 'code',
        '.ts': 'code',
        '.jsx': 'code',
        '.tsx': 'code',
        '.java': 'code',
        '.cpp': 'code',
        '.c': 'code',
        '.cs': 'code',
        '.php': 'code',
        '.rb': 'code',
        '.go': 'code',
        '.rs': 'code',
        '.csv': 'structured',
        '.json': 'structured',
        '.xml': 'structured',
        '.yaml': 'structured',
        '.yml': 'structured'
      };
      if (typeMap[ext]) return typeMap[ext];
    }

    if (content) {
      // Simple content type detection
      if (content.includes('```') || content.match(/^#{1,6}\s/m)) return 'markdown';
      if (content.match(/^\s*(function|class|const|let|var|def|import|export)\s/m)) return 'code';
      if (content.match(/^[{\[]/) && content.match(/[}\]]$/)) return 'structured';
    }

    return 'text';
  }

  #setSplitter(config = {}) {
    const contentType = config.contentType || 
      TextSplitter.detectContentType(config.splitByFilename);
    // Choose strategy based on content type and config
    const strategy = config.strategy || this.#determineStrategy(contentType);

    switch (strategy) {
      case 'semantic':
        return new SemanticSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 100 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          minChunkSize: config.minChunkSize || 100,
          preserveStructure: config.preserveStructure !== false
        });

      case 'markdown':
        return new MarkdownSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 50 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader()
        });

      case 'code':
        return new CodeSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 50 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          language: config.language
        });

      default:
        return new RecursiveSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader()
        });
    }
  }

  #determineStrategy(contentType) {
    const strategyMap = {
      'markdown': 'markdown',
      'code': 'code',
      'structured': 'recursive',
      'text': 'semantic'
    };
    return strategyMap[contentType] || 'semantic';
  }

  async splitText(documentText) {
    const chunks = await this.#splitter._splitText(documentText);
    // Post-process chunks for quality
    return this.#postProcessChunks(chunks, documentText);
  }

  #postProcessChunks(chunks, originalText) {
    const processedChunks = [];
    chunks.forEach((chunk, index) => {
      // Skip empty or too small chunks
      if (!chunk || chunk.trim().length < this.config.minChunkSize) {
        this.log(`Skipping chunk ${index}: too small (${chunk.length} chars)`);
        return;
      }
      // Add position metadata
      const startPos = originalText.indexOf(chunk);
      const enrichedChunk = this.#enrichChunkMetadata(chunk, {
        chunkIndex: index,
        totalChunks: chunks.length,
        startPosition: startPos,
        endPosition: startPos + chunk.length,
        isFirst: index === 0,
        isLast: index === chunks.length - 1
      });
      processedChunks.push(enrichedChunk);
    });
    return processedChunks;
  }

  #enrichChunkMetadata(chunk, metadata) {
    // For now, just return the chunk as is
    // In a full implementation, we could prepend metadata to each chunk
    return chunk;
  }
}

// Semantic-aware splitter that preserves meaning and context
class SemanticSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader, minChunkSize, preserveStructure }) {
    this.chunkSize = chunkSize;
    this.chunkOverlap = chunkOverlap;
    this.chunkHeader = chunkHeader;
    this.minChunkSize = minChunkSize;
    this.preserveStructure = preserveStructure;
    this.log(`Initialized with semantic chunking`, { chunkSize, chunkOverlap });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[SemanticSplitter]\x1b[0m ${text}`, ...args);
  }

  async _splitText(documentText) {
    // First, split by paragraphs to maintain structure
    const paragraphs = this.#splitIntoParagraphs(documentText);
    const chunks = [];
    let currentChunk = this.chunkHeader || '';
    let currentSize = currentChunk.length;

    for (let i = 0; i < paragraphs.length; i++) {
      const paragraph = paragraphs[i];
      const paragraphSize = paragraph.length;

      // If adding this paragraph would exceed chunk size, finalize current chunk
      if (
        currentSize + paragraphSize > this.chunkSize &&
        currentChunk.length > this.minChunkSize
      ) {
        // Add overlap from current paragraph if possible
        const overlapText = this.#createOverlap(paragraph, this.chunkOverlap);
        if (overlapText) {
          chunks.push(currentChunk.trim());
          currentChunk = (this.chunkHeader || '') + overlapText;
          currentSize = currentChunk.length;
        } else {
          chunks.push(currentChunk.trim());
          currentChunk = this.chunkHeader || '';
          currentSize = currentChunk.length;
        }
      }

      // Add paragraph to current chunk
      currentChunk +=
        (currentChunk.length > (this.chunkHeader?.length || 0) ? "\n\n" : "") +
        paragraph;
      currentSize = currentChunk.length;

      // If current chunk exceeds size, split it semantically
      if (currentSize > this.chunkSize) {
        const splitChunks = await this.#splitLargeParagraph(currentChunk, paragraph);
        chunks.push(...splitChunks.slice(0, -1));
        currentChunk = splitChunks[splitChunks.length - 1];
        currentSize = currentChunk.length;
      }
    }

    // Add final chunk
    if (currentChunk.length > (this.chunkHeader?.length || 0)) {
      chunks.push(currentChunk.trim());
    }

    return chunks.filter((chunk) => chunk && chunk.length >= this.minChunkSize);
  }

  #splitIntoParagraphs(text) {
    // Split by double newlines, but preserve structure
    const paragraphs = text.split(/\n\n+/);
    return paragraphs.filter((p) => p.trim().length > 0);
  }

  #createOverlap(text, overlapSize) {
    // Create semantic overlap by finding sentence boundaries
    const sentences = text.match(/[^.!?]+[.!?]+/g) || [];
    if (sentences.length === 0) return text.substring(0, overlapSize);

    let overlap = '';
    for (const sentence of sentences) {
      if (overlap.length + sentence.length > overlapSize) break;
      overlap += sentence;
    }

    return overlap || text.substring(0, overlapSize);
  }

  async #splitLargeParagraph(currentChunk, paragraph) {
    // Split by sentences for semantic coherence
    const sentences = paragraph.match(/[^.!?]+[.!?]+/g) || [paragraph];
    const chunks = [];
    let tempChunk = this.chunkHeader || '';

    for (const sentence of sentences) {
      if (
        tempChunk.length + sentence.length > this.chunkSize &&
        tempChunk.length > this.minChunkSize
      ) {
        chunks.push(tempChunk.trim());
        // Add overlap
        const overlapText = this.#createOverlap(sentence, this.chunkOverlap);
        tempChunk = (this.chunkHeader || '') + overlapText;
      }
      tempChunk += sentence;
    }

    if (tempChunk.length > (this.chunkHeader?.length || 0)) {
      chunks.push(tempChunk.trim());
    }

    return chunks.length > 0 ? chunks : [currentChunk];
  }
}

// Markdown-aware splitter
class MarkdownSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader }) {
    const { RecursiveCharacterTextSplitter } = require("@langchain/textsplitters");
    this.log(`Initialized for Markdown content`, { chunkSize, chunkOverlap });
    this.chunkHeader = chunkHeader;

    // Use markdown-specific separators
    this.engine = new RecursiveCharacterTextSplitter({
      chunkSize,
      chunkOverlap,
      separators: [
        "\n## ",  // H2 headers
        "\n### ", // H3 headers
        "\n#### ", // H4 headers
        "\n\n",   // Paragraphs
        "\n",     // Lines
        " ",      // Words
        ""        // Characters
      ],
      keepSeparator: true
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[MarkdownSplitter]\x1b[0m ${text}`, ...args);
  }

  async _splitText(documentText) {
    if (!this.chunkHeader) return this.engine.splitText(documentText);

    const strings = await this.engine.splitText(documentText);
    const processedChunks = [];

    for (const chunk of strings) {
      // Ensure each chunk starts with a header if possible
      const enrichedChunk = this.#ensureHeaderContext(chunk, documentText);
      processedChunks.push(this.chunkHeader + enrichedChunk);
    }

    return processedChunks;
  }

  #ensureHeaderContext(chunk, fullText) {
    // Try to find the nearest header before this chunk
    const chunkStart = fullText.indexOf(chunk);
    if (chunkStart === -1) return chunk;

    const textBefore = fullText.substring(0, chunkStart);
    const headerMatch = textBefore.match(/(^|\n)(#{1,6}\s+[^\n]+)(?:\n|$)/g);

    if (headerMatch && headerMatch.length > 0) {
      const lastHeader = headerMatch[headerMatch.length - 1].trim();
      // Only add if not already in chunk
      if (!chunk.includes(lastHeader)) {
        return `${lastHeader}\n\n${chunk}`;
      }
    }

    return chunk;
  }
}

// Code-aware splitter
class CodeSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader, language }) {
    const { RecursiveCharacterTextSplitter } = require("@langchain/textsplitters");
    this.log(`Initialized for code content`, { chunkSize, chunkOverlap, language });
    this.chunkHeader = chunkHeader;
    this.language = language;

    // Use code-specific separators
    const separators = this.#getLanguageSeparators(language);
    this.engine = new RecursiveCharacterTextSplitter({
      chunkSize,
      chunkOverlap,
      separators,
      keepSeparator: true
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[CodeSplitter]\x1b[0m ${text}`, ...args);
  }

  #getLanguageSeparators(language) {
    // Language-specific separators for better code splitting
    const commonSeparators = [
      "\n\nclass ",    // Class definitions
      "\n\ndef ",      // Python functions
      "\n\nfunction ", // JavaScript functions
      "\n\nconst ",    // Const declarations
      "\n\nlet ",      // Let declarations
      "\n\nvar ",      // Var declarations
      "\n\nexport ",   // Export statements
      "\n\nimport ",   // Import statements
      "\n\n",          // Double newline
      "\n",            // Single newline
      " ",             // Space
      ""               // Character
    ];

    // Add language-specific separators
    const langSpecific = {
      python: ["\n\nclass ", "\n\ndef ", "\n\nasync def ", "\n\nif __name__"],
      javascript: ["\n\nfunction ", "\n\nconst ", "\n\nlet ", "\n\nclass ", "\n\nexport "],
      typescript: ["\n\ninterface ", "\n\ntype ", "\n\nenum ", ...commonSeparators],
      java: ["\n\npublic class ", "\n\nprivate ", "\n\nprotected ", "\n\npublic "],
    };

    return langSpecific[language] || commonSeparators;
  }

  async _splitText(documentText) {
    if (!this.chunkHeader) return this.engine.splitText(documentText);

    const strings = await this.engine.splitText(documentText);
    const processedChunks = [];

    for (const chunk of strings) {
      // Ensure each chunk is syntactically complete if possible
      const validChunk = this.#ensureSyntaxValidity(chunk);
      processedChunks.push(this.chunkHeader + validChunk);
    }

    return processedChunks;
  }

  #ensureSyntaxValidity(chunk) {
    // Basic syntax validation - ensure balanced braces
    const openBraces = (chunk.match(/{/g) || []).length;
    const closeBraces = (chunk.match(/}/g) || []).length;

    if (openBraces > closeBraces) {
      // Try to add closing braces
      chunk += "\n" + "}".repeat(openBraces - closeBraces);
    }

    return chunk;
  }
}

// Wrapper for Langchain default RecursiveCharacterTextSplitter class.
class RecursiveSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null }) {
    const {
      RecursiveCharacterTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with`, { chunkSize, chunkOverlap });
    this.chunkHeader = chunkHeader;
    this.engine = new RecursiveCharacterTextSplitter({
      chunkSize,
      chunkOverlap,
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[RecursiveSplitter]\x1b[0m ${text}`, ...args);
  }

  async _splitText(documentText) {
    if (!this.chunkHeader) return this.engine.splitText(documentText);
    const strings = await this.engine.splitText(documentText);
    const documents = await this.engine.createDocuments(strings, [], {
      chunkHeader: this.chunkHeader,
    });
    return documents
      .filter((doc) => !!doc.pageContent)
      .map((doc) => doc.pageContent);
  }
}

module.exports.TextSplitter = TextSplitter;
