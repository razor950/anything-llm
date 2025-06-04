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
        splitByFilename: string, // TODO
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
    this.config = config;
    this.#splitter = this.#setSplitter(config);
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

  #setSplitter(config = {}) {
    // Determine splitter type
    const splitterType = config.splitterType || 'recursive';
    
    switch (splitterType) {
      case 'character':
        return new CharacterSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          separator: config?.separator || "\n\n",
          keepSeparator: config?.keepSeparator || false,
        });
      
      case 'token':
        return new TokenSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          encodingName: config?.encodingName || 'gpt2',
          allowedSpecial: config?.allowedSpecial || [],
          disallowedSpecial: config?.disallowedSpecial || 'all',
        });
      
      case 'markdown':
        return new MarkdownSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
        });
      
      case 'latex':
        return new LatexSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
        });
      
      case 'html':
        return new HtmlSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
        });
      
      case 'recursive':
      default:
        return new RecursiveSplitter({
          chunkSize: isNaN(config?.chunkSize) ? 1_000 : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap) ? 20 : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          separators: config?.separators,
          keepSeparator: config?.keepSeparator !== false,
        });
    }
  }

  async splitText(documentText) {
    return this.#splitter._splitText(documentText);
  }
}

// Wrapper for Langchain default RecursiveCharacterTextSplitter class.
class RecursiveSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null, separators, keepSeparator = true }) {
    const {
      RecursiveCharacterTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with RecursiveCharacterTextSplitter:`, { chunkSize, chunkOverlap });
    this.chunkHeader = chunkHeader;
    this.engine = new RecursiveCharacterTextSplitter({
      chunkSize,
      chunkOverlap,
      separators,
      keepSeparator,
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
  
  /**
   * Create a RecursiveSplitter from a specific language
   * @param {string} language - The programming language
   * @param {object} options - Additional options
   * @returns {RecursiveSplitter} - A configured RecursiveSplitter instance
   */
  static fromLanguage(language, options = {}) {
    const {
      RecursiveCharacterTextSplitter,
    } = require("@langchain/textsplitters");
    
    const separators = RecursiveCharacterTextSplitter.getSeparatorsForLanguage(language);
    return new RecursiveSplitter({
      ...options,
      separators,
      keepSeparator: options.keepSeparator !== false,
    });
  }
}

// Wrapper for Langchain CharacterTextSplitter class
class CharacterSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null, separator = "\n\n", keepSeparator = false }) {
    const {
      CharacterTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with CharacterTextSplitter:`, { chunkSize, chunkOverlap, separator });
    this.chunkHeader = chunkHeader;
    this.engine = new CharacterTextSplitter({
      chunkSize,
      chunkOverlap,
      separator,
      keepSeparator,
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[CharacterSplitter]\x1b[0m ${text}`, ...args);
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

// Wrapper for Langchain TokenTextSplitter class
class TokenSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null, encodingName = "gpt2", allowedSpecial = [], disallowedSpecial = "all" }) {
    const {
      TokenTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with TokenTextSplitter:`, { chunkSize, chunkOverlap, encodingName });
    this.chunkHeader = chunkHeader;
    this.engine = new TokenTextSplitter({
      chunkSize,
      chunkOverlap,
      encodingName,
      allowedSpecial,
      disallowedSpecial,
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[TokenSplitter]\x1b[0m ${text}`, ...args);
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

// Wrapper for Langchain MarkdownTextSplitter class
class MarkdownSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null }) {
    const {
      MarkdownTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with MarkdownTextSplitter:`, { chunkSize, chunkOverlap });
    this.chunkHeader = chunkHeader;
    this.engine = new MarkdownTextSplitter({
      chunkSize,
      chunkOverlap,
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[MarkdownSplitter]\x1b[0m ${text}`, ...args);
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

// Wrapper for Langchain LatexTextSplitter class
class LatexSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null }) {
    const {
      LatexTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with LatexTextSplitter:`, { chunkSize, chunkOverlap });
    this.chunkHeader = chunkHeader;
    this.engine = new LatexTextSplitter({
      chunkSize,
      chunkOverlap,
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[LatexSplitter]\x1b[0m ${text}`, ...args);
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

// Wrapper for HTML splitting using RecursiveCharacterTextSplitter with HTML separators
class HtmlSplitter {
  constructor({ chunkSize, chunkOverlap, chunkHeader = null }) {
    const {
      RecursiveCharacterTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with HTML separators:`, { chunkSize, chunkOverlap });
    this.chunkHeader = chunkHeader;
    this.engine = RecursiveCharacterTextSplitter.fromLanguage("html", {
      chunkSize,
      chunkOverlap,
    });
  }

  log(text, ...args) {
    console.log(`\x1b[35m[HtmlSplitter]\x1b[0m ${text}`, ...args);
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

// Export additional utility functions
TextSplitter.SUPPORTED_LANGUAGES = [
  "cpp",
  "go",
  "java",
  "js",
  "php",
  "proto",
  "python",
  "rst",
  "ruby",
  "rust",
  "scala",
  "swift",
  "markdown",
  "latex",
  "html",
  "sol",
];

/**
 * Create a TextSplitter instance configured for a specific programming language
 * @param {string} language - The programming language
 * @param {object} config - Additional configuration
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.fromLanguage = function(language, config = {}) {
  const {
    RecursiveCharacterTextSplitter,
  } = require("@langchain/textsplitters");
  
  if (!TextSplitter.SUPPORTED_LANGUAGES.includes(language)) {
    throw new Error(`Language ${language} is not supported. Supported languages: ${TextSplitter.SUPPORTED_LANGUAGES.join(', ')}`);
  }
  
  const separators = RecursiveCharacterTextSplitter.getSeparatorsForLanguage(language);
  
  return new TextSplitter({
    ...config,
    splitterType: 'recursive',
    separators,
    keepSeparator: config.keepSeparator !== false,
  });
};

/**
 * Create a TextSplitter instance for Markdown documents
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createMarkdownSplitter = function(config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: 'markdown',
  });
};

/**
 * Create a TextSplitter instance for LaTeX documents
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createLatexSplitter = function(config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: 'latex',
  });
};

/**
 * Create a TextSplitter instance for HTML documents
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createHtmlSplitter = function(config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: 'html',
  });
};

/**
 * Create a TextSplitter instance that splits by tokens
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createTokenSplitter = function(config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: 'token',
  });
};

/**
 * Create a TextSplitter instance that splits by a specific character/separator
 * @param {object} config - Configuration options (must include 'separator')
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createCharacterSplitter = function(config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: 'character',
  });
};

module.exports.TextSplitter = TextSplitter;
