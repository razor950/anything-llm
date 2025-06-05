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

// File extension to splitter type mapping
const FILE_EXTENSION_TO_SPLITTER = {
  // Markdown files
  ".md": "markdown",
  ".markdown": "markdown",
  ".mdown": "markdown",
  ".mkd": "markdown",
  ".mdx": "markdown",

  // LaTeX files
  ".tex": "latex",
  ".latex": "latex",
  ".ltx": "latex",

  // HTML files
  ".html": "html",
  ".htm": "html",
  ".xhtml": "html",
  ".xml": "html", // XML can use HTML splitter

  // Programming languages
  ".js": { type: "language", language: "js" },
  ".jsx": { type: "language", language: "js" },
  ".ts": { type: "language", language: "js" },
  ".tsx": { type: "language", language: "js" },
  ".mjs": { type: "language", language: "js" },
  ".cjs": { type: "language", language: "js" },

  ".py": { type: "language", language: "python" },
  ".pyw": { type: "language", language: "python" },
  ".pyi": { type: "language", language: "python" },

  ".java": { type: "language", language: "java" },
  ".class": { type: "language", language: "java" },

  ".cpp": { type: "language", language: "cpp" },
  ".cc": { type: "language", language: "cpp" },
  ".cxx": { type: "language", language: "cpp" },
  ".c++": { type: "language", language: "cpp" },
  ".hpp": { type: "language", language: "cpp" },
  ".h": { type: "language", language: "cpp" },
  ".hh": { type: "language", language: "cpp" },
  ".hxx": { type: "language", language: "cpp" },

  ".go": { type: "language", language: "go" },

  ".rs": { type: "language", language: "rust" },

  ".php": { type: "language", language: "php" },
  ".phtml": { type: "language", language: "php" },
  ".php3": { type: "language", language: "php" },
  ".php4": { type: "language", language: "php" },
  ".php5": { type: "language", language: "php" },
  ".php7": { type: "language", language: "php" },
  ".phps": { type: "language", language: "php" },

  ".rb": { type: "language", language: "ruby" },
  ".rbw": { type: "language", language: "ruby" },

  ".swift": { type: "language", language: "swift" },

  ".scala": { type: "language", language: "scala" },
  ".sc": { type: "language", language: "scala" },

  ".proto": { type: "language", language: "proto" },

  ".sol": { type: "language", language: "sol" },

  ".rst": { type: "language", language: "rst" },

  // Plain text files - use character splitter
  ".txt": { type: "character", separator: "\n\n" },
  ".log": { type: "character", separator: "\n" },
  ".csv": { type: "character", separator: "\n" },
  ".tsv": { type: "character", separator: "\n" },

  // JSON/Config files - use character splitter
  ".json": { type: "character", separator: "\n" },
  ".yml": { type: "character", separator: "\n" },
  ".yaml": { type: "character", separator: "\n" },
  ".toml": { type: "character", separator: "\n" },
  ".ini": { type: "character", separator: "\n" },
  ".conf": { type: "character", separator: "\n" },
  ".config": { type: "character", separator: "\n" },
};

class TextSplitter {
  #splitter;
  constructor(config = {}) {
    /*
      config can be a ton of things depending on what is required or optional by the specific splitter.
      {
        splitByFilename: string, // Filename or path to determine splitter type
        splitterType: string, // Explicit splitter type (overrides filename detection)
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

  /**
   * Determine splitter configuration from filename/extension
   * @param {string} filename - The filename or path
   * @returns {object|null} - Splitter configuration or null
   */
  static #getSplitterConfigFromFilename(filename) {
    if (!filename || typeof filename !== "string") return null;

    const extension = path.extname(filename).toLowerCase();
    const config = FILE_EXTENSION_TO_SPLITTER[extension];

    if (!config) return null;

    // If config is a string, it's a direct splitter type
    if (typeof config === "string") {
      return { splitterType: config };
    }

    // If it's an object with type 'language', configure language splitter
    if (config.type === "language") {
      return {
        splitterType: "recursive",
        separators: RecursiveSplitter.getSeparatorsForLanguage(config.language),
        keepSeparator: true,
      };
    }

    // If it's an object with type 'character', configure character splitter
    if (config.type === "character") {
      return {
        splitterType: "character",
        separator: config.separator,
        keepSeparator: false,
      };
    }

    return null;
  }

  #setSplitter(config = {}) {
    // If splitByFilename is provided and no explicit splitterType is set,
    // try to determine the splitter type from the filename
    if (config.splitByFilename && !config.splitterType) {
      const fileConfig = TextSplitter.#getSplitterConfigFromFilename(
        config.splitByFilename
      );
      if (fileConfig) {
        this.log(
          `Auto-detected splitter config for ${config.splitByFilename}:`,
          fileConfig
        );
        // Merge file-based config with user config (user config takes precedence)
        config = { ...fileConfig, ...config };
      } else {
        this.log(
          `No specific splitter found for ${config.splitByFilename}, using default recursive splitter`
        );
      }
    }

    // Determine splitter type
    const splitterType = config.splitterType || "recursive";

    switch (splitterType) {
      case "character":
        return new CharacterSplitter({
          chunkSize: isNaN(config?.chunkSize)
            ? 1_000
            : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap)
            ? 20
            : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          separator: config?.separator || "\n\n",
          keepSeparator: config?.keepSeparator || false,
        });

      case "token":
        return new TokenSplitter({
          chunkSize: isNaN(config?.chunkSize)
            ? 1_000
            : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap)
            ? 20
            : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
          encodingName: config?.encodingName || "gpt2",
          allowedSpecial: config?.allowedSpecial || [],
          disallowedSpecial: config?.disallowedSpecial || "all",
        });

      case "markdown":
        return new MarkdownSplitter({
          chunkSize: isNaN(config?.chunkSize)
            ? 1_000
            : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap)
            ? 20
            : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
        });

      case "latex":
        return new LatexSplitter({
          chunkSize: isNaN(config?.chunkSize)
            ? 1_000
            : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap)
            ? 20
            : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
        });

      case "html":
        return new HtmlSplitter({
          chunkSize: isNaN(config?.chunkSize)
            ? 1_000
            : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap)
            ? 20
            : Number(config?.chunkOverlap),
          chunkHeader: this.stringifyHeader(),
        });

      case "recursive":
      default:
        return new RecursiveSplitter({
          chunkSize: isNaN(config?.chunkSize)
            ? 1_000
            : Number(config?.chunkSize),
          chunkOverlap: isNaN(config?.chunkOverlap)
            ? 20
            : Number(config?.chunkOverlap),
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
  constructor({
    chunkSize,
    chunkOverlap,
    chunkHeader = null,
    separators,
    keepSeparator = true,
  }) {
    const {
      RecursiveCharacterTextSplitter,
    } = require("@langchain/textsplitters");
    this.log(`Will split with`, { chunkSize, chunkOverlap });
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
   * Get separators for a specific language from LangChain
   * @param {string} language - The programming language
   * @returns {string[]} - Array of separators
   */
  static getSeparatorsForLanguage(language) {
    const {
      RecursiveCharacterTextSplitter,
    } = require("@langchain/textsplitters");
    return RecursiveCharacterTextSplitter.getSeparatorsForLanguage(language);
  }
}

// Wrapper for Langchain CharacterTextSplitter class
class CharacterSplitter {
  constructor({
    chunkSize,
    chunkOverlap,
    chunkHeader = null,
    separator = "\n\n",
    keepSeparator = false,
  }) {
    const { CharacterTextSplitter } = require("@langchain/textsplitters");
    this.log(`Will split with CharacterTextSplitter:`, {
      chunkSize,
      chunkOverlap,
      separator,
    });
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
  constructor({
    chunkSize,
    chunkOverlap,
    chunkHeader = null,
    encodingName = "gpt2",
    allowedSpecial = [],
    disallowedSpecial = "all",
  }) {
    const { TokenTextSplitter } = require("@langchain/textsplitters");
    this.log(`Will split with TokenTextSplitter:`, {
      chunkSize,
      chunkOverlap,
      encodingName,
    });
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
    const { MarkdownTextSplitter } = require("@langchain/textsplitters");
    this.log(`Will split with MarkdownTextSplitter:`, {
      chunkSize,
      chunkOverlap,
    });
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
    const { LatexTextSplitter } = require("@langchain/textsplitters");
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
TextSplitter.fromLanguage = function (language, config = {}) {
  const {
    RecursiveCharacterTextSplitter,
  } = require("@langchain/textsplitters");

  if (!TextSplitter.SUPPORTED_LANGUAGES.includes(language)) {
    throw new Error(
      `Language ${language} is not supported. Supported languages: ${TextSplitter.SUPPORTED_LANGUAGES.join(", ")}`
    );
  }

  const separators =
    RecursiveCharacterTextSplitter.getSeparatorsForLanguage(language);

  return new TextSplitter({
    ...config,
    splitterType: "recursive",
    separators,
    keepSeparator: config.keepSeparator !== false,
  });
};

/**
 * Create a TextSplitter instance based on file extension
 * @param {string} filename - The filename or path
 * @param {object} config - Additional configuration
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.fromExtension = function (filename, config = {}) {
  return new TextSplitter({
    ...config,
    splitByFilename: filename,
  });
};

/**
 * Create a TextSplitter instance for Markdown documents
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createMarkdownSplitter = function (config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: "markdown",
  });
};

/**
 * Create a TextSplitter instance for LaTeX documents
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createLatexSplitter = function (config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: "latex",
  });
};

/**
 * Create a TextSplitter instance for HTML documents
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createHtmlSplitter = function (config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: "html",
  });
};

/**
 * Create a TextSplitter instance that splits by tokens
 * @param {object} config - Configuration options
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createTokenSplitter = function (config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: "token",
  });
};

/**
 * Create a TextSplitter instance that splits by a specific character/separator
 * @param {object} config - Configuration options (must include 'separator')
 * @returns {TextSplitter} - A configured TextSplitter instance
 */
TextSplitter.createCharacterSplitter = function (config = {}) {
  return new TextSplitter({
    ...config,
    splitterType: "character",
  });
};

module.exports.TextSplitter = TextSplitter;
