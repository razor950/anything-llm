const { v4 } = require("uuid");
const { DocxLoader } = require("@langchain/community/document_loaders/fs/docx");
const {
  createdDate,
  trashFile,
  writeToServerDocuments,
} = require("../../utils/files");
const { tokenizeString } = require("../../utils/tokenizer");
const { default: slugify } = require("slugify");

/**
 * Processes a DOCX file and converts it to the internal document format
 * @param {Object} params - The parameters object
 * @param {string} params.fullFilePath - The full path to the DOCX file
 * @param {string} params.filename - The original filename
 * @returns {Promise<{success: boolean, reason: string|null, documents: Array}>}
 */
async function asDocX({ fullFilePath = "", filename = "" }) {
  // Input validation
  if (!fullFilePath || !filename) {
    return {
      success: false,
      reason: "Missing required parameters: fullFilePath and filename are required.",
      documents: [],
    };
  }

  let loader;
  let docs;
  
  try {
    // Initialize loader with proper error handling
    loader = new DocxLoader(fullFilePath);
    console.log(`-- Processing DOCX file: ${filename} --`);
    
    // Load documents with error handling
    docs = await loader.load();
    
    if (!docs || docs.length === 0) {
      console.warn(`No documents found in ${filename}`);
      trashFile(fullFilePath);
      return {
        success: false,
        reason: `No documents could be extracted from ${filename}.`,
        documents: [],
      };
    }

  } catch (error) {
    console.error(`Error loading DOCX file ${filename}:`, error.message);
    trashFile(fullFilePath);
    return {
      success: false,
      reason: `Failed to load DOCX file: ${error.message}`,
      documents: [],
    };
  }

  try {
    // Extract and process content with better filtering
    const pageContent = [];
    let totalPages = 0;
    
    for (const doc of docs) {
      totalPages++;
      
      if (!doc.pageContent || typeof doc.pageContent !== 'string') {
        console.warn(`-- Skipping page ${totalPages}: no valid content --`);
        continue;
      }
      
      // Trim whitespace and check if content is meaningful
      const trimmedContent = doc.pageContent.trim();
      if (trimmedContent.length === 0) {
        console.warn(`-- Skipping page ${totalPages}: empty content after trimming --`);
        continue;
      }
      
      console.log(`-- Extracted content from page ${totalPages} (${trimmedContent.length} characters) --`);
      pageContent.push(trimmedContent);
    }

    // Validate extracted content
    if (pageContent.length === 0) {
      console.error(`No meaningful text content found in ${filename} after processing ${totalPages} pages.`);
      trashFile(fullFilePath);
      return {
        success: false,
        reason: `No meaningful text content found in ${filename}.`,
        documents: [],
      };
    }

    // Combine content with proper spacing
    const combinedContent = pageContent.join('\n\n');
    
    // Validate final content
    if (combinedContent.trim().length === 0) {
      console.error(`Combined content is empty for ${filename}.`);
      trashFile(fullFilePath);
      return {
        success: false,
        reason: `Combined content resulted in empty text for ${filename}.`,
        documents: [],
      };
    }

    // Create document metadata with proper error handling
    let createdDateValue;
    try {
      createdDateValue = createdDate(fullFilePath);
    } catch (error) {
      console.warn(`Could not determine created date for ${filename}, using current date:`, error.message);
      createdDateValue = new Date().toLocaleString();
    }

    // Calculate word count more accurately
    const wordCount = combinedContent
      .split(/\s+/)
      .filter(word => word.trim().length > 0).length;

    // Calculate token count with error handling
    let tokenCount;
    try {
      tokenCount = tokenizeString(combinedContent);
    } catch (error) {
      console.warn(`Could not calculate token count for ${filename}, estimating:`, error.message);
      // Rough estimation: ~4 characters per token
      tokenCount = Math.ceil(combinedContent.length / 4);
    }

    const data = {
      id: v4(),
      url: "file://" + fullFilePath,
      title: filename,
      docAuthor: "no author found",
      description: "No description found.",
      docSource: "docx file uploaded by the user.", // Fixed: was incorrectly labeled as "pdf file"
      chunkSource: "",
      published: createdDateValue,
      wordCount: wordCount,
      pageContent: combinedContent,
      token_count_estimate: tokenCount,
    };

    // Write document with error handling
    let document;
    try {
      document = writeToServerDocuments(
        data,
        `${slugify(filename)}-${data.id}`
      );
    } catch (error) {
      console.error(`Failed to write document for ${filename}:`, error.message);
      trashFile(fullFilePath);
      return {
        success: false,
        reason: `Failed to save processed document: ${error.message}`,
        documents: [],
      };
    }

    // Clean up source file
    try {
      trashFile(fullFilePath);
    } catch (error) {
      console.warn(`Could not remove source file ${fullFilePath}:`, error.message);
      // Don't fail the operation if we can't clean up the source file
    }

    console.log(
      `[SUCCESS]: ${filename} converted successfully ` +
      `(${pageContent.length} pages, ${wordCount} words, ~${tokenCount} tokens) ` +
      `and ready for embedding.\n`
    );
    
    return { 
      success: true, 
      reason: null, 
      documents: [document] 
    };

  } catch (error) {
    console.error(`Unexpected error processing ${filename}:`, error.message);
    trashFile(fullFilePath);
    return {
      success: false,
      reason: `Unexpected error during processing: ${error.message}`,
      documents: [],
    };
  }
}

module.exports = asDocX;