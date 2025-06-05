const { v4 } = require("uuid");
const {
  createdDate,
  trashFile,
  writeToServerDocuments,
} = require("../../../utils/files");
const { tokenizeString } = require("../../../utils/tokenizer");
const { default: slugify } = require("slugify");
const { parsePdf, pdf2string } = require("afpp");
const fs = require("fs").promises;
const OCRLoader = require("../../../utils/OCRLoader");
const pdfjsLib = require("pdfjs-dist");

async function extractPDFMetadata(pdfBuffer, password = null) {
  try {
    const loadingTask = pdfjsLib.getDocument({
      data: new Uint8Array(pdfBuffer),
      password: password,
      verbosity: 0, // Quiet mode - no console spam
      useWorkerFetch: false,
      isEvalSupported: false,
      useSystemFonts: true
    });
    
    const pdfDocument = await loadingTask.promise;
    const metadata = await pdfDocument.getMetadata();
    
    // Clean up the document to free memory
    await pdfDocument.cleanup();
    await pdfDocument.destroy();
    
    return {
      author: metadata.info?.Author || metadata.info?.Creator || "no author found",
      title: metadata.info?.Title || "No description found.",
      subject: metadata.info?.Subject || null,
      creator: metadata.info?.Creator || null,
      producer: metadata.info?.Producer || null,
      creationDate: metadata.info?.CreationDate || null,
      modificationDate: metadata.info?.ModDate || null,
      keywords: metadata.info?.Keywords || null,
      totalPages: pdfDocument.numPages,
      // Include any custom metadata
      customMetadata: metadata.metadata || {}
    };
  } catch (error) {
    console.log(`[PDF Metadata] Could not extract metadata: ${error.message}`);
    return {
      author: "no author found",
      title: "No description found.",
      totalPages: null
    };
  }
}

async function asPdf({ fullFilePath = "", filename = "", options = {} }) {
  console.log(`-- Working ${filename} --`);
  
  try {
    // Read the file once for both metadata and content extraction
    const pdfBuffer = await fs.readFile(fullFilePath);
    
    // Extract metadata first
    const pdfMeta = await extractPDFMetadata(pdfBuffer, options?.password);
    
    // First, try simple text extraction
    let pageTexts = [];
    let needsOCR = false;
    const ocrPages = [];

    // Parse PDF with mixed content support to detect image-only pages
    const parseResults = await parsePdf(
      pdfBuffer,
      {
        concurrency: 4, // Process 4 pages concurrently for better performance
        password: options?.password, // Support encrypted PDFs
        scale: 2.0, // Good quality for image extraction if needed
        imageEncoding: 'png'
      },
      async (content, pageNumber, totalPages) => {
        console.log(`-- Parsing content from pg ${pageNumber}/${totalPages} --`);

        if (typeof content === 'string' && content.trim()) {
          // Page has text content
          return {
            pageNumber,
            type: 'text',
            content: content.trim(),
            needsOCR: false
          };
        } else {
          // Page is an image or has no text - mark for OCR
          console.log(`[asPDF] Page ${pageNumber} has no text content, marking for OCR`);
          return {
            pageNumber,
            type: 'image',
            content: content, // This is an image buffer
            needsOCR: true
          };
        }
      }
    );

    // Separate text pages from pages needing OCR
    for (const result of parseResults) {
      if (result.type === 'text') {
        pageTexts.push(result.content);
      } else {
        ocrPages.push(result);
        needsOCR = true;
      }
    }

    // If we have pages that need OCR, process them
    if (needsOCR && ocrPages.length > 0) {
      console.log(`[asPDF] ${ocrPages.length} pages need OCR processing for ${filename}`);
      
      try {
        // Use the existing OCR loader for pages that need it
        const ocrDocs = await new OCRLoader({
          targetLanguages: options?.ocr?.langList,
        }).ocrPDF(fullFilePath);

        // Extract text from OCR results
        for (const doc of ocrDocs) {
          if (doc.pageContent && doc.pageContent.length) {
            pageTexts.push(doc.pageContent);
          }
        }
      } catch (ocrError) {
        console.error(`[asPDF] OCR processing failed for ${filename}:`, ocrError.message);
        // Continue with whatever text we extracted
      }
    }

    // If still no content after OCR attempt, try simple text extraction as fallback
    if (pageTexts.length === 0) {
      console.log(`[asPDF] Attempting simple text extraction as final fallback for ${filename}`);
      try {
        const simpleTextPages = await pdf2string(pdfBuffer, {
          password: options?.password
        });
        pageTexts = simpleTextPages.filter(text => text.trim());
      } catch (textError) {
        console.error(`[asPDF] Simple text extraction also failed for ${filename}:`, textError.message);
      }
    }

    if (pageTexts.length === 0) {
      console.error(`[asPDF] No text content could be extracted from ${filename}.`);
      trashFile(fullFilePath);
      return {
        success: false,
        reason: `No text content found in ${filename}.`,
        documents: [],
      };
    }

    // Join all page content with double newlines for better readability
    const content = pageTexts.join("\n\n");

    // Create document data with extracted metadata
    const data = {
      id: v4(),
      url: "file://" + fullFilePath,
      title: filename,
      docAuthor: pdfMeta.author,
      description: pdfMeta.title,
      docSource: "pdf file uploaded by the user.",
      chunkSource: "",
      published: createdDate(fullFilePath),
      wordCount: content.split(" ").length,
      pageContent: content,
      token_count_estimate: tokenizeString(content),
      // Store additional metadata for potential future use
      metadata: {
        totalPages: pdfMeta.totalPages,
        creator: pdfMeta.creator,
        producer: pdfMeta.producer,
        subject: pdfMeta.subject,
        keywords: pdfMeta.keywords,
        creationDate: pdfMeta.creationDate,
        modificationDate: pdfMeta.modificationDate
      }
    };

    const document = writeToServerDocuments(
      data,
      `${slugify(filename)}-${data.id}`
    );
    trashFile(fullFilePath);
    console.log(`[SUCCESS]: ${filename} converted & ready for embedding.\n`);
    return { success: true, reason: null, documents: [document] };

  } catch (error) {
    console.error(`[asPDF] Failed to process ${filename}:`, error.message);
    trashFile(fullFilePath);
    return {
      success: false,
      reason: `Failed to process PDF: ${error.message}`,
      documents: [],
    };
  }
}

module.exports = asPdf;