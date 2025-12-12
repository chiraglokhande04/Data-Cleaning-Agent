// scripts/runReport.js
require("dotenv").config();
const fs = require("fs-extra");
const path = require("path");
const { provenanceToDoc } = require("./provenanceToDocs.js");
const { upsertDocs } = require("./embeddingsUpsert.js");
const { retrieveDocs } = require("./retrieveDocs.js");
const { generateReport } = require("./generateReport.js");
const { markdownToPdf } = require("./exportPdf.js");
const mongoose = require("mongoose");
const DatasetMetadata = require("../../models/DatasetMetadata.js");

async function main(datasetId) {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(process.env.MONGO_URI);
  }

  const dataset = await DatasetMetadata.findById(datasetId);
  if (!dataset) throw new Error("Dataset not found");

  const provenanceList = Array.isArray(dataset.provenance)
    ? dataset.provenance.map(item => item.details).filter(Boolean)
    : [];

  console.log("povenanceList.length............:", provenanceList);

  // Convert provenance events to small docs
  const docs = provenanceList
    .map(evt => provenanceToDoc(evt, dataset))
    .filter(Boolean);

  console.log('docs............', docs)


  // Upsert docs to Pinecone
  console.log("Upserting docs...");
  await upsertDocs(docs);

  // Retrieve context (you can craft query from dataset summary)
  const queryText = `Dataset ${dataset.filename} cleaning provenance and techniques`;
  const retrieved = await retrieveDocs(queryText, 6, process.env.PINECONE_NAMESPACE);

  // For readability we attach some metadata text to retrieved results
  // If you stored doc text in metadata, it's present; otherwise just metadata
  const retrievedForPrompt = retrieved.map((r) => ({
    id: r.id,
    score: r.score,
    metadata: r.metadata,
  }));

  // Call LLM to generate Markdown
  const markdown = await generateReport(
    dataset,
    JSON.stringify(provenanceList, null, 2),
    retrievedForPrompt,
    dataset.row_count
  );

  const outDir = path.join(process.cwd(), "reports");
  await fs.ensureDir(outDir);
  const ts = Date.now();
  const mdPath = path.join(outDir, `report_${datasetId}_${ts}.md`);
  await fs.writeFile(mdPath, markdown, "utf8");

  const pdfPath = path.join(outDir, `report_${datasetId}_${ts}.pdf`);
  await markdownToPdf(markdown, pdfPath);

  console.log("Report generated:", { mdPath, pdfPath });

  // Disconnect ONLY if this script was run as stand-alone CLI
  if (process.env.IS_CLI === "true") {
    await mongoose.disconnect();
  }

  return { mdPath, pdfPath };

}

/* Run from CLI: node --experimental-modules scripts/runReport.js <datasetId> */
if (process.argv.length >= 3) {
  const id = process.argv[2];
  main(id).catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = main;
