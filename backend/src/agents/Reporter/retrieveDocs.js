require("dotenv").config();

const { Pinecone } = require("@pinecone-database/pinecone");
const { pipeline } = require("@xenova/transformers");

let embedder = null;

// Load local embedding model
async function getEmbedder() {
  if (!embedder) {
    embedder = await pipeline(
      "feature-extraction",
      "nomic-ai/nomic-embed-text-v1.5"
    );
  }
  return embedder;
}

async function embed(text) {
  const model = await getEmbedder();
  const output = await model(text, { pooling: "mean", normalize: true });
  return Array.from(output.data);
}

// Pinecone client
const pc = new Pinecone({
  apiKey: process.env.PINECONE_API_KEY,
});

const index = pc.index(process.env.PINECONE_INDEX_NAME);
const NAMESPACE = process.env.PINECONE_NAMESPACE || "default";


// -------------------------------------------------------------
// Retrieve Docs
// -------------------------------------------------------------
module.exports.retrieveDocs = async (queryText, topK = 5, namespace = NAMESPACE) => {
  const emb = await embed(queryText);

  // CORRECT Pinecone v3 query syntax
  const queryResp = await index.namespace(namespace).query({
    topK,
    vector: emb,
    includeMetadata: true,
  });

  const matches = queryResp.matches || [];

  return matches.map(match => {
    const meta = match.metadata || {};

    // decode params
    let parsedParams = {};
    try {
      if (meta.params) {
        parsedParams = JSON.parse(
          Buffer.from(meta.params, "base64").toString("utf8")
        );
      }
    } catch {
      parsedParams = {};
    }

    // decode mapping summary
    let mappingSummary = "";
    try {
      if (meta.mapping_summary) {
        mappingSummary = Buffer.from(meta.mapping_summary, "base64").toString("utf8");
      }
    } catch {}

    return {
      id: match.id,
      score: match.score,
      metadata: {
        ...meta,
        parsedParams,
        before_sample: meta.before_sample || [],
        after_sample: meta.after_sample || [],
        mapping_summary_text: mappingSummary,
      },
      code_snippet: meta.code_snippet || ""
    };
  });
};
