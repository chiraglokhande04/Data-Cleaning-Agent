// require('dotenv').config();

// const OpenAI = require('openai');
// const { Pinecone } = require("@pinecone-database/pinecone");
// const fs = require('fs');
// const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// const pc = new Pinecone({
//   apiKey: process.env.PINECONE_API_KEY,
// });
// const index = pc.Index(process.env.PINECONE_INDEX_NAME || 'data-cleaning-agent');
// const NAMESPACE = process.env.PINECONE_NAMESPACE || 'default';


// module.exports.retrieveDocs = async (queryText, topK = 5, namespace = NAMESPACE) => {

//     const emb = (await openai.embeddings.create({
//         model: 'text-embedding-3-small',
//         input: queryText,
//     })).data[0].embedding;

//     const queryResp = await index.query({
//         queryRequest: {
//             vector: emb,
//             topK,
//             includeMetadata: true,
//             namespace,
//         },
//     });

//     const matches =  queryResp.matches || [];

//     return matches.map(match => ({
//         id: match.id,
//         score: match.score,
//         metadata: match.metadata,
//     }));    

// }


require("dotenv").config();

const { Pinecone } = require("@pinecone-database/pinecone");
const { pipeline } = require("@xenova/transformers");

let embedder = null;

// Lazy-load local embedding model
async function getEmbedder() {
  if (!embedder) {
    embedder = await pipeline(
      "feature-extraction",
      "nomic-ai/nomic-embed-text-v1.5"
    );
  }
  return embedder;
}

// Generate embedding (local, free)
async function embed(text) {
  const model = await getEmbedder();

  const output = await model(text, {
    pooling: "mean",
    normalize: true,
  });

  return Array.from(output.data);
}

// Pinecone client
const pc = new Pinecone({
  apiKey: process.env.PINECONE_API_KEY,
});

const index = pc.index(process.env.PINECONE_INDEX_NAME);
const NAMESPACE = process.env.PINECONE_NAMESPACE || "default";


// ---- Retrieve Top-K Docs ----
module.exports.retrieveDocs = async (queryText, topK = 5, namespace = NAMESPACE) => {
  const emb = await embed(queryText);

  // New SDK syntax
  const queryResp = await index.namespace(namespace).query({
    topK,
    vector: emb,
    includeMetadata: true,
  });

  const matches = queryResp.matches || [];

  return matches.map(match => {
  let parsedParams = null;

  if (match.metadata?.params) {
    try {
      parsedParams = JSON.parse(match.metadata.params);
    } catch {
      parsedParams = match.metadata.params; // fallback
    }
  }

  return {
    id: match.id,
    score: match.score,
    metadata: {
      ...match.metadata,
      parsedParams
    }
  };
});

};
