// // require("dotenv").config();
// // const OpenAI = require("openai");
// // const { Pinecone } = require("@pinecone-database/pinecone");

// // const openai = new OpenAI({
// //   apiKey: process.env.OPENAI_API_KEY,
// // });

// // const pc = new Pinecone({
// //   apiKey: process.env.PINECONE_API_KEY,
// // });

// // const index = pc.index(process.env.PINECONE_INDEX_NAME);
// // const NAMESPACE = process.env.PINECONE_NAMESPACE || "default";


// // // ---- Embed text ----
// // async function embedText(text) {
// //   const model = "text-embedding-3-small";

// //   const resp = await openai.embeddings.create({
// //     model,
// //     input: text,
// //   });

// //   return resp.data[0].embedding;
// // }

// // exports.embedText = embedText;


// // // ---- Upsert multiple docs ----
// // // docs: [{ id, text, metadata }]
// // exports.upsertDocs = async (docs = []) => {
// //   const vectors = [];

// //   for (const doc of docs) {
// //     const vector = await embedText(doc.text);

// //     vectors.push({
// //       id: doc.id,
// //       values: vector,
// //       metadata: doc.metadata || {},
// //     });
// //   }

// //   // NEW SDK syntax
// //   await index.namespace(NAMESPACE).upsert(vectors);

// //   return { upserted: vectors.length };
// // };


// require("dotenv").config();
// const { Pinecone } = require("@pinecone-database/pinecone");
// const { pipeline } = require("@xenova/transformers");

// let embedder = null; // lazy load for performance

// // Load Pinecone
// const pc = new Pinecone({
//   apiKey: process.env.PINECONE_API_KEY,
// });

// const index = pc.index(process.env.PINECONE_INDEX_NAME);
// const NAMESPACE = process.env.PINECONE_NAMESPACE || "default";


// // ---- Initialize local embedding model ----
// async function getEmbedder() {
//   if (!embedder) {
//     // Recommended: Nomic Embed v1.5 (FREE, excellent quality)
//     embedder = await pipeline(
//       "feature-extraction",
//       "nomic-ai/nomic-embed-text-v1.5"
//     );
//   }
//   return embedder;
// }


// // ---- Embed text (local, GPU/CPU auto) ----
// async function embedText(text) {
//   const model = await getEmbedder();

//   const output = await model(text, {
//     pooling: "mean",
//     normalize: true,
//   });

//   // output.data is a Float32Array → convert to JS array
//   return Array.from(output.data);
// }

// exports.embedText = embedText;


// // ---- Upsert multiple docs ----
// // docs: [{ id, text, metadata }]
// exports.upsertDocs = async (docs = []) => {

//   console.log("Upserting docs to Pinecone...",docs);
//   const vectors = [];

//   for (const doc of docs) {
//     const vector = await embedText(doc.text);

//     vectors.push({
//       id: doc.id,
//       values: vector,
//       metadata: doc.metadata || {},
//     });
//   }

//   // Pinecone upsert
//   await index.namespace(NAMESPACE).upsert(vectors);

//   return { upserted: vectors.length };
// };


require("dotenv").config();
const { Pinecone } = require("@pinecone-database/pinecone");
const { pipeline } = require("@xenova/transformers");

let embedder = null;

// ---- Load Pinecone ----
const pc = new Pinecone({
  apiKey: process.env.PINECONE_API_KEY,
});

const index = pc.index(process.env.PINECONE_INDEX_NAME);
const NAMESPACE = process.env.PINECONE_NAMESPACE || "default";

// ----------------------------------------
// INIT LOCAL EMBEDDING MODEL (lazy)
// ----------------------------------------
async function getEmbedder() {
  if (!embedder) {
    embedder = await pipeline(
      "feature-extraction",
      "nomic-ai/nomic-embed-text-v1.5"
    );
  }
  return embedder;
}

// ----------------------------------------
// EMBED TEXT (uses Xenova offline)
// ----------------------------------------
async function embedText(text) {
  if (!text || typeof text !== "string") return null;

  const model = await getEmbedder();
  const output = await model(text, {
    pooling: "mean",
    normalize: true,
  });

  return Array.from(output.data);
}

exports.embedText = embedText;

// ----------------------------------------
// UPSERT MULTIPLE DOCS SAFELY
// docs: [{ id, text, metadata }]
// ----------------------------------------
exports.upsertDocs = async (docs = []) => {
  if (!Array.isArray(docs) || docs.length === 0) {
    console.log("[upsertDocs] No docs received. Skipping Pinecone.");
    return { upserted: 0 };
  }

  const vectors = [];

  for (const doc of docs) {
    if (!doc || !doc.text) {
      console.log("[upsertDocs] Skipping invalid doc:", doc);
      continue;
    }

    const vector = await embedText(doc.text);
    if (!vector) {
      console.log("[upsertDocs] Embedding failed, skipping doc:", doc.id);
      continue;
    }

    // Pinecone metadata must contain ONLY: string, number, bool, or array of strings
    const safeMetadata = {};
    if (doc.metadata && typeof doc.metadata === "object") {
      for (const [key, value] of Object.entries(doc.metadata)) {
        if (
          typeof value === "string" ||
          typeof value === "number" ||
          typeof value === "boolean" ||
          (Array.isArray(value) && value.every((v) => typeof v === "string"))
        ) {
          safeMetadata[key] = value;
        }
      }
    }

    vectors.push({
      id: doc.id,
      values: vector,
      metadata: safeMetadata,
    });
  }

  if (vectors.length === 0) {
    console.log("[upsertDocs] No valid vectors to upsert. Skipping.");
    return { upserted: 0 };
  }

  console.log(`[upsertDocs] Upserting ${vectors.length} vectors...`);
  await index.namespace(NAMESPACE).upsert(vectors);

  return { upserted: vectors.length };
};
