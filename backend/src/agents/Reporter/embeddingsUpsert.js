require("dotenv").config();
const OpenAI = require("openai");
const {PineconeClient} = require("@pinecone-database/pinecone");

const openai = new OpenAI({apiKey: process.env.OPENAI_API_KEY});

const pinecone = new PineconeClient();

pinecone.init({
    apiKey: process.env.PINECONE_API_KEY,
    environment: process.env.PINECONE_ENVIRONMENT,
});

const index = pinecone.Index(process.env.PINECONE_INDEX_NAME || "data-cleaning-agent");
const NAMESPACE = process.env.PINECONE_NAMESPACE || "default";


exports.embeddingsUpsert = async (text) => {
    const model = "text-embedding-3-small";
    const resp = await openai.embeddings.create({
        model: model,
        input: text,
    });
    return resp.data[0].embedding;
}

/**
 * Upsert an array of docs: [{ id, text, metadata }]
 */
exports.upsertDocs = async(docs = []) =>{
  // create vectors with embedding
  const vectors = [];
  for (const doc of docs) {
    const vector = await embedText(doc.text);
    vectors.push({
      id: doc.id,
      metadata: doc.metadata,
      values: vector,
    });
  }
  // Pinecone upsert in batch
  await index.upsert({
    upsertRequest: {
      vectors,
      namespace: NAMESPACE,
    },
  });
  return { upserted: vectors.length };
}