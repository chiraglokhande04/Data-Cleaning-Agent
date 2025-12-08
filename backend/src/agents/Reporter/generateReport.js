require("dotenv").config();

const Groq = require("groq-sdk");
const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

exports.generateReport = async (
    datasetMeta,
    provenanceJson,
    retrievedDocs = [],
    rowCount = null
) => {

    snippets = retrievedDocs.map(d => {
        const params = d.metadata.parsedParams || d.metadata.params;
        return `Snippet: ${d.metadata.transform} with params ${JSON.stringify(params)}`
    })
    .slice(0, 8)
    .join("\n\n");

    const systemPrompt = `
You are a technical data-cleaning assistant. Produce a clear, trustworthy Markdown report describing and justifying the cleaning operations performed. 
Use deterministic, non-hallucinated, audit-safe language.
  `;

    const userPrompt = `
Dataset: ${datasetMeta.filename ?? datasetMeta._id ?? "unknown"}
Row count: ${rowCount ?? "unknown"}

Retrieved knowledge snippets:
${snippets}

Provenance JSON:
${provenanceJson}

Task: Produce a Markdown report with sections:
1) Executive summary
2) What changed (bulleted per transform)
3) Why each change was done
4) Code snippets executed
5) Confidence levels
6) Recommended next actions
7) Deterministic audit JSON

Output: Markdown only.
  `;

    const resp = await groq.chat.completions.create({
        model: "llama-3.3-70b-versatile",     // ✔ your strongest supported model
        messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt }
        ],
        temperature: 0.0,
        max_tokens: 2000,
    });

    return resp.choices?.[0]?.message?.content ?? "";
};
