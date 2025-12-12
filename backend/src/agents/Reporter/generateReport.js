require("dotenv").config();

const Groq = require("groq-sdk");
const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

exports.generateReport = async (
    datasetMeta,
    provenanceJson,
    retrievedDocs = [],
    rowCount = null
) => {

    const snippets = (retrievedDocs || [])
        .slice(0, 8)
        .map(d => {
            const params = d.metadata?.parsedParams || d.metadata?.params || {};
            return `Snippet: ${d.metadata?.transform || "unknown"} with params ${JSON.stringify(params)}`
        })
        .join("\n\n");

    const systemPrompt = `
You are a technical data-cleaning assistant. Produce a strict, deterministic Markdown report.
OUTPUT REQUIREMENTS: Respond ONLY with Markdown, and include exactly these sections in this order. Do not add extra commentary.

# Executive Summary
- 2-3 line concise summary.

# Transformations Applied
- A bullet list, each item: {transform name} on {column} — short one-line description and changed count.

# For each transformation (separate sub-section)
## {transform name} on {column}
- Why: (1 sentence)
- Before sample: (show BEFORE rows, up to 3 lines)
- After sample: (show AFTER rows, up to 3 lines, or "n/a")
- Code executed: fenced python code block
- Confidence: numeric (0–1)
- Mapping summary: include ONLY if available.

# Next recommended actions
- 3 concise, prioritized actions.

# Deterministic Audit JSON
- At the end, output a JSON array (only JSON code block) containing the original provenance array EXACTLY as given.

Do not hallucinate. Missing fields must be "unknown" or "n/a".
`;

    const docsForPrompt = (retrievedDocs || []).slice(0, 8).map((d, i) => {
        let paramsObj = {};
        try {
            if (d.metadata?.params) {
                paramsObj = JSON.parse(Buffer.from(d.metadata.params, "base64").toString("utf8"));
            }
        } catch { paramsObj = {}; }

        let mappingSummary = "";
        try {
            if (d.metadata?.mapping_summary) {
                mappingSummary = Buffer.from(d.metadata.mapping_summary, "base64").toString("utf8");
            }
        } catch { }

        return {
            title: d.metadata?.transform || `Snippet ${i + 1}`,
            params: paramsObj,
            before_sample: d.metadata?.before_sample || [],
            after_sample: d.metadata?.after_sample || [],
            mapping_summary: mappingSummary,
            code_snippet: d.code_snippet || ""
        };
    });

    const userPrompt = `
Dataset: ${datasetMeta.filename ?? datasetMeta._id ?? "unknown"}
Row count: ${rowCount ?? "unknown"}

Retrieved docs:
${JSON.stringify(docsForPrompt, null, 2)}

Provenance JSON:
${provenanceJson}

Task: Produce the Markdown report following system instructions exactly.
`;

    const resp = await groq.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt }
        ],
        temperature: 0.0,
        max_tokens: 2500,
    });

    return resp.choices?.[0]?.message?.content ?? "";
};
