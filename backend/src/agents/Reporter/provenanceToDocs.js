const crypto = require("crypto");

// ------------------------------------------------------------
// Redaction helper
// ------------------------------------------------------------
function sanitizeProvenance(prov) {
    const p = JSON.parse(JSON.stringify(prov));

    const redact = (v) =>
        typeof v === "string" &&
            (
                /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i.test(v) ||
                /\b\d{10,}\b/.test(v)
            )
            ? "[REDACTED]"
            : v;

    function walk(obj) {
        if (!obj || typeof obj !== "object") return obj;

        for (const k of Object.keys(obj)) {
            const val = obj[k];
            if (typeof val === "object") obj[k] = walk(val);
            else obj[k] = redact(val);
        }
        return obj;
    }

    return walk(p);
}

module.exports.sanitizeProvenance = sanitizeProvenance;


// ------------------------------------------------------------
// Pretty row formatter for before/after samples
// ------------------------------------------------------------
function formatRow(row) {
    try {
        if (row && typeof row === "object" && !Array.isArray(row)) {
            const keys = Object.keys(row).slice(0, 6);
            return keys.map(k => `${k}: ${JSON.stringify(row[k])}`).join(", ");
        }
        return JSON.stringify(row);
    } catch {
        return String(row);
    }
}


// ------------------------------------------------------------
// Main converter
// ------------------------------------------------------------
module.exports.provenanceToDoc = function provenanceToDoc(provEvent, datasetMeta = {}) {

    const p = sanitizeProvenance(provEvent);

    const transform = p.transform || {};
    const tname = transform.name || "Unknown Transformation";
    const tparams = transform.params || {};

    // -------------------------
    // SAMPLE extraction
    //--------------------------
    const beforeSample = (
        p.before_sample ||
        p.evidence?.before_records ||
        p.evidence?.before_sample ||
        []
    ).slice(0, 3);

    const afterSample = (
        p.after_sample ||
        p.evidence?.after_records ||
        p.evidence?.after_sample ||
        []
    ).slice(0, 3);

    const beforeText = beforeSample.map(formatRow).join("\n");
    const afterText = afterSample.map(formatRow).join("\n");

    // -------------------------
    // CODE SNIPPET generation
    // -------------------------
    let code_snippet = "";
    switch (tname) {
        case "fill_missing":
        case "FillMissing": {
            const col = transform.params?.column;
            const strategy = transform.params?.strategy;
            const value = transform.params?.value;

            if (strategy === "mean") {
                code_snippet = `df['${col}'] = df['${col}'].fillna(df['${col}'].mean())`;
            } else if (strategy === "median") {
                code_snippet = `df['${col}'] = df['${col}'].fillna(df['${col}'].median())`;
            } else if (strategy === "mode") {
                code_snippet = `df['${col}'] = df['${col}'].fillna(df['${col}'].mode()[0])`;
            } else if (strategy === "constant") {
                code_snippet = `df['${col}'] = df['${col}'].fillna(${JSON.stringify(value)})`;
            } else {
                code_snippet = `# FillMissing (unknown strategy)\ndf['${col}'] = df['${col}'].fillna(...)`;
            }
            break;
        }

        case "coerce_numeric":
        case "CoerceNumeric":
            code_snippet =
                `df['${transform.params?.column}'] = pd.to_numeric(df['${transform.params?.column}'], errors='coerce')`;
            break;

        case "coerce_datetime":
        case "CoerceDatetime":
            code_snippet =
                `df['${transform.params?.column}'] = pd.to_datetime(df['${transform.params?.column}'], errors='coerce')`;
            break;

        case "clip_outliers_iqr":
        case "ClipOutliersIQR": {
            const col = transform.params?.column;
            const k = transform.params?.k ?? 1.5;
            const method = transform.params?.method || "clip";

            code_snippet = `
# IQR Outlier Handling
Q1 = df['${col}'].quantile(0.25)
Q3 = df['${col}'].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - (${k} * IQR)
upper = Q3 + (${k} * IQR)

${method === "clip"
        ? `df['${col}'] = df['${col}'].clip(lower, upper)`
        : method === "remove"
            ? `df = df[(df['${col}'] >= lower) & (df['${col}'] <= upper)]`
            : `df['${transform.params?.flag_column_name}'] = (df['${col}'] < lower) | (df['${col}'] > upper)`
    }
`;
            break;
        }

        case "map_categorical":
        case "MapCategorical": {
            const col = transform.params?.column;
            const mapping = transform.params?.mapping;

            if (mapping) {
                code_snippet = `# Explicit mapping\nmapping = ${JSON.stringify(mapping, null, 2)}\ndf['${col}'] = df['${col}'].replace(mapping)`;
            } else {
                code_snippet =
`# Fuzzy categorical cleaning
from rapidfuzz import fuzz, process`;
            }
            break;
        }

        case "DeriveColumn": {
            const newCol = transform.params?.new_column;
            code_snippet =
`# Derive new column
# Custom logic required
df['${newCol}'] = df.apply(lambda row: ..., axis=1)`;
            break;
        }

        default:
            code_snippet = `# transform: ${tname} params: ${JSON.stringify(tparams)}`;
    }

    // ---------------------------------------
    // MAPPING SUMMARY (for map_categorical)
    // ---------------------------------------
    let mappingSummary = "";
    if (tname.toLowerCase().includes("map_categorical")) {
        const canonical = p.evidence?.canonical || {};

        if (canonical && typeof canonical === "object" && Object.keys(canonical).length > 0) {
            const inv = {};
            for (const [variant, canon] of Object.entries(canonical)) {
                inv[canon] = inv[canon] || [];
                inv[canon].push(variant);
            }

            const groups = Object.entries(inv)
                .map(([canon, variants]) => ({
                    canon,
                    count: variants.length,
                    variants,
                }))
                .sort((a, b) => b.count - a.count);

            const top = groups
                .slice(0, 5)
                .map(g => `- ${g.canon}: ${g.count} variants (examples: ${g.variants.slice(0, 3).join(", ")})`)
                .join("\n");

            mappingSummary =
`Mapping summary:
Total canonical categories: ${groups.length}
Top groups:
${top}`;
        }
    }

    // ---------------------------------------
    // TEXT BLOCK for LLM
    // ---------------------------------------
    const textParts = [
        `Action: ${tname}`,
        `Why: ${p.reason || "heuristic / configured threshold"}`,
        `Params: ${JSON.stringify(tparams)}`,
        mappingSummary ? `\n${mappingSummary}` : "",
        `Evidence sample (before):\n${beforeText || "n/a"}`,
        `Evidence sample (after):\n${afterText || "n/a"}`,
        `Changed count: ${p.evidence?.changed_count ?? p.evidence?.dropped_count ?? "n/a"}`,
    ];

    const text = textParts.filter(Boolean).join("\n\n");

    // ---------------------------------------
    // ID generation
    // ---------------------------------------
    const id = p.id || crypto.createHash("sha256").update(JSON.stringify(p)).digest("hex");

    // ---------------------------------------
    // RETURN OBJECT (used for RAG)
    // ---------------------------------------
    return {
        id,
        title: `${tname} on ${transform.params?.column ?? "dataset"}`,
        text: `${text}\n\nCode:\n${code_snippet}`,
        code_snippet,
        metadata: {
            dataset_id: datasetMeta._id ?? datasetMeta.id ?? "unknown",
            timestamp: p.timestamp || new Date().toISOString(),
            transform: tname,
            params: Buffer.from(JSON.stringify(tparams)).toString("base64"),
            mapping_summary: mappingSummary
                ? Buffer.from(mappingSummary).toString("base64")
                : "",
            before_sample: beforeSample.map(formatRow),
            after_sample: afterSample.map(formatRow),
            confidence: p.evidence?.confidence ?? 0.75,
        },
    };
};
