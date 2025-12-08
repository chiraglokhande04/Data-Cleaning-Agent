const crypto = require("crypto");

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



module.exports.provenanceToDoc = function provenanceToDoc(provEvent, datasetMeta = {}) {

    const p = sanitizeProvenance(provEvent);

    const transform = p.transform || {};
    const tname = transform.name || "Unknown Transformation";
    const tparams = transform.params || {};

    const evidenceSample =
        (p.evidence && (p.evidence.before_records || p.evidence.sample_before)) ||
        p.before_sample ||
        [];

    const sampleText = JSON.stringify(evidenceSample).slice(0, 800);

  // code snippet suggestions 
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
                code_snippet = `df['${col}'] = df['${col}'].fillna(${JSON.stringify(
                    value
                )})`;
            } else {
                code_snippet = `# FillMissing (unknown strategy)\ndf['${col}'] = df['${col}'].fillna(...)`;
            }
            break;
        }

        case "coerce_numeric":
        case "CoerceNumeric":
            code_snippet = `df['${transform.params?.column}'] = pd.to_numeric(df['${transform.params?.column}'], errors='coerce')`;
            break;

        case "coerce_datetime":
        case "CoerceDatetime":
            code_snippet = `df['${transform.params?.column}'] = pd.to_datetime(df['${transform.params?.column}'], errors='coerce')`;
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
                code_snippet = `# Explicit mapping\nmapping = ${JSON.stringify(
                    mapping,
                    null,
                    2
                )}\ndf['${col}'] = df['${col}'].replace(mapping)`;
            } else {
                code_snippet = `# Fuzzy categorical cleaning not native in pandas\n# Use rapidfuzz or similar library\nfrom rapidfuzz import fuzz, process`;
            }
            break;
        }

        case "DeriveColumn": {
            const newCol = transform.params?.new_column;
            code_snippet = `# Derive new column\n# Custom logic must be implemented manually\n# Example:\ndf['${newCol}'] = df.apply(lambda row: ..., axis=1)`;
            break;
        }

        default:
            code_snippet = `# transform: ${tname} params: ${tparams}`;
    }


    const text = [
        `Action: ${tname}`,
        `Why: ${p.reason || "heuristic / configured threshold"}`,
        `Params: ${JSON.stringify(tparams)}`,
        `Evidence sample: ${sampleText}`,
        `Changed count: ${p.evidence?.changed_count ?? p.evidence?.dropped_count ?? "n/a"}`,
    ].join("\n\n");

    const id = p.id || crypto.createHash("sha256").update(JSON.stringify(p)).digest("hex");

    return {
        id,
        title: `${tname} on ${transform.params?.column ?? "dataset"}`,
        text: `${text}\n\nCode:\n${code_snippet}`,
        code_snippet,
        metadata: {
            dataset_id: datasetMeta._id ?? datasetMeta.id ?? "unknown",
            timestamp: p.timestamp || new Date().toISOString(),
            transform: tname,
            params: JSON.stringify(tparams), 
            confidence: p.evidence?.confidence ?? 0.75,
        },
    };
};
