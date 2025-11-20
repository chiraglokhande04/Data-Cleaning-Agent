const dfd = require("danfojs-node");
const Fuse = require("fuse.js");
const { v4: uuidv4 } = require("uuid");

const CONFIG = {
    col_missing_threshold: 0.2,
    row_missing_threshold: 0.5,
    pk_uniqueness_threshold: 0.98,
    numeric_parse_fraction: 0.9,
    outlier_iqr_k: 1.5,
    categorical_fuzzy_ratio: 0.85, // 85%
}

// ---------- Helper: issue object ----------
function issue(scope, column, rows, issue_type, severity, score, evidence, suggestedFix = null) {
    return {
        id: uuidv4(),
        scope,
        column,
        rows,
        issue_type,
        severity,
        score,
        evidence,
        suggestedFix,
        timestamp: new Date().toISOString(),
    }
}


class Analyzer {
    constructor(df) {
        this.df = df;
        this.issues = [];
    }


    // ----------------------------------
    // 1) Schema Inference
    // ----------------------------------
    inferColumnType(series) {
        const nonNull = series.filter((v) => v !== null && v != undefined && v !== "")

        if (nonNull.length == 0) return "empty";
        const values = nonNull.map(String);

        // Helper functions
        const isStrictNumber = (v) => {
            if (!v || !v.trim) return false
            return /^[-+]?\d+(\.\d+)?$/.test(v.trim());
        }

        const isValidDate = (v) => {
            if (/^\d{4,}$/.test(v)) return false;

            const timestamp = Date.parse(v)
            return !NaN(timestamp)
        }

        // Date Detection
        let dateCount = 0;
        const sample = values.slice(0, 300)

        for (const v of sample) {
            if (isValidDate(v)) dateCount++;
        }

        const dateRatio = dateCount / sample.length;
        if (dateRatio >= 0.9) return "datetime";

        // Numeric Detection
        let numCount = 0;
        for (const v of sample) {
            if (isStrictNumber(v)) numCount++;
        }

        const numRatio = numCount / sample.length;
        if (numRatio >= 0.9) return "numeric";

        //default
        return "string"
    }

    inferSchema() {
        const schema = {}

        this.df.columns.forEach((col) => {
            const s = this.df[col]
            const nonNull = s.dropNa()

            schema[col] = {
                inferred_type: this.inferColumnType(s),
                pandas_dtypes: s.dtypes,
                missing_count: s.isNa().sum(),
                missing_pct: s.isNa().sum() / s.size(),
                nunique: nonNull.unique().size(),
                example: nonNull.head(5).values,
            }
        })
        return schema
    }


    // ----------------------------------
    // 2) Missing Values
    // ----------------------------------
    detectMissing() {
        const colThresh = CONFIG.col_missing_threshold
        const rowThresh = CONFIG.row_missing_threshold

        //Helper
        const isMissing = (v) =>
            v === null ||
            v === undefined ||
            v === "" ||
            (typeof v == "number" && Number.isNaN(v)) ||
            (typeof v === "string" && ["na", "n/a", "-", "null"].includes(v.trim().toLowerCase()));


        // Column - level Missingness
        this.df.columns.forEach((col) => {
            s = this.df[col]

            let missingCount = 0
            let missingRowIdx = []

            for (i = 0; i < s.length; i++) {
                const val = s.iloc(i)
                if (isMissing(val)) {
                    missingCount++;
                    if (missingRowIdx.length < 5) {
                        missingRowIdx.push(i)
                    }
                }
            }

            const missingPct = missingCount / s.size
            if (missingPct >= colThresh) {
                this.issues.push(
                    issue(
                        "column",
                        col,
                        missingRowIdx,
                        "missing_values",
                        missingPct > 0.5 ? "high" : "medium",
                        missingPct,
                        { missingPct },
                        "impute_or_drop"
                    )
                )
            }

        })

        //Row - level Missingness
        for (let i = 0; i < this.df.shape[0]; i++) {
            const row = this.df.iloc({ rows: [i] });
            const values = Object.values(row.data[0]);

            const missingCells = values.filter(isMissing).length;
            const missingFraction = missingCells / values.length;

            if (missingFraction >= rowThresh) {
                this.issues.push(
                    issue(
                        "row",
                        null,
                        [i],
                        "row_sparsity",
                        "high",
                        missingFraction,
                        { missingFraction },
                        "drop_or_review"
                    )
                );
            }
        }
    }


    // ----------------------------------
    // 3) Duplicate rows + PK detection
    // ----------------------------------
    detectDuplicatesAndPK() {
        const jsonRows = this.df.values.map((v) => JSON.stringify(v));
        // crypto.createHash("md5").update(JSON.stringify(row)).digest("hex");

        const seen = new Set();
        let duplicates = [];

        jsonRows.forEach((r, i) => {
            if (seen.has(r)) duplicates.push(i);
            else seen.add(r);
        });

        if (duplicates.length) {
            this.issues.push(
                issue(
                    "dataset",
                    null,
                    duplicates.slice(0, 10),
                    "exact_duplicates",
                    "high",
                    0.9,
                    { count: duplicates.length },
                    "drop_or_merge"
                )
            );
        }

        // Primary key candidates
        this.df.columns.forEach((col) => {
            const uniqueCount = new Set(this.df[col].dropNa().values).size;
            const frac = uniqueCount / this.df.shape[0];
            if (frac >= CONFIG.pk_uniqueness_threshold) {
                this.issues.push(
                    issue("column", col, [], "pk_candidate", "low", frac, {
                        unique_fraction: frac,
                    })
                );
            }
        });
    }




}

