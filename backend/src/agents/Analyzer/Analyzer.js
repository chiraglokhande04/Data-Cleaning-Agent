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
};

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
  };
}

class Analyzer {
  constructor(df) {
    this.df = df; // expecting a danfo DataFrame
    this.issues = [];
  }

  // Helper: safe series -> array
  _seriesValues(series) {
    if (!series) return [];
    // If passed a plain array already, return it
    if (Array.isArray(series)) return series;
    // If series has .values use it
    if (series.values) return Array.isArray(series.values) ? series.values : [];
    return [];
  }

  // 1) Infer type from a danfo Series
  inferColumnType(series) {
    const vals = this._seriesValues(series).filter((v) => v !== null && v !== undefined && String(v).trim() !== "");
    if (vals.length === 0) return "empty";

    const sample = vals.slice(0, 300).map((v) => String(v).trim());

    const isStrictNumber = (v) => /^[-+]?\d+(?:\.\d+)?$/.test(v);
    const isValidDate = (v) => {
      // avoid plain years like "2024"
      if (/^\d{4,}$/.test(v)) return false;
      const t = Date.parse(v);
      return !Number.isNaN(t);
    };

    // Date detection
    let dateCount = 0;
    for (const v of sample) if (isValidDate(v)) dateCount++;
    if (sample.length > 0 && dateCount / sample.length >= 0.9) return "datetime";

    // Numeric detection
    let numCount = 0;
    for (const v of sample) if (isStrictNumber(v)) numCount++;
    if (sample.length > 0 && numCount / sample.length >= 0.9) return "numeric";

    return "string";
  }

  inferSchema() {
    const schema = {};
    const cols = this.df.columns;

    for (const col of cols) {
      const s = this.df[col];
      const arr = this._seriesValues(s);
      const nonNull = arr.filter((v) => v !== null && v !== undefined && String(v).trim() !== "");

      const inferred_type = this.inferColumnType(s);
      const missing_count = arr.filter((v) => v === null || v === undefined || String(v).trim() === "").length;
      const missing_pct = arr.length === 0 ? 0 : missing_count / arr.length;
      const uniqueCount = new Set(nonNull.map((v) => String(v))).size;

      schema[col] = {
        inferred_type,
        // danfo provides dtypes property on Series but keep a safe fallback
        pandas_dtypes: s && s.dtypes ? s.dtypes : typeof nonNull[0] || "unknown",
        missing_count,
        missing_pct,
        nunique: uniqueCount,
        example: nonNull.slice(0, 5),
      };
    }

    return schema;
  }

  // 2) Missing values (column + row)
  detectMissing() {
    const colThresh = CONFIG.col_missing_threshold;
    const rowThresh = CONFIG.row_missing_threshold;

    const isMissing = (v) =>
      v === null || v === undefined || String(v).trim() === "" || (typeof v === "number" && Number.isNaN(v)) || (typeof v === "string" && ["na", "n/a", "-", "null"].includes(String(v).trim().toLowerCase()));

    // column level
    for (const col of this.df.columns) {
      const s = this._seriesValues(this.df[col]);
      let missingCount = 0;
      const missingRowIdx = [];

      for (let i = 0; i < s.length; i++) {
        if (isMissing(s[i])) {
          missingCount++;
          if (missingRowIdx.length < 5) missingRowIdx.push(i);
        }
      }

      const missingPct = s.length === 0 ? 0 : missingCount / s.length;
      if (missingPct >= colThresh) {
        this.issues.push(issue("column", col, missingRowIdx, "missing_values", missingPct > 0.5 ? "high" : "medium", missingPct, { missingPct }, "impute_or_drop"));
      }
    }

    // row level
    const nRows = this.df.shape[0];
    for (let i = 0; i < nRows; i++) {
      const rowValues = this.df.values && Array.isArray(this.df.values) ? this.df.values[i] : null;
      if (!rowValues) continue;
      const missingCells = rowValues.filter(isMissing).length;
      const missingFraction = missingCells / rowValues.length;
      if (missingFraction >= rowThresh) {
        this.issues.push(issue("row", null, [i], "row_sparsity", "high", missingFraction, { missingFraction }, "drop_or_review"));
      }
    }
  }

  // 3) duplicates & PK candidates
  detectDuplicatesAndPK() {
    const rows = this.df.values || [];
    const seen = new Set();
    const duplicates = [];

    for (let i = 0; i < rows.length; i++) {
      const key = JSON.stringify(rows[i]);
      if (seen.has(key)) duplicates.push(i);
      else seen.add(key);
    }

    if (duplicates.length) {
      this.issues.push(issue("dataset", null, duplicates.slice(0, 10), "exact_duplicates", "high", 0.9, { count: duplicates.length }, "drop_or_merge"));
    }

    // PK candidates
    const nRows = this.df.shape[0];
    for (const col of this.df.columns) {
      const vals = this._seriesValues(this.df[col]).filter((v) => v !== null && v !== undefined && String(v).trim() !== "");
      const uniqueCount = new Set(vals.map((v) => String(v))).size;
      const frac = nRows === 0 ? 0 : uniqueCount / nRows;
      if (frac >= CONFIG.pk_uniqueness_threshold) {
        this.issues.push(issue("column", col, [], "pk_candidate", "low", frac, { unique_fraction: frac }));
      }
    }
  }

  // 4) Outliers using IQR
  detectOutliersIQR(col) {
    const arr = this._seriesValues(this.df[col]).map((v) => Number(v)).filter((x) => !Number.isNaN(x));
    if (arr.length < 5) return;
    arr.sort((a, b) => a - b);
    const Q1 = arr[Math.floor(arr.length * 0.25)];
    const Q3 = arr[Math.floor(arr.length * 0.75)];
    const iqr = Q3 - Q1;
    const lower = Q1 - CONFIG.outlier_iqr_k * iqr;
    const upper = Q3 + CONFIG.outlier_iqr_k * iqr;

    const outliers = [];
    const original = this._seriesValues(this.df[col]);
    for (let i = 0; i < original.length; i++) {
      const num = Number(original[i]);
      if (!Number.isNaN(num) && (num < lower || num > upper)) outliers.push({ index: i, value: original[i] });
    }

    if (outliers.length) {
      this.issues.push(issue("column", col, outliers.slice(0, 10).map((o) => o.index), "outlier_iqr", "medium", outliers.length / this.df.shape[0], { lower, upper, count: outliers.length }, "review_or_cap"));
    }
  }

  runOutliers() {
    for (const col of this.df.columns) {
      const type = this.inferColumnType(this.df[col]);
      if (type === "numeric") this.detectOutliersIQR(col);
    }
  }

  // 5) Date parsing issues
  detectDateParsing() {
    for (const col of this.df.columns) {
      const type = this.inferColumnType(this.df[col]);
      if (type !== "datetime") continue;

      const vals = this._seriesValues(this.df[col]).slice(0, 200);
      let failures = 0;
      let total = 0;
      for (const v of vals) {
        total++;
        if (Number.isNaN(Date.parse(String(v)))) failures++;
      }

      const frac = total === 0 ? 0 : failures / total;
      if (frac > 0.1) {
        this.issues.push(issue("column", col, [], "date_parse_failures", frac > 0.5 ? "high" : "medium", frac, { failed_fraction: frac }, "parse_with_formats"));
      }
    }
  }

  // 6) Fuzzy categorical inconsistency
  detectCategoricalInconsistency() {
    for (const col of this.df.columns) {
      const type = this.inferColumnType(this.df[col]);
      if (type !== "string") continue;

      const vals = this._seriesValues(this.df[col]);
      const uniques = [...new Set(vals.map((v) => String(v)))];
      if (uniques.length > 500) continue;

      const fuse = new Fuse(uniques, { includeScore: true, threshold: 0.4 });
      const clusters = [];
      const used = new Set();

      for (const u of uniques) {
        if (used.has(u)) continue;
        const matches = fuse.search(u).filter((m) => m.score < (1 - CONFIG.categorical_fuzzy_ratio));
        const group = matches.map((m) => m.item);
        group.forEach((g) => used.add(g));
        if (group.length > 1) clusters.push(group);
      }

      if (clusters.length) {
        this.issues.push(issue("column", col, [], "categorical_inconsistency", "medium", 0.6, { clusters: clusters.slice(0, 10) }, "map_or_standardize"));
      }
    }
  }

  // 7) Suspicious text
  detectSuspiciousText() {
    for (const col of this.df.columns) {
      const type = this.inferColumnType(this.df[col]);
      if (type !== "string") continue;

      const vals = this._seriesValues(this.df[col]);
      const longRows = [];
      for (let i = 0; i < vals.length; i++) {
        if (String(vals[i]).length > 1000) longRows.push(i);
      }

      if (longRows.length) {
        this.issues.push(issue("column", col, longRows.slice(0, 10), "suspicious_text_long", "low", 0.6, { examples: longRows.slice(0, 3).map((i) => vals[i]) }, "truncate_or_clean"));
      }
    }
  }

  // Run all
  runAll() {
    const schema = this.inferSchema();
    this.detectMissing();
    this.detectDuplicatesAndPK();
    this.runOutliers();
    this.detectDateParsing();
    this.detectCategoricalInconsistency();
    this.detectSuspiciousText();

    return { schema, issues: this.issues };
  }
}

module.exports = { Analyzer, CONFIG };



