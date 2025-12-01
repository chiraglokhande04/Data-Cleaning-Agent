const { v4: uuidv4 } = require("uuid");
const fuse = require("fuse.js")


// Base class Transfromation
class Transformation {
    constructor(name, params = {}, destructive = false) {
        this.name = name;
        this.params = params;
        this.destructive = destructive
        this.id = uuidv4()
    }


    apply(records) {
        throw new Error("apply() not implemented")
    }

    toJSON() {
        return {
            id: this.id,
            name: this.name,
            params: this.params,
            destructive: this.destructive
        }
    }
}


// Helper Function
function sampleArray(arr, n = 5) {
    return arr.slice(0, n)
}



/** 1
 * CoerceNumeric: tries to cast column values to Number. Non-parsable -> null
 * params: { column: 'colname' }
 */
class CoerceNumeric extends Transformation {
    constructor(params = {}) {
        super("coerce_numeric", params, false)
    }

    apply(records) {
        const { column } = this.params
        const beforeSample = records.slice(0, 5).map((r) => r[column])
        let changed = 0
        const newRecords = records.map((r) => {
            const copy = Object.assign({}, r)
            const v = copy[column]

            const num = v === null || v === undefined || v === "" ? null : Number(v)

            if (v !== num && !(v === null && num === null)) changed += (num === v ? 0 : 1)
            copy[column] = isNaN(num) ? null : num
            return copy
        })

        const evidence = {
            before_sample: beforeSample,
            changed_count: changed
        }
        return { records: newRecords, evidence }
    }
}


/** 2
 * CoerceDatetime: tries to parse Date. Non-parsable -> null
 * params: { column: 'colname' }
*/
class CoerceDatetime extends Transformation {
    constructor(params = {}) {
        super("coerce_datetime", params, false)
    }

    apply(records) {
        const { column } = this.params
        const beforeSample = records.slice(0, 5).map((r) => r[column])
        let changed = 0

        const newRecords = records.map((r) => {
            const copy = Object.assign({}, r)
            const v = copy[column]

            const parsed = v === null || v === undefined || v === "" ? null : Date.parse(String(v))
            const out = isNaN(parsed) ? null : new Date(parsed).toISOString();
            if (String(parsed) !== String(out)) changed++
            copy[column] = out
            return copy
        })

        const evidence = {
            before_sample: beforeSample,
            changed_count: changed
        }

        return { records: newRecords, evidence }
    }
}


/** 3
 * FillMissing: fill a column with strategy {mean, median, mode, constant}
 * params: { column, strategy: 'mean'|'median'|'mode'|'constant', value? }
*/
class FillMissing extends Transformation {
    constructor(params = {}) {
        super("fill_missing", params, false)
    }

    apply(records) {
        const { column, strategy = "mean", value } = this.params

        const values = records.map((r) => r[column]).filter(v => v !== null && v !== undefined && v !== "")
        let fillValue = value

        if (strategy == "mean") {
            const nums = values.map(Number).filter(x => !isNaN(x))
            fillValue = nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : null
        } else if (strategy == "median") {
            const nums = values.map(Number).filter(x => !isNaN(x)).sort((a, b) => a - b)
            fillValue = nums.length ? nums[Math.floor(nums.length / 2)] : null
        } else if (strategy == "mode") {
            const freq = {}
            values.forEach((v) => { freq[v] = (freq[v] || 0) + 1 })
            const pairs = Object.entries(freq).sort((a, b) => b[1] - a[1])
            fillValue = pairs.length ? pairs[0][0] : null
        }

        const beforeRecords = records.slice(0, 5).map((r) => r[column])
        let changed = 0
        const newRecords = records.map((r) => {
            const copy = Object.assign({}, r)
            if (copy[column] === null || copy[column] === undefined || copy[column] === "") {
                copy[column] = fillValue
                changed++
            }
            return copy
        })

        const evidence = {
            before_records: beforeRecords,
            changed_count: changed,
            filled_value: fillValue
        }
        return { records: newRecords, evidence }
    }
}


/**
 * ClipOutliersIQR: clip numeric column to [lower, upper] computed by IQR k
 * params: { column, k = 1.5, method: 'clip'|'remove'|'flag', flag_column_name }
 * destructive=true only when method==='remove'
 */
class ClipOutliersIQR extends Transformation {
    constructor(params = {}) {
        const destructive = params.method === "remove"
        super("clip_outliers_iqr", params, destructive)
    }

    apply(records) {
        const { column, k = 1.5, method = "clip", flag_column_name = "_outliers" } = this.params
        const nums = records.map((r) => { const v = Number(r[column]); return isNaN(v) ? null : v }).filter(x => x !== null)
        if (nums.length < 5) return { records, evidence: { reason: "not_enough_numeric" } }

        nums.sort((a, b) => a - b)
        const q1 = nums[Math.floor(nums.length * 0.25)]
        const q3 = nums[Math.floor(nums.length * 0.75)]
        const iqr = q3 - q1
        const lower = q1 - k * iqr
        const upper = q1 + k * iqr

        let changed = 0

        if (method == "clip") {
            const newRecords = records.map((r) => {
                const copy = Object.assign({}, r)
                const v = Number(copy[column])
                if (!isNaN(v) && (v < lower || v > upper)) {
                    copy[column] = Math.max(Math.min(v, upper), lower)
                    changed++
                }
                return copy
            })
            const evidence = { method, lower, upper, changed_count: changed }
            return { records: newRecords, evidence }
        } else if (method == "flag") {
            const newRecords = records.map((r) => {
                const copy = Object.assign({}, r)
                const v = Number(copy[column])
                copy[flag_column_name] = !isNaN(v) && (v < lower || v > upper) ? true : (copy[flag_column_name] || false)

                if (copy[flag_column_name]) changed++
                return copy
            })
            const evidence = { method, lower, upper, flagged_count: changed }
            return { records: newRecords, evidence }
        } else if (method == "remove") {
            const kept = records.filter(r => {
                const v = Number(r[column])
                return isNaN(v) ? true : (v >= lower && v <= upper)
            })

            const removed = records.length - kept.length
            const evidence = { method, lower, upper, removed_count: removed }
            return { records: kept, evidence }
        }

        return { records, evidence: { reason: "unknown_method" } }
    }
}



/**
 * MapCategorical: map values by explicit map or fuzzy via Fuse.js
 * params: { column, mapping: {old: new, ...} } OR { fuzzy: true, threshold: 0.85 }
 */
class MapCategorical extends Transformation {
    constructor(params = {}) {
        super("map_categorical", params, false);
    }

    apply(records) {
        const { column, fuzzy = false, threshold = 0.6, mapping = null } = this.params;

        // Explicit mapping case
        if (mapping && typeof mapping === "object") {
            const newRecords = records.map(r => {
                const copy = { ...r };
                if (copy[column] in mapping) {
                    copy[column] = mapping[copy[column]];
                }
                return copy;
            });
            return {
                records: newRecords,
                evidence: { mapping }
            };
        }

        // Normalize values for fuzzy match
        const rawVals = [...new Set(records.map(r => r[column]))]
            .filter(v => v !== null && v !== undefined);

        const normalizedVals = rawVals.map(v =>
            String(v).trim().toLowerCase().replace(/[^a-z0-9 ]/gi, "")
        );

        const fuseInstance = new fuse(
            normalizedVals.map((v, i) => ({ key: rawVals[i], norm: v })), 
            {
                keys: ["norm"],
                includeScore: true,
                threshold: 1
            }
        );

        const canonical = {};
        const used = new Set();

        for (let item of normalizedVals) {
            if (used.has(item)) continue;

            const matches = fuseInstance.search(item)
                .filter(m => (1 - m.score) >= threshold)
                .map(m => m.item);

            matches.forEach(m => used.add(m.norm));

            // canonical name = first seen raw value
            const canon = matches[0].key;
            for (let m of matches) {
                canonical[m.key] = canon;
            }
        }

        let changed = 0;
        const newRecords = records.map(r => {
            const copy = { ...r };
            if (canonical[copy[column]]) {
                copy[column] = canonical[copy[column]];
                changed++;
            }
            return copy;
        });

        return {
            records: newRecords,
            evidence: {
                canonical,
                changed_count: changed
            }
        };
    }
}


/**
 * DeriveColumn: create a new column derived from existing row using a JS function
 * params: { new_column, fn } where fn is a stringified function or actual function (row->{...})
 * Note: executing arbitrary JS from users is a security risk; pass function from server code, not raw user input.
 */
class DeriveColumn extends Transformation {
    constructor(params = {}) {
        super("derived_column", params, false)
    }

    apply(records) {
        const { new_column, fn } = this.params
        const beforeSample = records.slice(0, 5).map((r) => r[new_column])
        let changed = 0
        const newRecords = records.map((r) => {
            const copy = Object.assign({}, r)

            const value = (typeof fn === "function") ? fn(copy) : null
            if (value !== undefined) {
                copy[new_column] = value
                changed++
            }
            return copy
        })
        return { records: newRecords, evidence: { before_sample: beforeSample, changed_count: changed } };
    }
}


class DropEmpty extends Transformation {
    constructor(params = {}) {
        // params: { target: "row" | "column", threshold: 1.0 }
        // threshold default = 1.0 (drop only fully empty)
        super("drop_empty", params, true);
    }

    apply(records) {
        const { target = "column", threshold = 1.0 } = this.params;

        if (!records || records.length === 0) {
            return { records, evidence: { reason: "empty_dataset" } };
        }

        const columns = Object.keys(records[0]);

        const isEmpty = (v) =>
            v === null ||
            v === undefined ||
            v === "" ||
            (typeof v === "number" && isNaN(v));

        // --------------------------------------------------------------------
        // DROP EMPTY / MOSTLY EMPTY COLUMNS
        // --------------------------------------------------------------------
        if (target === "column") {
            const colsToDrop = [];

            for (let col of columns) {
                let missingCount = 0;
                let total = records.length;

                for (const row of records) {
                    if (isEmpty(row[col])) missingCount++;
                }

                const missingPct = total === 0 ? 1 : missingCount / total;

                if (missingPct >= threshold) {
                    colsToDrop.push(col);
                }
            }

            const newRecords = records.map((r) => {
                const copy = { ...r };
                colsToDrop.forEach((c) => delete copy[c]);
                return copy;
            });

            return {
                records: newRecords,
                evidence: {
                    dropped_columns: colsToDrop,
                    dropped_count: colsToDrop.length,
                    threshold,
                },
            };
        }

        // --------------------------------------------------------------------
        // DROP EMPTY ROWS (only fully empty makes sense)
        // --------------------------------------------------------------------
        if (target === "row") {
            const newRecords = records.filter((r) => {
                const values = Object.values(r);
                // threshold logic for rows is usually pointless — keep simple
                return !(values.filter(v => isEmpty(v)).length / values.length >= threshold);
            });

            const removed = records.length - newRecords.length;

            return {
                records: newRecords,
                evidence: {
                    removed_rows: removed,
                    threshold,
                },
            };
        }

        return {
            records,
            evidence: { reason: "invalid_target" },
        };
    }
}




module.exports = {
    Transformation,
    CoerceNumeric,
    CoerceDatetime,
    FillMissing,
    ClipOutliersIQR,
    MapCategorical,
    DeriveColumn,
    DropEmpty
};