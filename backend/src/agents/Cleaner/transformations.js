const { v4: uuidv4 } = require("uuid");
const Fuse = require("fuse.js")


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
            const out = isNaN(parsed) ? null : new Date.parse(parsed).toISOString()
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

        const values = records.map((r) => r[column]).filter((v) => v !== null && v !== undefined && v !== "")
        let fillValue = value

        if (strategy == "mean") {
            const nums = values.map(Number).filter((x) => !isNaN(x))
            fillValue = nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : null
        } else if (strategy == "median") {
            const nums = values.map(Number).filter((x) => !isNaN(x)).sort((a, b) => a - b)
            fillValue = nums.length ? nums[Math.floor(nums.length / 2)] : null
        } else if (strategy == "mode") {
            freq = {}
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
        const nums = records.map((r) => { const v = Number(r[column]); return isNaN(v) ? null : v }).filter((x) => x !== null)
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
            const evidence = {method, lower, upper, changed_count: changed}
            return {records: newRecords, evidence}
        }else if (method == "flag"){
            const newRecords = records.map((r) => {
                const copy = Object.assign({}, r)
                const v = Number(copy[column])
                copy[flag_column_name] = !isNaN(v) && (v < lower || v > upper) ? true : (copy[flag_column_name] || false)

                if (copy[flag_column_name]) changed++
                return copy
            })
            const evidence = {method, lower, upper, flagged_count: changed}
            return {records: newRecords, evidence}
        }else if (method == "remove"){
            const kept = records.filter((r) => {
                const v = Number(r[column])
                return isNaN(v) ? true : (v >= lower && v <= upper)
            })

            const removed = records.length - kept.length
            const evidence = {method, lower, upper, removed_count: removed}
            return {records: kept, evidence}
        }

        return { records, evidence: {reason: "unknown_method"}}
    }
}


