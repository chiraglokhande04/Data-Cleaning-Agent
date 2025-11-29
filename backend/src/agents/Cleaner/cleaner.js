const dfd = require("danfojs-node");
const { v4: uuidv4 } = require("uuid");

class Cleaner {
    constructor(df) {
        this.originalDf = df

        let raw = dfd.toJSON(df, { format: "row" });
        // console.log("DEBUG DANFO JSON:", raw);

        // Normalize to array-of-objects
        // Convert column-oriented → row-oriented
        if (!Array.isArray(raw)) {
            const columns = Object.keys(raw);

            // Validate column lengths
            const length = raw[columns[0]].length;

            const normalized = [];

            for (let i = 0; i < length; i++) {
                const row = {};
                for (const col of columns) {
                    row[col] = raw[col][i];
                }
                normalized.push(row);
            }

            raw = normalized;
        }

        this.originalRecords = raw;
        this.currentRecords = JSON.parse(JSON.stringify(raw));


        this.originalRecords = raw;
        this.currentRecords = JSON.parse(JSON.stringify(raw));
        this.transformations = []
        this.provenance = []
    }

    _recordDf() {
        return new dfd.DataFrame(this.currentRecords)
    }

    getCurrentDf() {
        return this._recordDf()
    }

    applyTransformation(transformation, options = { autoClean: false, userId: null }) {

        const { autoClean = false, userId = null } = options;

        


        if (transformation.destructive && !autoClean) {

            const prov = {
                id: uuidv4(),
                transform: transformation.toJSON(),
                userId,
                autoClean,
                timestamp: new Date().toISOString(),
                applied: false,
                reason: "destructive_requires_confirmation",
            }

            this.provenance.push(prov)
            return { applied: false, suggestion: true, provenance: prov, currentDf: this.getCurrentDf() };
        }

        const beforeSample = this.currentRecords.slice(0, 5)
        const { records: newRecords, evidence } = transformation.apply(this.currentRecords)

        const prov = {
            id: uuidv4(),
            transform: transformation.toJSON(),
            userId,
            autoClean,
            timestamp: new Date().toISOString(),
            applied: true,
            evidence,
            before_sample: beforeSample,
            after_sample: newRecords.slice(0, 5),
            row_count_before: this.currentRecords.length,
            row_count_after: newRecords.length,
        };

        this.currentRecords = newRecords
        this.transformations.push(transformation.toJSON());
        this.provenance.push(prov);
        console.log("NEW RECORDS SAMPLE:", newRecords[0]);
        return { applied: true, suggestion: false, provenance: prov, currentDf: this.getCurrentDf() };

    }

    /**
  * apply a list/sequence of transformations with options per transform
  * transforms: [{ transformation, options }]
  */
    applySequence(seq = []) {
        const results = []
        for (const item of seq) {
            const { transformation, options } = item
            console.log("APPLYING:", transformation.name, transformation.params);

            const res = this.applyTransformation(transformation, options)
            results.push(res)
        }
        return results
    }

    resetAndReapply(n = null) {
        this.currentRecords = JSON.parse(JSON.stringify(this.originalRecords));
        if (n === null) return this.getCurrentDf();
        // reapply first n transformations from this.transformations? Note we only stored JSON of transformations,
        // we cannot re-run logic unless original Transformation objects are provided. So in practice you should
        // store transformations as objects (or persist params and types and be able to rehydrate).
        // For simplicity here, we clear provenance and transformations and expect caller to reapply from stored action list.
        this.transformations = [];
        this.provenance = [];
        return this.getCurrentDf();
    }

    // Export Provenance
    exportProvenance() {
        return this.provenance;
    }


    /**
     * Export current dataset metadata snapshot
     */
    snapshot() {
        return {
            row_count: this.currentRecords.length,
            preview: this.currentRecords.slice(0, 5),
            provenance_count: this.provenance.length,
            transformations_count: this.transformations.length,
        };
    }
}

module.exports = { Cleaner };

