const express = require("express");
const fs = require("fs");
const fetch = require("node-fetch");
const dfd = require("danfojs-node");
const cloudinary = require("cloudinary").v2;
const path = require("path");

// Import Analyzer + Cleaner
const { Analyzer } = require("../agents/Analyzer/Analyzer.js");
const { Cleaner } = require("../agents/Cleaner/cleaner.js");

const {
    CoerceNumeric,
    CoerceDatetime,
    FillMissing,
    ClipOutliersIQR,
    MapCategorical
} = require("../agents/Cleaner/transformations.js");

const DatasetMetadata = require("../models/DatasetMetadata.js");



const router = express.Router();

function isNumericColumn(df, col) {
    return df[col].values.every(v => v === null || v === undefined || v === "" || !isNaN(Number(v)));
}


// -----------------------------------------
// POST /datasets/:id/clean?auto=true|false
// -----------------------------------------
router.post("/:id/clean", async (req, res) => {
    try {
        const { id } = req.params;
        const auto = req.query.auto === "true"; // autoClean button

        const dataset = await DatasetMetadata.findById(id);
        if (!dataset) return res.status(404).json({ error: "Dataset not found" });

        // Step 1 - Download raw CSV
        const tmpPath = `temp_${Date.now()}.csv`;
        const resp = await fetch(dataset.cloudinary_url);
        const buf = Buffer.from(await resp.arrayBuffer());
        fs.writeFileSync(tmpPath, buf);

        // Step 2 - Load DF
        const df = await dfd.readCSV(tmpPath);
        console.log("DF Columns:", df.columns);


        // Step 3 - Run Analyzer
        const analyzer = new Analyzer(df);
        const analysis = analyzer.runAll();

        // Step 4 - Build Cleaner pipeline (based on issues)
        const cleaner = new Cleaner(df);

        console.log("ANALYSIS ISSUES:", analysis.issues);


        const seq = [];

        for (const issue of analysis.issues) {
            const col = issue.column;

            if (!df.columns.includes(col)) {
                console.warn("Skipping invalid column:", col);
                continue;
            }

            switch (issue.issue_type) {
                case "missing_values":
                    seq.push({
                        transformation: new FillMissing({
                            column: col,
                            strategy: isNumericColumn(df, col) ? "median" : "mode"
                        }),
                        options: { autoClean: auto, userId: "system" }
                    });
                    break;

                case "outlier_iqr":
                    seq.push({
                        transformation: new ClipOutliersIQR({ column: col, method: "clip" }),
                        options: { autoClean: auto, userId: "system" }
                    });
                    break;


                case "categorical_inconsistency":
                    seq.push({
                        transformation: new MapCategorical({
                            column: col,
                            fuzzy: true,
                            threshold: 0.6 // FIXED
                        }),
                        options: { autoClean: auto, userId: "system" }
                    });
                    break;

                case "date_parse_failures":
                    seq.push({
                        transformation: new CoerceDatetime({ column: col }),
                        options: { autoClean: auto, userId: "system" }
                    });
                    break;

                default:
                    break;
            }
        }

        console.log("seq", seq)
        // Step 5 - Run cleaner
        cleaner.applySequence(seq);

        const finalDf = cleaner.getCurrentDf();

        // Save cleaned file locally
        const cleanedPath = path.join(__dirname, `cleaned_${Date.now()}.csv`);
        const cleanedCsv = finalDf.toCSV({ download: false });
        fs.writeFileSync(cleanedPath, cleanedCsv);


        // Upload cleaned file
        const upload = await cloudinary.uploader.upload(cleanedPath, {
            resource_type: "raw",
            folder: "data_cleaning_agent/cleaned"
        });


        // Step 6 - Save metadata
        dataset.cleaned_version_url = upload.secure_url;
        dataset.issues = analysis.issues.map(issue => ({
            column: issue.column,
            issue_type: issue.issue_type,
            description: issue.description ?? `Auto-detected issue: ${issue.issue_type}`,
            severity: issue.severity ?? "medium"
        }));

        dataset.transformations = cleaner.transformations.map(t => ({
            name: t.name ?? t.transformationName ?? "unknown",
            parameters: t.params ?? t.parameters ?? {},
            timestamp: new Date()
        }));

        dataset.provenance = cleaner.provenance.map(p => ({
            actor: p.actor ?? "system",
            action: p.action ?? "transformation",
            timestamp: new Date(),
            details: p.details ?? p
        }));
        dataset.preview = dataset.preview ?? [];



        const schemaKeys = Object.keys(analysis.schema);

        // HARD FILTER (removes "" | null | undefined | spaces | weird keys)
        const validKeys = schemaKeys.filter(k => typeof k === "string" && k.trim().length > 0);

        // LOG TO CONFIRM
        console.log("RAW_SCHEMA_KEYS:", schemaKeys);
        console.log("VALID_SCHEMA_KEYS:", validKeys);

        const finalSchema = {};

        for (const col of validKeys) {
            const info = analysis.schema[col];

            finalSchema[col] = {
                name: col,
                dtype: info.inferred_type || info.pandas_dtypes || "string",
                missing_count: info.missing_count ?? 0,
                unique_count: info.nunique ?? null,
                example_values: info.example ?? []
            };
        }


        dataset.schema = finalSchema;




        await dataset.save();

        // Cleanup
        fs.unlinkSync(tmpPath);
        fs.unlinkSync(cleanedPath);

        res.json({
            message: "Dataset cleaned successfully",
            cleaned_url: upload.secure_url,
            provenance: cleaner.provenance
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Cleaning failed" });
    }
});

module.exports = router
