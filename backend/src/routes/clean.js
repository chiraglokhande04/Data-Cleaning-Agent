const express = require("express");
const fs = require("fs");
const fetch = require("node-fetch");
const dfd = require("danfojs-node");
const cloudinary = require("cloudinary").v2;
const path = require("path");
// Reporter Agent
const runReport = require("../agents/Reporter/runReport.js"); 


// Import Analyzer + Cleaner
const { Analyzer } = require("../agents/Analyzer/Analyzer.js");
const { Cleaner } = require("../agents/Cleaner/cleaner.js");

const {
    CoerceNumeric,
    CoerceDatetime,
    FillMissing,
    ClipOutliersIQR,
    MapCategorical,
    DropEmpty
} = require("../agents/Cleaner/transformations.js");

const DatasetMetadata = require("../models/DatasetMetadata.js");



const router = express.Router();


// Helper Functions 
function isNumericColumn(df, col) {
    return df[col].values.every(v => v === null || v === undefined || v === "" || !isNaN(Number(v)));
}

function cleanMapKeys(map) {
    if (!map) return {};

    const clean = {};

    // Mongoose Maps can be iterated with map.forEach
    Object.keys(map).forEach((value, key) => {
        if (!key || typeof key !== "string") return;
        const trimmed = key.trim();
        if (!trimmed) return; // drop empty keys

        clean[trimmed] = value;
    });

    return clean;
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

        console.log("RAW SCHEMA KEYS:", Object.keys(analysis.schema));



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

                case "empty_column":
                    seq.push({
                        transformation: new DropEmpty({ target: "column" }),
                        options: { autoClean: auto, userId: "system" }
                    });
                    break;

                case "mostly_empty_column":
                    seq.push({
                        transformation: new DropEmpty({ target: "column", threshold: 0.95 }),
                        options: { autoClean: auto, userId: "system" }
                    });
                    break;

                case "empty_row":
                    seq.push({
                        transformation: new DropEmpty({ target: "row" }),
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
        dataset.issues = analysis.issues
            .filter(issue => issue.column && issue.column.trim().length > 0) // FIX #1
            .map(issue => ({
                column: issue.column.trim(),
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
        console.log("prov", dataset.provenance)
        dataset.preview = dataset.preview ?? [];



        // --- Build finalSchema ---
        const finalSchema = {};
        const rawKeys = analysis && analysis.schema ? Object.keys(analysis.schema) : [];

        for (const col of rawKeys) {
            if (!col || typeof col !== "string") continue;
            const trimmed = col.trim();
            if (!trimmed) continue;

            const info = analysis.schema[col] || {};
            finalSchema[trimmed] = {
                name: trimmed,
                dtype: info.inferred_type || info.pandas_dtypes || "string",
                missing_count: info.missing_count ?? 0,
                unique_count: info.nunique ?? null,
                example_values: info.example ?? []
            };
        }
        console.log("FINAL SCHEMA RAW:", finalSchema);

        const sanitizedNewSchema = cleanMapKeys(finalSchema);

        // Replace with sanitized new schema
        dataset.schema = sanitizedNewSchema;

        // Confirm assignment (log right before save)
        console.log("DATASET.SCHEMA KEYS AT SAVE:", Object.keys(dataset.schema || {}));

        await dataset.save();

        console.log("Generating Report…");

        // ---- STEP 1: RUN REPORTER AGENT ----
        const { mdPath, pdfPath } = await runReport(dataset._id);

        // ---- STEP 2: Upload PDF report to Cloudinary ----
        const reportUpload = await cloudinary.uploader.upload(pdfPath, {
            resource_type: "raw",
            folder: "data_cleaning_agent/reports"
        });

        // ---- STEP 3: Save cleaned_report_url in DB ----
        dataset.cleaned_report_url = reportUpload.secure_url;
        await dataset.save();

        // ---- STEP 4: Cleanup temp files ----
        fs.unlinkSync(tmpPath);
        fs.unlinkSync(cleanedPath);
        fs.unlinkSync(pdfPath);   // remove local pdf

        // ---- STEP 5: Return response ----
        res.json({
            message: "Dataset cleaned + analyzed + reported",
            cleaned_url: upload.secure_url,
            report_url: reportUpload.secure_url,
            dataset,
            provenance: cleaner.provenance
        });


    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "Cleaning failed" });
    }
});

module.exports = router
