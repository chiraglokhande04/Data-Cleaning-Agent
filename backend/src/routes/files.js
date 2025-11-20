import express from "express";
import multer from "multer";
import fs from "fs";
import cloudinary from "cloudinary";
import { parse } from "fast-csv";
import DatasetMetadata from "../src/models/DatasetMetadata.js";

const router = express.Router();
const upload = multer({ dest: "uploads/" });

// Cloudinary setup
cloudinary.v2.config({
  cloud_name: process.env.CLOUDINARY_CLOUD,
  api_key: process.env.CLOUDINARY_KEY,
  api_secret: process.env.CLOUDINARY_SECRET,
});

router.post("/upload", upload.single("file"), async (req, res) => {
  if (!req.file)
    return res.status(400).json({ error: "File is required" });

  const filePath = req.file.path;

  const preview = [];
  const columnStats = {};
  let rowCount = 0;

  const stream = fs.createReadStream(filePath).pipe(parse({ headers: true }));

  return new Promise((resolve) => {
    stream
      .on("error", (err) => {
        console.error(err);
        res.status(500).json({ error: "CSV parsing failed" });
        resolve();
      })

      .on("data", (row) => {
        rowCount++;

        // --- Preview: first 5 rows only ---
        if (preview.length < 5) preview.push(row);

        // --- Initialize per-column stats ---
        Object.keys(row).forEach((col) => {
          if (!columnStats[col]) {
            columnStats[col] = {
              missing_count: 0,
              unique_set: new Set(),
              example_values: new Set(),
            };
          }

          const val = row[col];

          // Missing count
          if (val === "" || val === null || val === undefined) {
            columnStats[col].missing_count++;
          } else {
            // Unique values
            columnStats[col].unique_set.add(val);

            // Example values
            if (columnStats[col].example_values.size < 3)
              columnStats[col].example_values.add(val);
          }
        });
      })

      .on("end", async () => {
        // ---- Build schema metadata ----
        const schema = {};
        for (const col of Object.keys(columnStats)) {
          schema[col] = {
            name: col,
            dtype: "string", // Can't infer types via stream unless you want extra logic
            missing_count: columnStats[col].missing_count,
            unique_count: columnStats[col].unique_set.size,
            example_values: [...columnStats[col].example_values],
          };
        }

        // ---- Upload raw CSV file to Cloudinary ----
        const uploadResult = await cloudinary.v2.uploader.upload(filePath, {
          resource_type: "raw",
          folder: "data_cleaning_agent",
        });

        // ---- Save in MongoDB ----
        const metadata = new DatasetMetadata({
          filename: req.file.originalname,
          cloudinary_url: uploadResult.secure_url,
          size: fs.statSync(filePath).size,
          row_count: rowCount,
          preview,
          schema,
          issues: [],
          transformations: [],
          provenance: [
            {
              actor: "System",
              action: "upload",
            },
          ],
        });

        await metadata.save();

        // Cleanup
        fs.unlinkSync(filePath);

        res.json({
          message: "File uploaded",
          dataset: metadata,
        });

        resolve();
      });
  });
});

export default router;
