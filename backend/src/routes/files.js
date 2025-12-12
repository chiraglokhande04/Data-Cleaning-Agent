const express = require("express");
require('dotenv').config();
const multer = require("multer");
const fs = require("fs");
const cloudinary = require("cloudinary").v2;
const { parse } = require("fast-csv");
const DatasetMetadata = require("../models/DatasetMetadata.js");

const router = express.Router();
const upload = multer({ dest: "uploads/" });

// Cloudinary setup
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

router.post("/upload", upload.single("file"), async (req, res) => {
  if (!req.file)
    return res.status(400).json({ error: "File is required" });

  const filePath = req.file.path;

  const preview = [];
  const columnStats = {};
  let rowCount = 0;

  // ------------------------------
  // FIX #1 → SANITIZE HEADERS
  // ------------------------------
  let headerIndex = 1;
  const sanitizeHeader = (col) => {
    const cleaned = (col || "").trim();

    if (!cleaned) {
      return `column_${headerIndex++}`; // auto rename empty header
    }

    return cleaned;
  };
  // ------------------------------

  const stream = fs.createReadStream(filePath).pipe(parse({ headers: (headers) =>
    headers.map(h => sanitizeHeader(h)) // FIX #2: sanitize headers BEFORE data event
  }));

  return new Promise((resolve) => {
    stream
      .on("error", (err) => {
        console.error(err);
        res.status(500).json({ error: "CSV parsing failed" });
        resolve();
      })

      .on("data", (row) => {
        rowCount++;

        if (preview.length < 5) preview.push(row);

        Object.keys(row).forEach((col) => {
          // FIX #3: ensure sanitized (paranoia check)
          const cleanedCol = sanitizeHeader(col);

          if (!columnStats[cleanedCol]) {
            columnStats[cleanedCol] = {
              missing_count: 0,
              unique_set: new Set(),
              example_values: new Set(),
            };
          }

          const val = row[col];

          if (val === "" || val === null || val === undefined) {
            columnStats[cleanedCol].missing_count++;
          } else {
            columnStats[cleanedCol].unique_set.add(val);

            if (columnStats[cleanedCol].example_values.size < 3) {
              columnStats[cleanedCol].example_values.add(val);
            }
          }
        });
      })

      .on("end", async () => {
        // ------------------------------
        // FIX #4: Ensure schema entries always contain a valid "name"
        // ------------------------------
        const schema = {};
        for (const col of Object.keys(columnStats)) {
          schema[col] = {
            name: col, // FIXED: col is always non-empty due to sanitizeHeader
            dtype: "string",
            missing_count: columnStats[col].missing_count,
            unique_count: columnStats[col].unique_set.size,
            example_values: [...columnStats[col].example_values],
          };
        }
        // ------------------------------

        // ---- Upload CSV file to Cloudinary ----
        const uploadResult = await cloudinary.uploader.upload(filePath, {
          resource_type: "raw",
          folder: "data_cleaning_agent/datasets",
        });

        const metadata = new DatasetMetadata({
          filename: req.file.originalname,
          cloudinary_url: uploadResult.secure_url,
          size: fs.statSync(filePath).size,
          row_count: rowCount,
          preview,
          schema, // FIXED
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

        fs.unlinkSync(filePath);

        res.json({
          message: "File uploaded",
          dataset: metadata,
        });

        resolve();
      });
  });
});

module.exports = router;
