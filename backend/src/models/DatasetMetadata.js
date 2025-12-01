const mongoose = require("mongoose")

const ColumnMetaSchema = new mongoose.Schema({
  name: { type: String, required: true },
  dtype: { type: String, required: true },
  missing_count: { type: Number, required: true },
  unique_count: { type: Number, default: null },
  example_values: { type: [mongoose.Schema.Types.Mixed], default: [] },
});

const IssueSchema = new mongoose.Schema({
  column: { type: String, required: true },
  issue_type: { type: String, required: true },
  description: { type: String, required: true },
  severity: { type: String, required: true }, // "low", "medium", "high"
});

const TransformationSchema = new mongoose.Schema({
  name: { type: String, required: true },
  parameters: { type: Object, required: true },
  timestamp: { type: Date, default: Date.now },
});

const ProvenanceEventSchema = new mongoose.Schema({
  actor: { type: String, required: true },
  action: { type: String, required: true },
  timestamp: { type: Date, default: Date.now },
  details: { type: Object, default: null },
});

const DatasetMetadataSchema = new mongoose.Schema({
  id: {
    type: String,
    default: () => new mongoose.Types.ObjectId().toString(),
  },
  filename: { type: String, required: true },
  cloudinary_url: { type: String, required: true },
  uploaded_at: { type: Date, default: Date.now },
  size: { type: Number, required: true },

  // --- Extended Metadata ---
  preview: { type: [Object], default: [] },
  schema: {
    type: Map,
    of: ColumnMetaSchema,
    required: true
  },

  issues: { type: [IssueSchema], default: [] },
  transformations: { type: [TransformationSchema], default: [] },
  provenance: { type: [ProvenanceEventSchema], default: [] },

  // --- Common Metadata ---
  row_count: { type: Number, required: true },
  status: { type: String, default: "raw" },
  notes: { type: String, default: null },
});

const DatasetMetadata = mongoose.model(
  "DatasetMetadata",
  DatasetMetadataSchema
);

module.exports = DatasetMetadata


// const mongoose = require("mongoose");

// //
// // ─────────────────────────────────────────────────────────
// //   COLUMN META SUBDOCUMENT
// // ─────────────────────────────────────────────────────────
// //
// const ColumnMetaSchema = new mongoose.Schema({
//   name: { type: String, required: true },
//   dtype: { type: String, required: true },
//   missing_count: { type: Number, required: true },
//   unique_count: { type: Number, default: null },
//   example_values: { type: [mongoose.Schema.Types.Mixed], default: [] },
// });

// //
// // ─────────────────────────────────────────────────────────
// //   OTHER SUBDOCS
// // ─────────────────────────────────────────────────────────
// //
// const IssueSchema = new mongoose.Schema({
//   column: { type: String, required: true },
//   issue_type: { type: String, required: true },
//   description: { type: String, required: true },
//   severity: { type: String, required: true },
// });

// const TransformationSchema = new mongoose.Schema({
//   name: { type: String, required: true },
//   parameters: { type: Object, required: true },
//   timestamp: { type: Date, default: Date.now },
// });

// const ProvenanceEventSchema = new mongoose.Schema({
//   actor: { type: String, required: true },
//   action: { type: String, required: true },
//   timestamp: { type: Date, default: Date.now },
//   details: { type: Object, default: null },
// });

// //
// // ─────────────────────────────────────────────────────────
// //   MAIN DOCUMENT SCHEMA
// // ─────────────────────────────────────────────────────────
// //
// const DatasetMetadataSchema = new mongoose.Schema({
//   id: {
//     type: String,
//     default: () => new mongoose.Types.ObjectId().toString(),
//   },
//   filename: { type: String, required: true },
//   cloudinary_url: { type: String, required: true },
//   uploaded_at: { type: Date, default: Date.now },
//   size: { type: Number, required: true },

//   preview: { type: [Object], default: [] },

//   //
//   // REPLACED Mongoose Map (buggy) WITH NORMAL OBJECT (stable)
//   // schema: { columnName: ColumnMeta }
//   //
//   schema: {
//     type: Object,
//     default: {},
//   },

//   issues: { type: [IssueSchema], default: [] },
//   transformations: { type: [TransformationSchema], default: [] },
//   provenance: { type: [ProvenanceEventSchema], default: [] },

//   row_count: { type: Number, required: true },
//   status: { type: String, default: "raw" },
//   notes: { type: String, default: null },
// });

// //
// // ─────────────────────────────────────────────────────────
// //   PRE-VALIDATE SANITIZER (NO EMPTY KEYS, NO MISSING FIELDS)
// // ─────────────────────────────────────────────────────────
// //
// DatasetMetadataSchema.pre("validate", function (next) {
//   try {
//     const raw = this.schema || {};
//     const cleaned = {};

//     for (const [key, value] of Object.entries(raw)) {
//       const col = key.trim();
//       if (!col) continue;

//       const v = value || {};

//       cleaned[col] = {
//         name: v.name?.trim() || col,
//         dtype: v.dtype || "string",
//         missing_count: Number(v.missing_count || 0),
//         unique_count: v.unique_count ?? null,
//         example_values: Array.isArray(v.example_values)
//           ? v.example_values
//           : [],
//       };
//     }

//     this.schema = cleaned;
//     next();
//   } catch (err) {
//     console.error("Schema sanitize error:", err);
//     next(err);
//   }
// });

// //
// // ─────────────────────────────────────────────────────────
// //   EXPORT MODEL
// // ─────────────────────────────────────────────────────────
// //
// module.exports = mongoose.model("DatasetMetadata", DatasetMetadataSchema);
