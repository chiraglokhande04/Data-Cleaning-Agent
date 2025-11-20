// import { z } from "zod";
// import { v4 as uuidv4 } from "uuid";

// // ---------- ColumnMeta ----------
// export const ColumnMeta = z.object({
//   name: z.string(),
//   dtype: z.string(),
//   missing_count: z.number(),
//   unique_count: z.number().optional(),
//   example_values: z.array(z.any()).optional(),
// });

// // ---------- Issue ----------
// export const Issue = z.object({
//   column: z.string(),
//   issue_type: z.string(),          // e.g. "missing_values", "outlier"
//   description: z.string(),
//   severity: z.string(),            // "low" | "medium" | "high"
// });

// // ---------- Transformation ----------
// export const Transformation = z.object({
//   name: z.string(),                // "fillna", "drop_duplicates", ...
//   parameters: z.record(z.any()),
//   timestamp: z
//     .string()
//     .datetime()
//     .default(() => new Date().toISOString()),
// });

// // ---------- ProvenanceEvent ----------
// export const ProvenanceEvent = z.object({
//   actor: z.string(),               // agent or user
//   action: z.string(),
//   timestamp: z
//     .string()
//     .datetime()
//     .default(() => new Date().toISOString()),
//   details: z.record(z.any()).optional(),
// });

// // ---------- DatasetMetadata ----------
// export const DatasetMetadata = z.object({
//   id: z.string().default(() => uuidv4()),
//   filename: z.string(),
//   cloudinary_url: z.string(),
//   uploaded_at: z
//     .string()
//     .datetime()
//     .default(() => new Date().toISOString()),
//   size: z.number(),

//   // Extended metadata
//   preview: z.array(z.record(z.any())),
//   schema: z.record(ColumnMeta),
//   issues: z.array(Issue).default([]),
//   transformations: z.array(Transformation).default([]),
//   provenance: z.array(ProvenanceEvent).default([]),

//   // Common metadata
//   row_count: z.number(),
//   status: z.string().default("raw"),    // raw | cleaned | validated
//   notes: z.string().optional(),
// });


import mongoose from "mongoose";

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
    default: {},
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

export default DatasetMetadata;

