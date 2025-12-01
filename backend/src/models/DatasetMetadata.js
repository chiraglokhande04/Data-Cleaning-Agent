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
