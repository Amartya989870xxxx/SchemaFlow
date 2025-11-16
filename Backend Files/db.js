// db.js
const mongoose = require("mongoose");

const rawRecordSchema = new mongoose.Schema({
  payload: { type: mongoose.Schema.Types.Mixed, required: true },
  source: { type: String },
  fileMetadata: { type: mongoose.Schema.Types.Mixed },
  ingestedAt: { type: Date, default: Date.now }
}, { strict: false });

const schemaVersionSchema = new mongoose.Schema({
  version: { type: Number, required: true },
  createdAt: { type: Date, default: Date.now },
  fields: { type: mongoose.Schema.Types.Mixed, required: true },
  totalSamples: { type: Number, required: true },
  notes: { type: String }
});

const normalizedSchema = new mongoose.Schema({
  canonical: { type: mongoose.Schema.Types.Mixed, required: true },
  rawId: { type: mongoose.Schema.Types.ObjectId, ref: 'RawRecord' },
  schemaVersion: { type: Number },
  normalizedAt: { type: Date, default: Date.now }
}, { strict: false });

const RawRecord = mongoose.model("RawRecord", rawRecordSchema);
const SchemaVersion = mongoose.model("SchemaVersion", schemaVersionSchema);
const NormalizedRecord = mongoose.model("NormalizedRecord", normalizedSchema);

async function connect(uri) {
  await mongoose.connect(uri);
  console.log("Connected to MongoDB");
}

module.exports = { connect, RawRecord, SchemaVersion, NormalizedRecord };
