// server.js
require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const cron = require("node-cron");

const { connect } = require("./db");
const routes = require("./routes");
const { inferAndMaybeCreateVersion } = require("./infer");
const { normalizePending } = require("./transform");

const PORT = process.env.PORT || 3000;
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://localhost:27017/dynamic_etl_demo";
const SCHEMA_CRON = process.env.SCHEMA_CRON || "*/1 * * * *";

async function start() {
  await connect(MONGODB_URI);

  const app = express();
  app.use(cors());
  app.use(bodyParser.json({ limit: '2mb' }));
  app.use("/api", routes);

  app.get("/", (req, res) => res.send("Dynamic ETL backend running"));

  app.listen(PORT, () => console.log(`Server listening on ${PORT}`));

  // schedule schema inference & normalization periodically
  console.log(`Scheduling schema job with cron: ${SCHEMA_CRON}`);
  cron.schedule(SCHEMA_CRON, async () => {
    console.log("Running scheduled schema inference + normalization...");
    try {
      await inferAndMaybeCreateVersion(parseInt(process.env.SAMPLE_SIZE || "50"), parseFloat(process.env.SCHEMA_OPTIONAL_THRESHOLD || "0.05"), "cron");
      await normalizePending();
    } catch (err) {
      console.error("Scheduled job error:", err.message);
    }
  });
}

start().catch(err => {
  console.error("Failed to start app:", err);
  process.exit(1);
});
