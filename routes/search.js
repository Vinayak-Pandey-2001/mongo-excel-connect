// routes/search.js
const express = require("express");
const { Client } = require("@elastic/elasticsearch");

const router = express.Router();

// ES client — v8 client forced to ES8-compatible headers
const es = new Client({
  node: "https://0620dabd16044863be94de92adb946bd.ap-south-1.aws.elastic-cloud.com:443",
  auth: {
    apiKey: "TnBHbGpaQUJ5ZFdaWkVrcXM1SVg6MmlaUG94d2lUdVMtU2NlQVlBdThHdw=="
  },
  requestTimeout: 120000,
  headers: {},          // leave empty — do NOT manually add Accept
  apiVersion: "8.10.0"  // force v8 API; ES 9 cluster accepts this
});

// Your index
const ES_INDEX = "vz-platform-website-docs-index-v2";

// POST /search
router.post("/", async (req, res) => {
  try {
    const { numberInput1, multiselect1, multiselect2, textInput1 } = req.body;

    const query = {
      size: numberInput1 || 10,
      sort: [{ _score: { order: "desc" } }],
      _source: [
        "gstn","companyName","serviceDescription","aboutUs"
      ],
      // Start simple — test connectivity
      query: { match_all: {} }
    };

    const response = await es.search({ index: ES_INDEX, body: query });
    res.json(response.hits);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Elasticsearch query failed", details: err.message });
  }
});

module.exports = router;