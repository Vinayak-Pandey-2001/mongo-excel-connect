const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());

// ---- DB setup ----
const uri = 'mongodb+srv://venwiz-mvp:j2IgHVjt6lyq0SId@cluster1.vs2kj.mongodb.net/vendor-profile?retryWrites=true&w=majority';
const client = new MongoClient(uri);

async function connectDB() {
  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");
  } catch (err) {
    console.error(err);
  }
}

function collectKeysRecursive(obj, prefix, fieldSet) {
  for (const key in obj) {
    if (!Object.prototype.hasOwnProperty.call(obj, key)) continue;
    const value = obj[key];
    const fullKey = prefix ? `${prefix}.${key}` : key;
    fieldSet.add(fullKey);

    if (
      typeof value === 'object' &&
      value !== null &&
      !Array.isArray(value) &&
      !(value instanceof Date)
    ) {
      collectKeysRecursive(value, fullKey, fieldSet);
    }
  }
}

function coerceValue(value) {
  if (typeof value === 'string') {
    const d = new Date(value);
    if (!isNaN(d.getTime())) return d;
    const n = Number(value);
    if (!Number.isNaN(n) && value.trim() !== '') return n;
    return value;
  }
  return value;
}

function buildMongoQueryFromFilters(filters) {
  if (!filters || typeof filters !== 'object') return {};
  if (filters.$and && Array.isArray(filters.$and)) {
    return {
      $and: filters.$and.map(cond => {
        const k = Object.keys(cond)[0];
        const v = cond[k];
        if (v && typeof v === 'object') {
          const out = {};
          for (const [op, val] of Object.entries(v)) out[op] = coerceValue(val);
          return { [k]: out };
        }
        return { [k]: coerceValue(v) };
      })
    };
  }
  const out = {};
  for (const [field, ops] of Object.entries(filters)) {
    if (ops && typeof ops === 'object') {
      const o = {};
      for (const [op, val] of Object.entries(ops)) o[op] = coerceValue(val);
      if (Object.keys(o).length) out[field] = o;
    }
  }
  return out;
}

function sanitizeProjection(fields) {
  const projection = {};
  const topLevelFields = new Set();
  const nestedFields = new Set();
  for (const field of fields || []) {
    if (!field) continue;
    const parts = field.split(".");
    const topLevel = parts[0];
    const parentAdded = topLevelFields.has(topLevel) && parts.length > 1;
    const childAdded = nestedFields.has(topLevel) && parts.length === 1;
    if (parentAdded || childAdded) continue;
    projection[field] = 1;
    if (parts.length > 1) nestedFields.add(topLevel);
    else topLevelFields.add(field);
  }
  return projection;
}

// ---------- Endpoints ----------
app.get('/databases', async (req, res) => {
  const dbs = await client.db().admin().listDatabases();
  res.json(dbs.databases.map(d => d.name));
});

app.get('/collections', async (req, res) => {
  const db = client.db(req.query.db);
  const collections = await db.listCollections().toArray();
  res.json(collections.map(c => c.name));
});

app.get('/fields', async (req, res) => {
  const { db, collection } = req.query;
  console.log("Incoming request params:", { db, collection });
  const col = client.db(db).collection(collection);
  const sampleDocs = await col.aggregate([{ $sample: { size: 1000 } }]).toArray();
  console.log("Hi :", sampleDocs);
  
  const topLevelKeys = new Set();
  const allFields = new Set();

  const deepKeyCollector = (obj, prefix) => {
    for (const key in obj) {
      if (key.startsWith('_') && key !== '_id') continue;
      const value = obj[key];
      const fullKey = prefix ? `${prefix}.${key}` : key;
      allFields.add(fullKey);
      if (typeof value === 'object' && value !== null && !Buffer.isBuffer(value)) {
        deepKeyCollector(value, fullKey);
      }
    }
  };

  sampleDocs.forEach(doc => {
    Object.keys(doc).forEach(k => {
      if (!k.startsWith('_') || k === '_id') {
        topLevelKeys.add(k);
      }
    });
    deepKeyCollector(doc, '');
  });

  const filteredFields = Array.from(allFields).filter(f => !f.toLowerCase().includes('buffer'));
  res.json({
    topLevelKeys: Array.from(topLevelKeys),
    otherFields: filteredFields.filter(f => !topLevelKeys.has(f))
  });
});

app.post('/fetch', async (req, res) => {
  const { dbName, collection, fields, filters } = req.body;
  const db = client.db(dbName);
  const col = db.collection(collection);

  const projection = sanitizeProjection(fields);
  const query = buildMongoQueryFromFilters(filters);

  try {
    const docs = await col.find(query, { projection }).toArray();
    res.json(docs);
  } catch (err) {
    console.error("❌ MongoDB fetch error:", err);
    res.status(500).json({ error: "Error fetching documents" });
  }
});

// ---- Start Server ----
connectDB().then(() => {
  const enrichedRoutes = require("./routes/enriched");
  app.use(enrichedRoutes);

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));
});