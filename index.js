const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());

// ---- DB setup (unchanged) ----
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

connectDB().then(() => {
  app.locals.client = client;
  const enrichedRoutes = require("./routes/enriched");
  app.use(enrichedRoutes);

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));
});

function detectScalarType(value) {
  if (value instanceof Date) return 'date';
  if (typeof value === 'string') {
    const d = new Date(value);
    if (!isNaN(d.getTime())) return 'date';
    return 'string';
  }
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  return 'string';
}

// Prefer strongest signal: date > number > boolean > string > unknown
function mergeTypes(existing, incoming) {
  const rank = { date: 4, number: 3, boolean: 2, string: 1, array: 0, unknown: -1 };
  if (!existing) return incoming;
  return (rank[incoming] > rank[existing]) ? incoming : existing;
}

function collectKeysAndTypes(obj, prefix, fieldMap) {
  if (!obj || typeof obj !== 'object') return;

  if (Array.isArray(obj)) {
    // For arrays, inspect elements to discover nested keys/types
    for (const el of obj) collectKeysAndTypes(el, prefix, fieldMap);
    // Also mark the path as array if we have a name at this level
    if (prefix && !fieldMap.has(prefix)) fieldMap.set(prefix, 'array');
    return;
  }

  for (const key of Object.keys(obj)) {
    const path = prefix ? `${prefix}.${key}` : key;
    const val = obj[key];

    if (val && typeof val === 'object' && !(val instanceof Date)) {
      collectKeysAndTypes(val, path, fieldMap);
    } else {
      const t = Array.isArray(val) ? 'array' : detectScalarType(val);
      const existing = fieldMap.get(path);
      fieldMap.set(path, mergeTypes(existing, t));
    }
  }
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

// UPDATED: /fields — no reliance on createdAt; use $sample; include types
app.get('/fields', async (req, res) => {
  try {
    const { db, collection } = req.query;
    const col = client.db(db).collection(collection);

    // Sample without assuming createdAt exists
    const SAMPLE_SIZE = 2000; // tune as needed
    const sampleDocs = await col.aggregate([{ $sample: { size: SAMPLE_SIZE } }]).toArray();

    const fieldMap = new Map();
    for (const doc of sampleDocs) {
      collectKeysAndTypes(doc, '', fieldMap);
    }

    const result = Array.from(fieldMap.entries()).map(([field, type]) => ({ field, type }));
    res.json(result);
  } catch (e) {
    console.error('❌ /fields error:', e);
    res.status(500).json({ error: 'Failed to fetch fields' });
  }
});

// keep your original key collector for other uses if needed
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

// sanitize parent/child projection collisions
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

// Convert ISO-ish strings into Date when appropriate (server-side safety)
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

// Convert incoming filters into a Mongo query with correct types
function buildMongoQueryFromFilters(filters) {
  // We support both a plain object of { field: { $gte, $lt, $eq, $ne } }
  // and a top-level { $and: [ ... ] } if the client sends that.
  if (!filters || typeof filters !== 'object') return {};

  // If already an AND query, coerce its values and pass through
  if (filters.$and && Array.isArray(filters.$and)) {
    const coerced = filters.$and.map(cond => {
      const k = Object.keys(cond)[0];
      const v = cond[k];
      if (v && typeof v === 'object') {
        const out = {};
        for (const [op, val] of Object.entries(v)) out[op] = coerceValue(val);
        return { [k]: out };
      }
      return { [k]: coerceValue(v) };
    });
    return { $and: coerced };
  }

  // Otherwise, coerce operator values per field
  const out = {};
  for (const [field, ops] of Object.entries(filters)) {
    if (ops && typeof ops === 'object') {
      const o = {};
      for (const [op, val] of Object.entries(ops)) {
        if (op === '$and' && Array.isArray(val)) {
          // Promote nested ANDs to top-level later
          continue;
        }
        o[op] = coerceValue(val);
      }
      if (Object.keys(o).length) out[field] = o;
    }
  }
  return out;
}

// UPDATED: /fetch — accept filters and apply to find()
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
    console.error("⚠️ Projection used:", projection);
    console.error("⚠️ Query used:", JSON.stringify(query));
    res.status(500).json({ error: "Error fetching documents" });
  }
});

// (If you truly need a second app.listen due to your routes/enriched wiring, keep it as-is below,
// but generally only one listen per process is recommended.)
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));