// index.js — full
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());

// ---- DB setup (unchanged) ----
const uri = 'mongodb+srv://venwiz-mvp:j2IgHVjt6lyq0SId@cluster1.vs2kj.mongodb.net/vendor-profile?retryWrites=true&w=majority';
const client = new MongoClient(uri);

// Connect and then start server (single listen)
async function connectDB() {
  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    // mount enriched routes if present
    try {
      const enrichedRoutes = require('./routes/enriched');
      app.use(enrichedRoutes);
    } catch (e) {
      console.log("No enriched routes or failed to load ./routes/enriched — continuing.");
    }

    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}
connectDB();

// -------------------- helpers --------------------

// Detect a sensible type label for a value
function detectType(v) {
  if (v === null) return 'null';
  if (v === undefined) return 'undefined';
  if (typeof v === 'bigint') return 'bigint';
  if (typeof v === 'symbol') return 'symbol';
  if (typeof v === 'function') return 'function';

  const tag = Object.prototype.toString.call(v); // reliable across realms
  if (tag === '[object Date]') return 'date';
  if (tag === '[object Array]') return 'array';
  if (tag === '[object RegExp]') return 'regexp';
  if (tag === '[object Map]') return 'map';
  if (tag === '[object Set]') return 'set';
  if (tag === '[object Int8Array]' || tag.endsWith('Array]')) return 'typed-array';

  if (typeof v === 'number') {
    if (Number.isNaN(v)) return 'NaN';
    if (!Number.isFinite(v)) return 'infinity';
    return 'number';
  }

  if (typeof v === 'string') {
    // Strict ISO-8601-ish check before parsing to avoid environment-dependent parsing
    const isoLike = /^\d{4}-\d{2}-\d{2}(T.*Z?)?$/;
    if (isoLike.test(v) && !Number.isNaN(Date.parse(v))) return 'date';
    return 'string';
  }

  if (typeof v === 'boolean') return 'boolean';
  if (typeof v === 'object') return 'object';

  return 'unknown';
}

// Merge type signals (prefer strongest)
function mergeTypes(existing, incoming) {
  const rank = { date: 5, number: 4, boolean: 3, string: 2, object: 1, array: 0, unknown: -1 };
  if (!existing) return incoming;
  return (rank[incoming] > rank[existing]) ? incoming : existing;
}

// Collect raw keys and types from a document (including array indices)
function collectRawKeysAndTypes(obj, prefix, map) {
  if (obj === null || obj === undefined) return;

  // Arrays: walk elements and mark parent as array
  if (Array.isArray(obj)) {
    if (prefix) {
      map.set(prefix, mergeTypes(map.get(prefix), 'array'));
    }
    for (let i = 0; i < obj.length; i++) {
      collectRawKeysAndTypes(obj[i], prefix ? `${prefix}.${i}` : `${i}`, map);
    }
    return;
  }

  // Plain object
  if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (!Object.prototype.hasOwnProperty.call(obj, key)) continue;

      const full = prefix ? `${prefix}.${key}` : key;
      const val = obj[key];

      // If val is an object (but not Date/Buffer), recurse
      const isBuffer = (typeof Buffer !== 'undefined' && Buffer.isBuffer && Buffer.isBuffer(val));
      if (val !== null && typeof val === 'object' && !Array.isArray(val) && !(val instanceof Date) && !isBuffer) {
        collectRawKeysAndTypes(val, full, map);
      } else {
        // Scalar / Date / Buffer / Array element
        const t = detectType(val);
        map.set(full, mergeTypes(map.get(full), t));
      }
    }
  } else {
    // primitive at prefix
    const t = detectType(obj);
    if (prefix) map.set(prefix, mergeTypes(map.get(prefix), t));
  }
}

// Normalize a raw field path by removing all ".<number>" components anywhere.
function normalizePath(path) {
  return path.replace(/\.\d+/g, '');
}

/**
 * Build a Mongo query object from client-sent filters.
 * Coerces numeric/date strings into Numbers/Dates where possible.
 */
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

// sanitize projection to avoid parent/child collisions
function sanitizeProjection(fields) {
  const projection = {};
  const topLevelFields = new Set();
  const nestedFields = new Set();
  for (const field of fields || []) {
    if (!field) continue;
    const parts = field.split('.');
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

// -------------------- endpoints --------------------

app.get('/databases', async (req, res) => {
  try {
    const dbs = await client.db().admin().listDatabases();
    res.json(dbs.databases.map(d => d.name));
  } catch (err) {
    console.error('/databases error', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/collections', async (req, res) => {
  try {
    const dbName = req.query.db;
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();
    res.json(collections.map(c => c.name));
  } catch (err) {
    console.error('/collections error', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * /fields
 * Returns:
 * {
 *   topLevelKeys: [{ field: "domain_name", type: "string" }, ...],
 *   otherFields: [{ field: "reconciled_info.meta_data.website_link", type: "string" }, ...]
 * }
 */
app.get('/fields', async (req, res) => {
  try {
    const { db, collection } = req.query;
    if (!db || !collection) return res.status(400).json({ error: "db and collection required" });

    const col = client.db(db).collection(collection);

    // sample documents (use $sample to avoid relying on createdAt)
    const SAMPLE_SIZE = 10000; // tune if needed
    const sampleDocs = await col.aggregate([{ $sample: { size: SAMPLE_SIZE } }]).toArray();
    console.log(`/fields - sampleDocs count: ${sampleDocs.length}`);

    // Raw map of paths -> types (may include .0 .1 indexes)
    const rawMap = new Map();
    for (const doc of sampleDocs) {
      collectRawKeysAndTypes(doc, '', rawMap);
    }

    // Normalize paths (remove numeric indexes) and merge types
    const normalizedMap = new Map();
    for (const [rawPath, t] of rawMap.entries()) {
      const norm = normalizePath(rawPath);
      if (!norm) continue;
      normalizedMap.set(norm, mergeTypes(normalizedMap.get(norm), t));
    }

    // Top-level keys (from actual doc top-level properties)
    const topLevelTypes = new Map();
    for (const doc of sampleDocs) {
      for (const key of Object.keys(doc)) {
        if (key.startsWith('_') && key !== '_id') continue; // skip internals except _id
        const val = doc[key];
        topLevelTypes.set(key, mergeTypes(topLevelTypes.get(key), detectType(val)));
      }
    }

    // Filter out unwanted normalized keys
    const filtered = [];
    for (const [field, type] of normalizedMap.entries()) {
      const lower = field.toLowerCase();
      if (lower.includes('buffer')) continue;
      if (field.length > 50) continue;
      if (field.startsWith('_') && field !== '_id') continue;
      filtered.push({ field, type });
    }

    // Build arrays for response
    const topLevelKeys = Array.from(topLevelTypes.entries()).map(([field, type]) => ({ field, type }));
    const otherFields = filtered; // contains nested + normalized base fields

    res.json({ topLevelKeys, otherFields });
  } catch (err) {
    console.error('/fields error:', err);
    res.status(500).json({ error: err.message });
  }
});

// fetch endpoint (accepts fields array and filters object)
app.post('/fetch', async (req, res) => {
  const { dbName, collection, fields, filters } = req.body;
  if (!dbName || !collection) return res.status(400).json({ error: 'dbName and collection required' });

  try {
    const db = client.db(dbName);
    const col = db.collection(collection);

    const projection = sanitizeProjection(fields || []);
    const query = buildMongoQueryFromFilters(filters || {});
    console.log(query);
    const docs = await col.find(query, { projection }).toArray();
    res.json(docs);
  } catch (err) {
    console.error('/fetch error', err);
    res.status(500).json({ error: 'Error fetching documents' });
  }
});