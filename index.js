const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());

const uri = 'mongodb+srv://venwiz-mvp:j2IgHVjt6lyq0SId@cluster1.vs2kj.mongodb.net/vendor-profile?retryWrites=true&w=majority'; // Replace with yours
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
  app.use(enrichedRoutes); // Registers your /api/longlist/enriched route

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));
});

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
  const col = client.db(db).collection(collection);

  const sampleDocs = await col.find().sort({ createdAt: -1 }).limit(10000).toArray();
  const fieldSet = new Set();

  sampleDocs.forEach(doc => {
    collectKeysRecursive(doc, '', fieldSet);
  });

  res.json(Array.from(fieldSet));
});

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
      !(value instanceof Date) &&
      !(value._bsontype === 'ObjectId') &&
      !(Buffer.isBuffer(value))
    ) {
      collectKeysRecursive(value, fullKey, fieldSet);
    }
  }
}

app.post('/fetch', async (req, res) => {
  const { dbName, collection, fields } = req.body;
  const db = client.db(dbName);
  const col = db.collection(collection);

  function sanitizeProjection(fields) {
    const projection = {};
    const topLevelFields = new Set();
    const nestedFields = new Set();

    for (const field of fields) {
      if (!field) continue;

      const parts = field.split(".");
      const topLevel = parts[0];

      const isParentAlreadyAdded = topLevelFields.has(topLevel) && parts.length > 1;
      const isChildAlreadyAdded = nestedFields.has(topLevel) && parts.length === 1;

      if (isParentAlreadyAdded || isChildAlreadyAdded) {
        continue;
      }

      // Safe to add
      projection[field] = 1;

      if (parts.length > 1) {
        nestedFields.add(topLevel);
      } else {
        topLevelFields.add(field);
      }
    }

    return projection;
  }

  const projection = sanitizeProjection(fields);

  try {
    try {
      const docs = await col.find({}, { projection }).toArray();
      res.json(docs);
    } catch (err) {
      console.error("❌ MongoDB fetch error:", err);
      console.error("⚠️ Projection used:", projection);
      res.status(500).json({ error: "Error fetching documents" });
    }
  } catch (err) {
    console.error("Fetch error:", err);
    res.status(500).json({ error: "Error fetching documents" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));