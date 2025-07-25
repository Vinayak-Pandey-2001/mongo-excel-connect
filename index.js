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

connectDB();

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
  const doc = await col.findOne();
  res.json(doc ? Object.keys(doc) : []);
});

app.post('/fetch', async (req, res) => {
  const { dbName, collection, fields } = req.body;
  const db = client.db(dbName);
  const col = db.collection(collection);

  const projection = Object.fromEntries(fields.map(f => [f, 1]));

  try {
    const docs = await col.find({}, { projection }).toArray();
    res.json(docs);
  } catch (err) {
    console.error("Fetch error:", err);
    res.status(500).json({ error: "Error fetching documents" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ Server running on http://localhost:${PORT}`));