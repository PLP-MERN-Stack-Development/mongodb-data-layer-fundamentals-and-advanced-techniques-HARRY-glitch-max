// queries.js - Run MongoDB queries on the books collection

const { MongoClient } = require('mongodb');

// Connection details
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // --------------------------
    // Task 2: Basic CRUD
    // --------------------------

    console.log("\n📌 Find all Fiction books:");
    console.log(await collection.find({ genre: "Fiction" }).toArray());

    console.log("\n📌 Find books published after 1950:");
    console.log(await collection.find({ published_year: { $gt: 1950 } }).toArray());

    console.log("\n📌 Find books by George Orwell:");
    console.log(await collection.find({ author: "George Orwell" }).toArray());

    console.log("\n📌 Update price of 'The Hobbit' to 16.99");
    await collection.updateOne({ title: "The Hobbit" }, { $set: { price: 16.99 } });
    console.log(await collection.findOne({ title: "The Hobbit" }));

    console.log("\n📌 Delete 'Moby Dick'");
    await collection.deleteOne({ title: "Moby Dick" });
    console.log("Deleted 'Moby Dick'");

    // --------------------------
    // Task 3: Advanced Queries
    // --------------------------

    console.log("\n📌 Find in-stock books published after 2010:");
    console.log(await collection.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    console.log("\n📌 Projection (title, author, price only):");
    console.log(await collection.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    console.log("\n📌 Sort by price ascending:");
    console.log(await collection.find().sort({ price: 1 }).toArray());

    console.log("\n📌 Sort by price descending:");
    console.log(await collection.find().sort({ price: -1 }).toArray());

    console.log("\n📌 Pagination - Page 1 (5 books):");
    console.log(await collection.find().skip(0).limit(5).toArray());

    console.log("\n📌 Pagination - Page 2 (next 5 books):");
    console.log(await collection.find().skip(5).limit(5).toArray());

    // --------------------------
    // Task 4: Aggregation
    // --------------------------

    console.log("\n📌 Average price of books by genre:");
    console.log(await collection.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray());

    console.log("\n📌 Author with the most books:");
    console.log(await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log("\n📌 Group books by publication decade:");
    console.log(await collection.aggregate([
      { $group: {
          _id: { $multiply: [ { $floor: { $divide: ["$published_year", 10] } }, 10 ] },
          count: { $sum: 1 }
      }},
      { $sort: { _id: 1 } }
    ]).toArray());

    // --------------------------
    // Task 5: Indexing
    // --------------------------

    console.log("\n📌 Creating index on title:");
    console.log(await collection.createIndex({ title: 1 }));

    console.log("\n📌 Creating compound index on author + published_year:");
    console.log(await collection.createIndex({ author: 1, published_year: -1 }));

    console.log("\n📌 Using explain() to check index performance for 'The Hobbit':");
    console.log(await collection.find({ title: "The Hobbit" }).explain("executionStats"));

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

runQueries().catch(console.error);
