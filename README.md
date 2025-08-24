# Anton

Anton is a lightweight SQL-like database built in Java, created from scratch as a learning project.
The goal of AntonDB is to explore the inner workings of databases, from parsing queries to executing them, while providing a minimal, working database engine.

## ✨ Features (Current)

1. Create Table: Define tables with schema (column names & types).
2. Insert Tuples: Add records into tables.
3. Select Tuples: Query and retrieve data by columns.
4. Basic CRUD: Full cycle of Create, Read, Update, and Delete operations.
5. Query Parser + Executor: SQL-like syntax parsing with execution on in-memory structures.

## 🛠️ Features in Progress / Planned

1. WHERE Clause: Conditional filtering of results.
2. Caching: Add result caching for repeated queries.
3. Filtering: Improve filtering mechanisms for faster data retrieval.
4. Update by Condition: Update rows based on WHERE filters.
5. Delete by Condition: Remove rows selectively.
6. Indexing: Implement basic indexing (B+ Tree) for faster lookups.
7. Transactions: Explore commit/rollback for safer operations.
8. Joins: (Ambitious, but planned) support basic inner joins between tables.

## 📖 Example Usage
1. Create a table. <br>
   CREATE TABLE users (id INT, name STRING)
3. Insert data. <br>
   INSERT INTO users VALUES (1, 'Alice')
4. Select data. <br>
   SELECT * FROM users

## ⚙️ Tech Stack

Language: Java

Core Concepts: Query parsing, execution engine, file I/O, indexing (planned)

## 🚧 Disclaimer

Anton is a learning project, not meant for production use.
The purpose is to understand the fundamentals of how databases work by building one from scratch.

## License

This project is licensed under the [MIT License](./LICENSE).
