# Cassandra/ScyllaDB Index Storage

## Overview

The `store_index.sh` script loads indexed data from HDFS into Cassandra/ScyllaDB tables using PySpark with the Spark ScyllaDB Connector.

This provides a fast, scalable NoSQL database solution for search queries with proper schema design for BM25 scoring.

## Schema Design

### 1. **Vocabulary Table**
Stores the indexed terms and their statistics.

```cql
CREATE TABLE vocabulary (
    term_id int PRIMARY KEY,
    term text,
    idf float,
    doc_frequency int
)
```

**Purpose**: Quick lookup of terms and their IDF scores
**Primary Key**: term_id (auto-assigned)
**Clustering**: None (lookup by term_id or secondary index on term)

**Example Data**:
```
term_id | term    | idf   | doc_frequency
--------|---------|-------|---------------
   1    | machine | 2.08  | 5
   2    | learning| 1.61  | 8
   3    | neural  | 3.30  | 2
```

### 2. **Inverted Index Table**
Stores the posting lists with term frequencies and TF-IDF scores.

```cql
CREATE TABLE inverted_index (
    term text,
    doc_id text,
    term_freq int,
    tfidf float,
    PRIMARY KEY (term, doc_id)
) WITH CLUSTERING ORDER BY (doc_id ASC)
```

**Purpose**: Core search index - retrieve documents containing a term
**Primary Key**: (term, doc_id)
  - Partition key: term (groups all documents for a term)
  - Clustering key: doc_id (sorts documents within partition)
**Clustering Order**: doc_id ascending (for range queries)

**Example Data**:
```
term    | doc_id | term_freq | tfidf
--------|--------|-----------|-------
machine | doc1   | 3         | 6.24
machine | doc2   | 1         | 2.08
machine | doc3   | 2         | 4.16
neural  | doc5   | 1         | 3.30
```

**Query Pattern**:
```cql
SELECT * FROM inverted_index WHERE term = 'machine'
-- Returns all documents containing "machine" with scores
```

### 3. **Document Statistics Table**
Stores document-level statistics needed for BM25 calculation.

```cql
CREATE TABLE document_stats (
    doc_id text PRIMARY KEY,
    unique_terms int,
    token_count int,
    avg_term_freq float
)
```

**Purpose**: BM25 length normalization - store document lengths
**Primary Key**: doc_id
**Use Case**: For BM25 formula: length normalization factor = |doc| / avgdl

**Example Data**:
```
doc_id | unique_terms | token_count | avg_term_freq
-------|--------------|-------------|---------------
doc1   | 250          | 1500        | 6.00
doc2   | 180          | 1200        | 6.67
doc3   | 320          | 2100        | 6.56
```

### 4. **Collection Statistics Table**
Stores collection-wide statistics for ranking algorithms.

```cql
CREATE TABLE collection_stats (
    stat_name text PRIMARY KEY,
    stat_value text
)
```

**Purpose**: Collection metadata needed for ranking
**Primary Key**: stat_name

**Example Data**:
```
stat_name  | stat_value
-----------|------------
total_docs | 100
index_date | 2024-04-03
total_terms| 5000
avg_doc_length | 6.21
```

## Usage

### Basic Usage

```bash
# Create keyspace and tables, then load data (default /input/data)
./store_index.sh --create-keyspace

# Load data from custom index path
./store_index.sh --create-keyspace --index-path /tmp/my_index

# Load into existing keyspace
./store_index.sh --cassandra-host scylla.example.com
```

### Advanced Options

```bash
# Custom replication factor
./store_index.sh --create-keyspace --replication-factor 3

# Custom Cassandra host and port
./store_index.sh --cassandra-host 192.168.1.10 --cassandra-port 9042

# Custom keyspace name
./store_index.sh --keyspace my_search --create-keyspace

# View help
./store_index.sh --help
```

### Environment Variables

```bash
# Override configuration via environment
export CASSANDRA_HOST=scylla1.example.com
export CASSANDRA_PORT=9042
export KEYSPACE_NAME=production_index
./store_index.sh
```

## Data Loading Process

### Step 1: Validation
- ✓ Check Spark is installed
- ✓ Check Python3 is available
- ✓ Verify load script exists
- ✓ Verify index exists in HDFS

### Step 2: Cassandra Connection
- ✓ Test connection to Cassandra/ScyllaDB
- ✓ Verify host and port are reachable

### Step 3: Create Schema (optional)
- ✓ Create keyspace with specified replication factor
- ✓ Create 4 tables with proper schemas
- ✓ Create indexes if needed

### Step 4: Load Data (PySpark)
- ✓ Read index from HDFS
- ✓ Parse vocabulary entries
- ✓ Parse inverted index postings
- ✓ Parse document statistics
- ✓ Parse collection statistics
- ✓ Write to Cassandra using Spark connector

### Step 5: Verification
- ✓ Count rows in each table
- ✓ Verify data integrity
- ✓ Print summary statistics

## Performance Characteristics

### Storage
```
Term Vocabulary:      O(V) where V = vocabulary size
Inverted Index:       O(D × V) where D = documents
Document Stats:       O(D)
Total:               ~1.5x original index size
```

### Query Performance

**Single Term Search**:
```cql
SELECT * FROM inverted_index WHERE term = 'machine'
-- O(1) - direct partition access
-- Returns all documents with scores
```

**Document Retrieval**:
```cql
SELECT * FROM document_stats WHERE doc_id = 'doc1'
-- O(1) - direct lookup
```

**Collection Stats**:
```cql
SELECT * FROM collection_stats
-- O(1) - small table
```

### Scaling

| Metric | Benchmark |
|--------|-----------|
| Vocabulary Load | ~1000 terms/second |
| Inverted Index Load | ~10000 postings/second |
| Document Stats | ~5000 docs/second |
| Single Term Search | <50ms |

## Integration with Search

### Python Search Integration

```python
from cassandra.cluster import Cluster
import math

def bm25_search(query_terms, cassandra_host="localhost"):
    cluster = Cluster(contact_points=[cassandra_host])
    session = cluster.connect("search_index")
    
    # Get collection stats
    coll_stats = {}
    for row in session.execute("SELECT * FROM collection_stats"):
        coll_stats[row.stat_name] = row.stat_value
    
    total_docs = int(coll_stats.get("total_docs", 1))
    
    # Score documents
    doc_scores = {}
    
    for term in query_terms:
        # Get postings for term
        rows = session.execute(
            "SELECT doc_id, term_freq, tfidf FROM inverted_index WHERE term = %s",
            [term]
        )
        
        for row in rows:
            if row.doc_id not in doc_scores:
                doc_scores[row.doc_id] = 0
            doc_scores[row.doc_id] += row.tfidf
    
    # Rank results
    ranked = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    
    session.shutdown()
    cluster.shutdown()
    
    return ranked[:10]

# Usage
results = bm25_search(['machine', 'learning'])
for doc_id, score in results:
    print(f"{doc_id}: {score:.2f}")
```

## Cassandra/ScyllaDB Setup

### Installing Cassandra

```bash
# Using Docker (recommended)
docker run -d --name cassandra -p 9042:9042 cassandra:latest

# Or install locally
apt-get install cassandra
service cassandra start
```

### Installing ScyllaDB

```bash
# Using Docker
docker run -d --name scylla -p 9042:9042 scylladb/scylla

# All similar - uses same CQL protocol
```

### Verifying Installation

```bash
# Connect with cqlsh
cqlsh localhost 9042

# Check status
cassandra-cli --host localhost --port 9042
```

## Spark ScyllaDB Connector

### Configuration

The Spark ScyllaDB Connector is automatically configured in `store_index.sh`:

```bash
spark-submit \
    --packages com.scylladb:spark-scylladb-connector_2.12:0.2.1 \
    load_index_to_cassandra.py
```

### Key Settings

```python
.config("spark.cassandra.connection.host", "localhost")
.config("spark.cassandra.connection.port", "9042")
.config("spark.cassandra.output.batch.size.rows", "10000")
```

### Batch Size Tuning

```bash
# In store_index.sh or load script, adjust:
--conf spark.cassandra.output.batch.size.rows=50000  # For large batches
--conf spark.cassandra.output.batch.size.rows=1000   # For small, frequent inserts
```

## Schema Optimization

### Indexes

For better query performance, may add secondary indexes:

```cql
-- Secondary index on term for vocabulary lookup by term
CREATE INDEX IF NOT EXISTS ON vocabulary (term);

-- Already optimal - term is partition key in inverted_index
-- Already optimal - doc_id is primary key in document_stats
```

### Compression

Enable compression to reduce storage:

```cql
CREATE TABLE inverted_index (
    term text,
    doc_id text,
    term_freq int,
    tfidf float,
    PRIMARY KEY (term, doc_id)
) WITH compression = {'class': 'LZ4Compressor'}
```

## Troubleshooting

### Connection Issues

```bash
# Test connectivity
cqlsh cassandra_host cassandra_port

# Check Cassandra is running
nodetool status

# Check Spark configuration
spark-shell --conf spark.cassandra.connection.host=your_host
```

### Loading Failures

```bash
# Check Spark logs
cat /path/to/spark/logs/spark-*.log

# Check HDFS index exists
hadoop fs -ls /indexer/final_index

# Test read permissions
hadoop fs -cat /indexer/final_index/part-r-00000 | head
```

### Data Integrity

```cql
-- Verify vocabulary
SELECT COUNT(*) FROM vocabulary;
SELECT * FROM vocabulary LIMIT 10;

-- Verify inverted index
SELECT COUNT(*) FROM inverted_index;
SELECT * FROM inverted_index LIMIT 10;

-- Verify document stats
SELECT COUNT(*) FROM document_stats;

-- Verify collection stats
SELECT * FROM collection_stats;
```

## Performance Tips

1. **Use SSD storage** for Cassandra - critical for write performance
2. **Tune Cassandra heap** - typically 8-16GB for 1TB+ datasets
3. **Multiple nodes** - use 3+ node cluster for production
4. **Batch size** - increase for large datasets, decrease for memory constraints
5. **Replication factor** - use 3+ for production, 1 for dev
6. **Compression** - LZ4 is fast, DEFLATE is smaller

## Next Steps

After loading the index:

1. **Search Integration**: Use Python/Java clients to query
2. **REST API**: Wrap with HTTP service for web apps
3. **Caching**: Add Redis layer for frequent queries
4. **Monitoring**: Track query performance and system metrics
5. **Backups**: Regular backups of Cassandra data

## Common Queries

### Find term frequency in document
```cql
SELECT term_freq, tfidf FROM inverted_index 
WHERE term = 'machine' AND doc_id = 'doc1';
```

### Find all terms in document
```cql
SELECT term, term_freq, tfidf FROM inverted_index 
WHERE doc_id = 'doc1' ALLOW FILTERING;
```

### Most frequent terms
```cql
SELECT term, COUNT(*) as doc_count FROM inverted_index 
GROUP BY term LIMIT 10;
```

### Document similarity
```cql
SELECT * FROM inverted_index WHERE term IN ('machine', 'learning')
-- Join manually in application to find docs with both terms
```

---

**See**: [store_index.sh](store_index.sh), [load_index_to_cassandra.py](load_index_to_cassandra.py)
