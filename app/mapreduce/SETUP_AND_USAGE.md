# MapReduce Indexing System - Setup & Usage Guide

## Overview

This is a production-ready Hadoop MapReduce pipeline system for building a searchable document index with BM25 scoring capability. The system is designed to handle large-scale document collections efficiently using distributed computing.

## Quick Start

### 1. Prerequisites
- Hadoop cluster (YARN + HDFS)
- Python 3.6+ on all Hadoop nodes
- Documents stored in or copied to `/data` in HDFS

### 2. Basic Usage

#### Using Python Orchestrator (Recommended)
```bash
cd /path/to/app/mapreduce

# Run with default settings
python3 orchestrator.py

# Run with custom paths
python3 orchestrator.py \
    --input /hdfs/input/data \
    --temp /hdfs/temp/indexing \
    --output /hdfs/index \
    --reducers 10
```

#### Using Shell Script
```bash
cd /path/to/app/mapreduce
bash run_pipeline.sh
```

### 3. Check Results
```bash
# View the index
hadoop fs -cat /indexer/final_index/part-r-00000 | head -20

# Count indexed terms
hadoop fs -cat /indexer/final_index/part-r-* | grep -v "^__" | wc -l

# View document statistics
hadoop fs -cat /indexer/final_index/part-r-* | grep "^__DOC_STATS__"
```

## System Architecture

### Two-Stage Pipeline

#### Pipeline 1: Tokenization and Basic Indexing
```
Input Documents → Mapper 1 → Shuffle & Sort → Reducer 1 → Intermediate Index
```

- **Mapper 1**: 
  - Tokenizes documents (lowercase, removes punctuation/URLs)
  - Counts term frequencies per document
  - Output: `term\tdoc_id:count`

- **Reducer 1**:
  - Aggregates counts per term across documents
  - Produces inverted index structure
  - Output: `term\tdoc_id:count,doc_id:count,...`

#### Pipeline 2: Statistics and Finalization
```
Intermediate Index → Mapper 2 → Shuffle & Sort → Reducer 2 → Final Index
```

- **Mapper 2**:
  - Calculates document frequency per term
  - Output: `term\tDOC_FREQ:df,POSTINGS:list`

- **Reducer 2**:
  - Computes IDF: `log(N / df) + 1`
  - Calculates TF-IDF scores
  - Aggregates collection statistics
  - Outputs complete index with all BM25 prerequisites

## Output Format

### Final Index File Structure
```
/indexer/
├── final_index/
│   ├── part-r-00000         # Main index output (split across multiple files)
│   ├── part-r-00001
│   └── ...
```

### Index Entry Format
```
term\tdoc_freq,idf,doc_id:term_freq:tfidf,doc_id:term_freq:tfidf,...

Example line:
machine\t5,2.0794,doc1:3:6.2382,doc2:1:2.0794,doc3:2:4.1588
```

Where:
- `machine` = indexed term
- `5` = document frequency (5 documents contain "machine")
- `2.0794` = IDF score
- `doc1:3:6.2382` = doc1 contains "machine" 3 times with TF-IDF score 6.2382

### Metadata Format

#### Document Statistics
```
__DOC_STATS__\tdoc_id,unique_terms,token_count,avg_term_freq

Example:
__DOC_STATS__\tdoc_1,245,1523,6.22
```

#### Collection Statistics
```
__COLLECTION_STATS__\ttotal_docs:N

Example:
__COLLECTION_STATS__\ttotal_docs:100
```

#### Vocabulary
```
__VOCABULARY__\tindex,term

Example:
__VOCABULARY__\t0,machine
__VOCABULARY__\t1,learning
```

## Data Requirements

### Input Format
- **Location**: HDFS `/data` directory (configurable)
- **Format**: Plain text UTF-8 files
- **Filename to Doc ID**: Filename (without .txt) becomes the document ID
- **One file per document** (typical arrangement)

### Input Preparation
```bash
# Copy local documents to HDFS
hadoop fs -mkdir -p /data
hadoop fs -put /local/path/to/documents/* /data/

# Verify
hadoop fs -ls /data | head
hadoop fs -cat /data/sample_doc.txt
```

## Configuration

### Command-Line Options
```bash
python3 orchestrator.py --help

Options:
  --input PATH          HDFS input directory (default: /data)
  --temp PATH           HDFS temp storage (default: /tmp/indexer)
  --output PATH         HDFS output directory (default: /indexer)
  --reducers N          Number of reducers (default: 1)
  --script-dir PATH     Directory with mapper/reducer scripts
```

### Performance Tuning

#### Number of Reducers
```bash
# Use more reducers for large datasets (80+ documents)
python3 orchestrator.py --reducers 10

# Recommendation:
# - 1-10 documents: 1 reducer
# - 10-100 documents: 2-5 reducers
# - 100+ documents: 5-20 reducers
```

#### HDFS Block Size
Configure in `/etc/hadoop/conf/hdfs-site.xml`:
```xml
<property>
  <name>dfs.blocksize</name>
  <value>134217728</value>  <!-- 128 MB -->
</property>
```

### Memory Configuration
If jobs fail due to memory, adjust in `core-site.xml`:
```bash
export HADOOP_HEAPSIZE=2048
```

## Using the Index

### Python Interface

#### Import and Initialize
```python
from app.mapreduce.index_reader import IndexReader

reader = IndexReader(index_path='/indexer/final_index')
```

#### Simple TF-IDF Search
```python
# Search for documents
results = reader.search(['machine', 'learning'], top_k=10)

for doc_id, score in results:
    print(f"{doc_id}: {score:.4f}")
```

#### BM25 Scoring (Recommended)
```python
# BM25 is more advanced but slower
results = reader.calculate_bm25(
    query_terms=['machine', 'learning'],
    k1=1.5,           # Term frequency saturation
    b=0.75,           # Length normalization
    top_k=10
)

for doc_id, bm25_score in results:
    print(f"{doc_id}: {bm25_score:.4f}")
```

#### Get Posting List for a Term
```python
# Get all documents containing a term
postings = reader.get_postings('machine')

for doc_id, term_freq, tfidf in postings:
    print(f"{doc_id}: freq={term_freq}, tfidf={tfidf:.4f}")
```

#### Access Statistics
```python
# Document statistics
doc_stats = reader.read_document_stats_from_hdfs()
for doc_id, (unique_terms, token_count, avg_freq) in doc_stats.items():
    print(f"{doc_id}: {unique_terms} unique terms, {token_count} total tokens")

# Collection statistics
collection = reader.read_collection_stats_from_hdfs()
print(f"Total documents: {collection['total_docs']}")

# Vocabulary
vocab = reader.read_vocabulary_from_hdfs()
print(f"Vocabulary size: {len(vocab)}")
```

### Direct HDFS Queries

#### View Specific Term
```bash
hadoop fs -cat /indexer/final_index/part-r-* | grep "^machine\t"
```

#### Count Unique Terms
```bash
hadoop fs -cat /indexer/final_index/part-r-* | grep -v "^__" | cut -f1 | sort -u | wc -l
```

#### Find Documents with High TF-IDF
```bash
# Extract and sort by TF-IDF score
hadoop fs -cat /indexer/final_index/part-r-* \
    | grep -v "^__" \
    | awk '{print $2}' \
    | tr ',' '\n' \
    | sort -t: -k3 -nr \
    | head -20
```

## Tokenization Details

### What Gets Indexed
- ✓ Alphanumeric characters (a-z, A-Z, 0-9)
- ✓ All words converted to lowercase
- ✗ Punctuation removed
- ✗ URLs removed
- ✗ Email addresses removed
- ✗ Single-character tokens skipped

### Examples
```
Input:  "Machine Learning (ML) is cool! Email: test@example.com"
Tokens: machine, learning, ml, is, cool
```

### Token Length
- Minimum: 1 character
- Maximum: unlimited
- Filtered: empty tokens

## BM25 Scoring Formula

The system supports BM25 ranking, which is more accurate than simple TF-IDF:

```
score(doc, query) = Σ IDF(qi) * (f(qi, doc) * (k1 + 1)) / (f(qi, doc) + k1 * (1 - b + b * |doc| / avgdl))
```

Where:
- `IDF(qi)` = log(N / df_i) + 1 (from index)
- `f(qi, doc)` = term frequency (from index)
- `|doc|` = document length in tokens (from doc_stats)
- `avgdl` = average document length (calculated from statistics)
- `k1` = parameter controlling term frequency saturation (default 1.5)
- `b` = parameter controlling length normalization (default 0.75)

### Why BM25?
- Accounts for document length (doesn't favor long documents)
- Diminishing returns for term frequency (doesn't over-weight repetition)
- Better relevance ranking than TF-IDF
- Standard in production search systems

## Troubleshooting

### Jobs Hang or Timeout
```bash
# Increase timeout
# In Hadoop configuration:
# mapred-site.xml: mapreduce.task.timeout = 600000ms
```

### Python Module Not Found
```bash
# Ensure Python is available on worker nodes
hadoop fs -put /path/to/python3 /hadoop/bin/

# Or use system Python:
which python3
```

### Insufficient Memory
```bash
# Increase mapper/reducer memory in mapred-site.xml:
# mapreduce.map.memory.mb = 2048
# mapreduce.reduce.memory.mb = 2048

# Or set Java heap:
export HADOOP_HEAPSIZE=4096
```

### Index File Too Large
```bash
# Use more reducers to split output
python3 orchestrator.py --reducers 20

# This creates multiple part-r-* files
```

### Slow Performance
1. Increase number of reducers
2. Increase HDFS block size
3. Check network bandwidth
4. Verify no Hadoop jobs running simultaneously
5. Check disk I/O on cluster nodes

## Monitoring

### Check Job Status
```bash
# In another terminal during execution:
hadoop application -list -appStates RUNNING
```

### View Job Logs
```bash
# After completion:
hadoop logs <application_id>
```

### Monitor HDFS Usage
```bash
hadoop fs -du -s /indexer
hadoop fs -du -s /tmp/indexer
```

## Advanced Topics

### Custom Tokenization
Edit `mapper1.py` function `tokenize()`:
```python
def tokenize(text):
    # Customize tokenization logic here
    # Default: lowercase, remove punctuation/URLs
    ...
```

### Scaling to Large Datasets

For 1M+ documents:
1. **Increase reducers**: `--reducers 100`
2. **Increase memory**: Edit mapred-site.xml
3. **Use compression**: Add to Hadoop config:
   ```bash
   -D mapred.compress.map.output=true
   -D mapred.output.compression.type=BLOCK
   ```
4. **Consider secondary sorting** for better performance

### Integration with Other Tools

The index can be imported to:
- **Elasticsearch**: Parse HDFS output and bulk index
- **Solr**: Convert to Solr XML format
- **Custom search engine**: Use `index_reader.py` module

### Incremental Indexing

Current system performs full reindexing. For incremental updates:
1. Run pipeline only on new documents
2. Merge with existing index
3. Recalculate IDF across merged index

This requires custom merge logic (not in base system).

## Examples

### Example 1: Index Blog Posts
```bash
# 1. Copy blog posts to HDFS
hadoop fs -put ~/blog_posts/* /data/

# 2. Run indexing
python3 orchestrator.py

# 3. Search
python3 << 'EOF'
from app.mapreduce.index_reader import IndexReader
reader = IndexReader()
results = reader.calculate_bm25(['python', 'programming'])
for doc, score in results:
    print(f"{doc}: {score:.2f}")
EOF
```

### Example 2: Large Dataset (1000 documents)
```bash
# Prepare data
hadoop fs -mkdir -p /large_dataset
hadoop fs -put /data/docs/* /large_dataset/

# Run with 20 reducers
python3 orchestrator.py \
    --input /large_dataset \
    --reducers 20 \
    --output /hdfs/large_index
```

### Example 3: Custom Index Location
```bash
python3 orchestrator.py \
    --input /company/documents \
    --temp /company/indexing_temp \
    --output /company/search_index \
    --reducers 5
```

## Files Generated

### Primary Files
- `/indexer/final_index/part-r-*` - Final inverted index with IDF and TF-IDF scores

### Temporary Files (can be deleted after indexing)
- `/tmp/indexer/pipeline1/part-r-*` - Intermediate index from Pipeline 1

### Recommended Cleanup
```bash
# Keep final_index/, delete temporaries
hadoop fs -rm -r /tmp/indexer

# Archive for backup
hadoop fs -cp /indexer /backup/indexer_YYYY-MM-DD
```

## References

- [Hadoop Streaming](https://hadoop.apache.org/docs/current/hadoop-streaming.html)
- [BM25 Algorithm](https://en.wikipedia.org/wiki/Okapi_BM25)
- [Indexing Theory](https://nlp.stanford.edu/IR-book/information-retrieval-book.html)

## Support

For issues or questions:
1. Check logs: `hadoop logs <application_id>`
2. Review this guide's troubleshooting section
3. Verify HDFS and Hadoop are running: `hadoop daemonsstatus`
