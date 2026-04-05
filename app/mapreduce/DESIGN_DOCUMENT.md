# MapReduce Indexing System - Design Documentation

## Executive Summary

This document describes the technical design of a two-stage Hadoop MapReduce pipeline system for building a searchable document index with BM25 scoring support. The system processes large document collections in a distributed manner to produce an inverted index suitable for information retrieval applications.

## System Overview

### Purpose
Build a production-grade document index from large text collections using Hadoop MapReduce, enabling efficient full-text search with BM25 ranking.

### Key Characteristics
- **Distributed Processing**: Scales horizontally with Hadoop cluster size
- **Two-Stage Pipeline**: Optimized two-pass algorithm for index construction
- **BM25-Ready**: Outputs all statistics needed for advanced ranking algorithms
- **Streaming-Based**: Minimal memory overhead using Hadoop Streaming
- **Extensible**: Modular design allows customization of tokenization and processing

## Design Goals

1. **Scalability**: Support indexing of millions of documents
2. **Efficiency**: Minimize CPU, memory, and I/O overhead
3. **Correctness**: Accurate term extraction and frequency calculation
4. **Usability**: Simple API for running and querying the index
5. **Maintainability**: Clear code structure and documentation

## Architecture

### High-Level Flow

```
Raw Documents (HDFS /data)
    ↓
    ├─→ [Pipeline 1] ─→ Intermediate Index (/tmp/indexer/pipeline1)
    │   ├─ Mapper 1: Tokenization & Term Extraction
    │   └─ Reducer 1: Index Aggregation
    ↓
    ├─→ [Pipeline 2] ─→ Final Index (/indexer/final_index)
    │   ├─ Mapper 2: Statistics Calculation
    │   └─ Reducer 2: IDF & Score Finalization
    ↓
    └─→ Output Index (ready for search)
```

### Pipeline 1: Tokenization and Index Construction

#### Mapper 1 Responsibilities
1. **Document Processing**
   - Reads input documents line by line
   - Extracts document ID from filename (via HDFS environment)

2. **Tokenization**
   - Converts text to lowercase
   - Removes URLs and email addresses
   - Removes punctuation and special characters
   - Splits on whitespace

3. **Term Frequency Counting**
   - Counts occurrences of each token
   - Tracks document length (total tokens)
   - Counts unique terms per document

4. **Output Emission**
   ```
   Key: term (string)
   Value: doc_id:count (string)
   
   Also emits metadata:
   Key: __DOC_STAT__:doc_id
   Value: doc_id,unique_terms,token_count
   ```

**Processing Example**:
```
Input Document:
"Machine Learning is important. Machine Learning is useful!"

After tokenization:
[machine, learning, is, important, machine, learning, is, useful]

Mapper output:
machine         doc1:2
learning        doc1:2
is              doc1:2
important       doc1:1
useful          doc1:1
__DOC_STAT__:doc1  doc1,5,8
```

#### Reducer 1 Responsibilities
1. **Aggregation by Term**
   - Groups mapper outputs by term (key)
   - Receives all (term, doc_id:count) pairs for a term

2. **Inverted Index Construction**
   - Combines counts for same (term, doc_id) across multiple mappers
   - Creates posting list: term → [(doc_id, freq), ...]

3. **Output Generation**
   ```
   Key: term (string)
   Value: doc_id:count,doc_id:count,... (posting list)
   
   Metadata output:
   Key: __DOC_STATS__
   Value: doc_id,unique_terms,token_count
   
   Vocabulary list:
   Key: __VOCABULARY__
   Value: term
   ```

**Reducer Example**:
```
Input (for term "machine"):
machine         doc1:2
machine         doc1:1
machine         doc2:3
machine         doc3:1

Reducer output:
machine         doc1:3,doc2:3,doc3:1
```

### Pipeline 2: Statistics and Finalization

#### Mapper 2 Responsibilities
1. **Index Entry Processing**
   - Reads inverted index from Pipeline 1
   - Parses posting lists

2. **Document Frequency Calculation**
   - Counts documents for each term
   - `df(term) = number of documents containing term`

3. **Output Generation**
   ```
   For index entries:
   Key: term
   Value: DOC_FREQ:df,POSTINGS:doc:freq,doc:freq,...
   
   Pass-through of metadata
   ```

#### Reducer 2 Responsibilities
1. **IDF Calculation**
   ```
   IDF(term) = log(N / df) + 1
   where:
     N = total number of documents
     df = document frequency
     +1 = smoothing factor
   ```

2. **TF-IDF Score Computation**
   ```
   TF-IDF(term, doc) = TF(term, doc) × IDF(term)
   where:
     TF = term frequency from index
     IDF = calculated above
   ```

3. **Statistics Aggregation**
   - Collects all document statistics
   - Calculates collection-wide statistics
   - Builds vocabulary index

4. **Final Output**
   ```
   Index entries with scores:
   Key: term
   Value: df,idf,doc:freq:tfidf,doc:freq:tfidf,...
   
   Document statistics:
   __DOC_STATS__    doc_id,unique_terms,token_count,avg_term_freq
   
   Collection statistics:
   __COLLECTION_STATS__  total_docs:N
   
   Vocabulary:
   __VOCABULARY__   index,term
   ```

**Reducer 2 Example**:
```
Input (for term "machine"):
machine         DOC_FREQ:3,POSTINGS:doc1:3,doc2:3,doc3:1

Processing:
- df = 3
- N = 100 (total documents)
- IDF = log(100/3) + 1 = 3.51 + 1 = 4.51
- TF-IDF(machine, doc1) = 3 × 4.51 = 13.53
- TF-IDF(machine, doc2) = 3 × 4.51 = 13.53
- TF-IDF(machine, doc3) = 1 × 4.51 = 4.51

Output:
machine         3,4.51,doc1:3:13.5300,doc2:3:13.5300,doc3:1:4.5100
```

## Data Format Specifications

### Input Format
**Type**: Plain text UTF-8
**Source**: HDFS directory `/data`
**Organization**: One file per document
**Naming**: Filename (without extension) becomes document ID

### Intermediate Format (Pipeline 1 Output)

**Text Format** (Hadoop Streaming):
```
term\tdoc_id:count[,doc_id:count]*
__DOC_STAT__:doc_id\tdoc_id,unique_terms,token_count
__VOCABULARY__\tterm
```

**Compression**: Optional (HDFS configurable)
**Partitioning**: By term hash (multiple reducers create multiple part-r-* files)

### Final Format (Pipeline 2 Output)

**Index Entries**:
```
term\tdoc_freq,idf_score,doc:freq:tfidf[,doc:freq:tfidf]*
```

**Statistics**:
```
__DOC_STATS__\tdoc_id,unique_terms,token_count,avg_term_freq
__COLLECTION_STATS__\ttotal_docs:N
__VOCABULARY__\tindex,term
```

**Example**:
```
algorithm\t8,2.223,doc5:2:4.446,doc12:1:2.223,doc34:3:6.669
__DOC_STATS__\tdoc5,1250,8765,7.01
__COLLECTION_STATS__\ttotal_docs:100
__VOCABULARY__\t42,algorithm
```

## Key Design Decisions

### 1. Two-Stage Pipeline vs. Single Stage
**Decision**: Two-stage pipeline
**Rationale**:
- Pipeline 1 focuses on index construction (map-reduce pair)
- Pipeline 2 handles statistical calculations and finalization
- Separation of concerns improves maintainability
- Allows for interchangeable output at Pipeline 1 if needed
- Enables optimization of each stage independently

### 2. Streaming-Based Approach
**Decision**: Use Hadoop Streaming with Python mappers/reducers
**Rationale**:
- Language-agnostic (Python available on most systems)
- Minimal overhead compared to Java
- Simple debugging and testing
- Natural map-reduce expression in Python
- Suitable for text processing tasks

**Trade-off**: Slightly slower than native Java, but sufficient for indexing workloads

### 3. Tokenization Strategy
**Decision**: Simple whitespace-based with character filtering
**Rationale**:
- Sufficient for general text retrieval
- Removes noise (punctuation, URLs, emails)
- Lowercase conversion handles case-insensitivity
- Easily customizable for domain-specific needs

**Alternative considered**: Sophisticated NLP tokenization (would require additional libraries)

### 4. TF-IDF + BM25 Support
**Decision**: Output both TF-IDF scores AND all statistics needed for BM25
**Rationale**:
- TF-IDF provides reasonable baseline
- BM25 available but more compute-intensive at query time
- System remains flexible for ranking algorithm selection
- Supports both online and offline scoring

### 5. Single Reducer Default
**Decision**: Default to 1 reducer, configurable for scaling
**Rationale**:
- Simplest setup for small-to-medium datasets
- Reduces network overhead for small datasets
- Easy to scale up for larger collections
- Users can tune based on data size

### 6. Metadata Encoding
**Decision**: Embed metadata (vocabulary, statistics) in output alongside index
**Rationale**:
- Single-pass reading of output gets everything needed
- No separate coordination needed
- Easier deployment and file management
- Metadata prefixed with `__` for easy filtering

## Performance Characteristics

### Time Complexity
- **Mapping**: O(D × T) where D = documents, T = avg tokens per doc
- **Shuffling**: O(D × V × log(V)) where V = unique vocabulary
- **Reducing**: O(D × V) for aggregation
- **Overall**: O(D × V × log(V)) due to shuffle/sort phase

### Space Complexity
- **Intermediate**: O(D × V) for posting lists
- **Final**: O(V + D × log(N)) where N = total docs
  - V for vocabulary
  - D × log(N) for IDF scores and TF-IDF per posting

### Scaling
- **Linear with documents**: Doubling documents doubles processing time
- **Sublinear with reducers**: More reducers improve throughput (to a point)
- **Network-bound**: Large clusters may be limited by network bandwidth

### Example Benchmarks
```
100 documents (~500KB):   ~2 minutes (1 reducer)
1,000 documents (~5MB):   ~5 minutes (2 reducers)
10,000 documents (~50MB): ~15 minutes (5 reducers)
100,000 documents (~500MB): ~90 minutes (20 reducers)

(Estimates on moderate cluster, actual times vary by hardware)
```

## Fault Tolerance

### Hadoop Native Features
- **Node Failure**: YARN automatically reruns failed tasks
- **Data Loss**: HDFS replication (default 3) protects against disk failures
- **Task Failure**: Configurable retries (default 4)

### Application-Level Considerations
1. **Idempotency**: Tasks can be safely retried
   - Mappers are stateless
   - Reducers produce deterministic output

2. **Partial Output**: If job fails, can restart
   - Must clean up `/tmp/indexer` manually before restart
   - Final output directory should be empty before Pipeline 2

3. **Monitoring**: Check logs for failures
   ```bash
   hadoop logs <application_id>
   ```

## Security Considerations

### Input Data
- Documents in `/data` are readable by cluster
- Consider encryption at rest if sensitive data

### Output Index
- Index stored in HDFS with cluster permissions
- Consider restricting read access to `/indexer` via FS permissions

### Processing
- No data transmitted outside cluster
- Python scripts execute on cluster nodes only

## Extension Points

### Custom Tokenization
Modify `mapper1.py` `tokenize()` function:
```python
def tokenize(text):
    # Add domain-specific tokenization logic
    # E.g., handle camelCase, abbreviations, entities
    ...
```

### Stemming/Lemmatization
Add to tokenization:
```python
from nltk.stem import PorterStemmer
stemmer = PorterStemmer()
tokens = [stemmer.stem(t) for t in tokens]
```

### Stop Word Filtering
Filter common words:
```python
STOP_WORDS = {'the', 'a', 'is', 'and', ...}
tokens = [t for t in tokens if t not in STOP_WORDS]
```

### Custom Scoring
Modify Reducer 2 instead of TF-IDF:
```python
# Custom scoring formula
score = custom_formula(tf, df, doc_length)
```

## Comparison with Alternatives

### vs. Elasticsearch/Solr
- **Indexing**: MapReduce is distributed, scales to massive datasets
- **Query**: Elasticsearch is optimized, MapReduce requires custom query layer
- **Use Case**: MapReduce for batch indexing, Elasticsearch for interactive search

### vs. Spark RDD
- **Performance**: Spark typically faster, but higher memory footprint
- **Ecosystem**: MapReduce is stable standard, Spark is newer
- **Complexity**: MapReduce simpler for simple tasks, Spark better for complex pipelines

### vs. Single-Machine Indexing
- **Scalability**: MapReduce handles unlimited scale, single-machine limited by hardware
- **Fault Tolerance**: MapReduce has built-in recovery, single-machine does not
- **Deployment**: Single-machine simpler, MapReduce requires cluster

## Future Improvements

1. **Incremental Indexing**: Support updating index with new documents
2. **Distributed Query**: Implement distributed query processing
3. **Advanced Tokenization**: Add stemming, POS tagging, entity recognition
4. **Compression**: Implement index compression for storage efficiency
5. **Caching**: Add distributed caching for frequently accessed terms
6. **Ranking Variants**: Support learning-to-rank models

## References

### Algorithms
- [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25)
- [Inverted Index](https://en.wikipedia.org/wiki/Inverted_index)
- [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)

### Systems
- [Hadoop MapReduce Architecture](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Hadoop Streaming Guide](https://hadoop.apache.org/docs/stable/hadoop-streaming/)

### References
- [Information Retrieval Book](https://nlp.stanford.edu/IR-book/)
- [Lucene Indexing](https://lucene.apache.org/)

## Conclusion

This MapReduce-based indexing system provides a scalable, maintainable solution for building high-quality document indexes suitable for information retrieval applications. The two-stage pipeline design balances computational efficiency with output quality necessary for modern ranking algorithms like BM25.
