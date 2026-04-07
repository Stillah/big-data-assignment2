#!/usr/bin/env python3
import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum as spark_sum, when, lit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BM25Searcher:
    """
    BM25 search engine for document retrieval from Cassandra.
    
    Uses the Spark ScyllaDB Connector to read index data from Cassandra tables.
    """
    
    def __init__(self, spark, cassandra_host="localhost", cassandra_port="9042", keyspace="search_index"):
        self.spark = spark
        self.cassandra_host = cassandra_host
        self.cassandra_port = cassandra_port
        self.keyspace = keyspace
        
        # Configure Spark Cassandra connection
        self.spark.conf.set("spark.cassandra.connection.host", cassandra_host)
        self.spark.conf.set("spark.cassandra.connection.port", cassandra_port)
        
        logger.info(f"Initialized BM25Searcher for {keyspace} at {cassandra_host}:{cassandra_port}")
    
    def read_cassandra_table(self, table_name):
        logger.info(f"Reading {table_name}")
        
        df = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.keyspace) \
            .option("table", table_name) \
            .load()
        
        return df
    
    def load_vocabulary_df(self):
        vocab_df = self.read_cassandra_table("vocabulary")
        # Cache and broadcast
        vocab_df = vocab_df.cache()
        logger.info(f"Loaded {vocab_df.count()} terms")
        return vocab_df
    
    def load_document_stats(self):
        logger.info("Loading document statistics")
        
        stats_df = self.read_cassandra_table("document_stats")
        logger.info(f"Loaded statistics")
        
        return stats_df
    
    def load_collection_stats(self):
        logger.info("Loading collection statistics")
        
        stats_df = self.read_cassandra_table("collection_stats")
        
        # Convert to dict
        rows = stats_df.select("stat_name", "stat_value").collect()
        collection_stats = {row.stat_name: row.stat_value for row in rows}
        
        logger.info(f"Loaded {len(collection_stats)} collection statistics")
        
        # Broadcast to all executors
        return self.spark.sparkContext.broadcast(collection_stats)
    
   
    def calculate_bm25(self, query_terms, vocab_df, inverted_index_df,
                        doc_stats_df, collection_stats_bcast):      
        collection_stats = collection_stats_bcast.value
        avgdl = float(collection_stats.get("avgdl", "100"))
        
        # Filter vocabulary to query terms
        query_terms_df = self.spark.createDataFrame([(t,) for t in query_terms], ["term"])
        logger.info("First join started")
        vocab_filtered = vocab_df.join(broadcast(query_terms_df), "term")
        
        # Join inverted index with filtered vocabulary
        logger.info("Second join started")
        filtered_postings = inverted_index_df.join(vocab_filtered, "term")
        
        # Join with document stats
        logger.info("Third join started")
        doc_index = filtered_postings.join(
            broadcast(doc_stats_df),
            filtered_postings.doc_id == doc_stats_df.doc_id,
            "left"
        ).select(
            filtered_postings.doc_id,
            filtered_postings.term,
            filtered_postings.term_freq,
            vocab_filtered["idf"],
            doc_stats_df["token_count"].alias("doc_length")
        ).fillna(avgdl, ["doc_length"])
        
        # BM25 parameters
        K1 = lit(1.5)
        B = lit(0.75)
        avgdl_lit = lit(avgdl)
        
        logger.info("Doc_index.withColumn started")
        doc_index = doc_index.withColumn(
            "bm25_component",
            col("idf") * (
                (col("term_freq") * (K1 + 1)) /
                (col("term_freq") + K1 * (1 - B + B * (col("doc_length") / avgdl_lit)))
            )
        )
        
        logger.info("groupBy started")
        scored_docs = doc_index.groupBy("doc_id").agg(
            spark_sum("bm25_component").alias("bm25_score")
        )
        
        return scored_docs
    
    
    def search(self, query_text, top_k=10):
        """
        Search for documents matching the query using BM25
        
        query_text: User query as string
        top_k: Number of top results to return (default: 10)
            
        Returns list of (doc_id, bm25_score) tuples sorted by score descending
        """
        logger.info(f"Searching for query: {query_text}")
        
        # lowercase query and split it into terms
        query_terms = set(query_text.lower().split())
        query_terms = [t.strip() for t in query_terms if len(t.strip()) > 0]
        
        logger.info(f"Query terms: {query_terms}")
        
        if not query_terms:
            logger.warning("No valid query terms")
            return []
               
 
        vocab_df = self.load_vocabulary_df()
        # vocab_df = vocab_df.coalesce(1)

        inverted_index_df = self.read_cassandra_table("inverted_index")
        # inverted_index_df = inverted_index_df.coalesce(1)

        doc_stats_df = self.load_document_stats()
        # doc_stats_df = doc_stats_df.coalesce(1)

        collection_stats_bcast = self.load_collection_stats()

        
        # Filter query terms using DatFrame
        existing_terms_df = vocab_df.filter(col("term").isin(query_terms))
        existing_query_terms = [row.term for row in existing_terms_df.select("term").collect()]
        if not existing_query_terms:
            logger.warning("No query terms found in vocabulary")
            return []
        
        logger.info("Starting to calculate BM25")

        bm25_scores_df = self.calculate_bm25(
            existing_query_terms, vocab_df, inverted_index_df,
            doc_stats_df, collection_stats_bcast
        )
        
        logger.info("Calculated BM25!")

        # Get top-k results sorted by score
        top_results_df = bm25_scores_df \
            .orderBy(col("bm25_score").desc()) \
            .limit(top_k)
        
        # Collect results while Cassandra connection is active
        try:
            top_results = [(row.doc_id, float(row.bm25_score)) for row in top_results_df.collect()]
            logger.info(f"Collected {len(top_results)} results")
        except Exception as e:
            logger.error(f"Failed to collect results: {e}", exc_info=True) # Might time out here
            top_results = []
        
        return top_results


def main():   
    # Parse arguments
    parser = argparse.ArgumentParser(description="Search indexed documents using BM25.")
    parser.add_argument(
        "--query",
        required=True,
        help="Search query (required)"
    )
    parser.add_argument(
        "--cassandra-host",
        default="scylla-master",
        help="ScyllaDB host (default: scylla-master)"
    )
    parser.add_argument(
        "--cassandra-port",
        default="9042",
        help="ScyllaDB port (default: 9042)"
    )
    parser.add_argument(
        "--keyspace",
        default="search_index",
        help="Cassandra keyspace (default: search_index)"
    )
    
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("DocumentSearch-BM25") \
        .config("spark.cassandra.connection.host", args.cassandra_host) \
        .config("spark.cassandra.connection.port", args.cassandra_port) \
        .getOrCreate()
    
    try:
        searcher = BM25Searcher(
            spark,
            cassandra_host=args.cassandra_host,
            cassandra_port=args.cassandra_port,
            keyspace=args.keyspace
        )
        
        # Search
        query = args.query
        results = searcher.search(query, top_k=1)
        
        # Display results
        print("\n" + "=" * 75)
        print(f"Search Results for: '{query}'")
        print("=" * 75)
        
        if results:
            print(f"\n{len(results)} most relevant document(s):\n")
            for rank, (doc_id, score) in enumerate(results, 1):
                print(f"{rank:2d}. {doc_id}")
                print(f"    Score: {score:.4f}")
                print()
        else:
            print("\nNo documents found for this query.\n")
        
        print("=" * 75)
                
        return 0
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"\nError: {e}\n")
        return 1
    
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())