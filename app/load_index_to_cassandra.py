#!/usr/bin/env python3
import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, split, trim, explode, 
    row_number, array_join, size, slice
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IndexLoader:
    """Loads MapReduce index data into ScyllaDB"""
    
    def __init__(self, spark, index_path, keyspace, batch_size=10000):
        self.spark = spark
        self.index_path = index_path
        self.keyspace = keyspace
        self.batch_size = batch_size
        logger.info(f"Initialized IndexLoader for {index_path} -> {keyspace}")
    
    def read_index_data(self):
        logger.info(f"Reading index data from {self.index_path}")
        
        # Read text files as single column
        raw_df = self.spark.read.text(self.index_path)
        
        # Filter out empty lines
        raw_df = raw_df.filter(col("value") != "")
        
        count = raw_df.count()
        logger.info(f"Loaded {count} lines from index")
        
        return raw_df
    
    def parse_index_entries(self, raw_df):
        logger.info("Parsing index entries")
        
        # Filter index entries from metadata
        index_entries = raw_df.filter(~col("value").startswith("__"))
        
        # Split by tab
        split_df = index_entries.select(
            split(col("value"), "\t").alias("fields")
        )
        
        # Extract fields. fields[1] contains: doc_freq,idf,doc1:freq1:tfidf1,doc2:freq2:tfidf2,...
        parsed_df = split_df.select(
            trim(col("fields")[0]).alias("term"),
            trim(col("fields")[1]).alias("full_data")
        )
        
        # Split the full_data by comma to extract doc_freq, idf, and postings
        split_data = split(col("full_data"), ",")
        
        # Extract first two elements (doc_freq, idf) and rejoin the rest (postings)
        parsed_df = parsed_df.select(
            col("term"),
            trim(split_data[0]).cast(IntegerType()).alias("doc_frequency"),
            trim(split_data[1]).cast(FloatType()).alias("idf"),
            # Rejoin postings: all elements from index 2 onwards, joined back with commas
            array_join(slice(split_data, 3, size(split_data) - 2), ",").alias("postings")
        )
        
        # Create vocabulary
        window = Window.orderBy(col("term"))
        vocab_df = parsed_df.select(col("term"), col("idf"), col("doc_frequency")).distinct()
        vocab_df = vocab_df.withColumn(
            "term_id",
            row_number().over(window)
        ).select("term_id", "term", col("idf"), col("doc_frequency"))
        
        logger.info(f"Created vocabulary with {vocab_df.count()} unique terms")
        
        # Create inverted index by exploding postings
        postings_split = parsed_df.select(
            col("term"),
            explode(split(col("postings"), ",")).alias("posting")
        )
        
        inverted_df = postings_split.select(
            col("term"),
            trim(split(col("posting"), ":")[0]).alias("doc_id"),
            trim(split(col("posting"), ":")[1]).cast(IntegerType()).alias("term_freq"),
            trim(split(col("posting"), ":")[2]).cast(FloatType()).alias("tfidf")
        ).filter(col("doc_id").isNotNull() & (col("doc_id") != ""))
        
        logger.info(f"Created inverted index with {inverted_df.count()} postings")
        
        return vocab_df, inverted_df
    
    def parse_document_stats(self, raw_df):
        """
        Parse document statistics from metadata.
        
        Format: __DOC_STATS__    doc_id,unique_terms,token_count,avg_term_freq
        
        Returns: DataFrame with (doc_id, unique_terms, token_count, avg_term_freq)
        """
        logger.info("Parsing document statistics")
        
        # Filter document stats entries
        doc_stats_lines = raw_df.filter(col("value").startswith("__DOC_STATS__"))
        
        # Split by tab and extract fields
        split_df = doc_stats_lines.select(split(col("value"), "\t")[1].alias("stats_fields"))
        
        stats_df = split_df.select(
            trim(split(col("stats_fields"), ",")[0]).alias("doc_id"),
            trim(split(col("stats_fields"), ",")[1]).cast(IntegerType()).alias("unique_terms"),
            trim(split(col("stats_fields"), ",")[2]).cast(IntegerType()).alias("token_count"),
            trim(split(col("stats_fields"), ",")[3]).cast(FloatType()).alias("avg_term_freq")
        )
        
        count = stats_df.count()
        logger.info(f"Parsed statistics for {count} documents")
        
        return stats_df
    
    def parse_collection_stats(self, raw_df):
        """
        Parse collection statistics from metadata.
        
        Format: __COLLECTION_STATS__    total_docs:N
        
        Returns: DataFrame with (stat_name, stat_value)
        """
        logger.info("Parsing collection statistics")
        
        # Filter collection stats entries
        coll_stats_lines = raw_df.filter(col("value").startswith("__COLLECTION_STATS__"))
        
        # Split and extract
        split_df = coll_stats_lines.select(
            split(col("value"), "\t")[1].alias("stat_pair")
        )
        
        stats_df = split_df.select(
            trim(split(col("stat_pair"), ":")[0]).alias("stat_name"),
            trim(split(col("stat_pair"), ":")[1]).alias("stat_value")
        )
        
        count = stats_df.count()
        logger.info(f"Parsed {count} collection statistics")
        
        return stats_df
    
    def write_to_cassandra(self, df, table_name, write_mode="append"):

        logger.info(f"Writing {df.count()} rows to table: {table_name}")
        
        try:
            writer = df.write\
                .format("org.apache.spark.sql.cassandra")\
                .option("keyspace", self.keyspace)\
                .option("table", table_name)\
                .option("spark.cassandra.output.batch.size.rows", str(self.batch_size))
            
            # Add truncate confirmation for overwrite mode
            if write_mode == "overwrite":
                writer = writer.option("confirm.truncate", "true")
            
            writer.mode(write_mode).save()
            
            logger.info(f"Successfully wrote to {table_name}")
        except Exception as e:
            logger.error(f"Failed to write to {table_name}: {e}")
            raise
    
    def load_all(self):
        logger.info("Starting index data load to ScyllaDB")
        
        # Read raw data
        raw_df = self.read_index_data()
        
        # Parse and load vocabulary
        logger.info("=" * 52)
        logger.info("Loading Vocabulary")
        logger.info("=" * 52)
        vocab_df, inverted_df = self.parse_index_entries(raw_df)
        self.write_to_cassandra(vocab_df, "vocabulary", write_mode="overwrite")
        
        # Parse and load inverted index
        logger.info("=" * 52)
        logger.info("Loading Inverted Index")
        logger.info("=" * 52)
        self.write_to_cassandra(inverted_df, "inverted_index", write_mode="overwrite")
        
        # Parse and load document statistics
        logger.info("=" * 52)
        logger.info("Loading Document Statistics")
        logger.info("=" * 52)
        doc_stats_df = self.parse_document_stats(raw_df)
        self.write_to_cassandra(doc_stats_df, "document_stats", write_mode="overwrite")
        
        # Parse and load collection statistics
        logger.info("=" * 52)
        logger.info("Loading Collection Statistics")
        logger.info("=" * 52)
        coll_stats_df = self.parse_collection_stats(raw_df)
        self.write_to_cassandra(coll_stats_df, "collection_stats", write_mode="overwrite")
        
        logger.info("=" * 52)
        logger.info("All data loaded successfully!")
        logger.info("=" * 52)


def main():
    parser = argparse.ArgumentParser(description='Load MapReduce index data into ScyllaDB')
    parser.add_argument(
        '--index-path',
        default='/indexer/final_index',
        help='HDFS path to index data'
    )
    parser.add_argument(
        '--keyspace',
        default='search_index',
        help='Cassandra keyspace name'
    )
    parser.add_argument(
        '--cassandra-host',
        default='scylla-master',
        help='Cassandra/ScyllaDB host (default: scylla-master)'
    )
    parser.add_argument(
        '--cassandra-port',
        type=int,
        default=9042,
        help='Cassandra/ScyllaDB port (default: 9042)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for Cassandra writes'
    )
    
    args = parser.parse_args()
    
    # Create Spark session with host/port from arguments
    spark = SparkSession.builder\
        .appName("LoadIndexToCassandra")\
        .config("spark.cassandra.connection.host", args.cassandra_host)\
        .config("spark.cassandra.connection.port", str(args.cassandra_port))\
        .getOrCreate()
    
    logger.info(f"Spark session created - Cassandra: {args.cassandra_host}:{args.cassandra_port}")
    
    try:
        loader = IndexLoader(
            spark=spark,
            index_path=args.index_path,
            keyspace=args.keyspace,
            batch_size=args.batch_size
        )
        
        loader.load_all()
        
        logger.info("Index loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error loading index: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == '__main__':
    main()
