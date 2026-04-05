#!/usr/bin/env python3
"""
Create ScyllaDB keyspace and search tables.

Usage:
    python3 create_cassandra_schema.py \
        --host localhost \
        --port 9042 \
        --keyspace search_index \
        --replication-factor 1
"""

import sys
import argparse
from cassandra.cluster import Cluster


# CQL table definitions
TABLES = [
    # Vocabulary table
    """
    CREATE TABLE IF NOT EXISTS {keyspace}.vocabulary (
        term_id int PRIMARY KEY,
        term text,
        idf float,
        doc_frequency int
    )
    """,
    
    # Inverted index table
    """
    CREATE TABLE IF NOT EXISTS {keyspace}.inverted_index (
        term text,
        doc_id text,
        term_freq int,
        tfidf float,
        PRIMARY KEY (term, doc_id)
    ) WITH CLUSTERING ORDER BY (doc_id ASC)
    """,
    
    # Document statistics table
    """
    CREATE TABLE IF NOT EXISTS {keyspace}.document_stats (
        doc_id text PRIMARY KEY,
        unique_terms int,
        token_count int,
        avg_term_freq float
    )
    """,
    
    # Collection statistics table
    """
    CREATE TABLE IF NOT EXISTS {keyspace}.collection_stats (
        stat_name text PRIMARY KEY,
        stat_value text
    )
    """
]


def create_keyspace(session, keyspace, replication_factor):
    """Create keyspace."""
    query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
    """
    session.execute(query)
    print(f"✓ Keyspace '{keyspace}' created")


def create_tables(session, keyspace):
    """Create all search tables."""
    for table_query in TABLES:
        formatted_query = table_query.format(keyspace=keyspace)
        try:
            session.execute(formatted_query)
            table_name = formatted_query.split("CREATE TABLE")[1].split("(")[0].strip()
            table_name = table_name.replace("IF NOT EXISTS", "").strip()
            print(f"✓ Table '{table_name}' created")
        except Exception as e:
            print(f"⚠ Table creation: {str(e)[:60]}")


def main():
    parser = argparse.ArgumentParser(
        description="Create ScyllaDB keyspace and tables"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="ScyllaDB host (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9042,
        help="ScyllaDB port (default: 9042)"
    )
    parser.add_argument(
        "--keyspace",
        default="search_index",
        help="Keyspace name (default: search_index)"
    )
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor (default: 1)"
    )
    
    args = parser.parse_args()
    
    try:
        cluster = Cluster(
            contact_points=[args.host],
            port=args.port,
            connect_timeout=10
        )
        session = cluster.connect()
        
        create_keyspace(session, args.keyspace, args.replication_factor)
        
        # Set keyspace for table creation
        session.set_keyspace(args.keyspace)
        create_tables(session, args.keyspace)
        
        session.shutdown()
        cluster.shutdown()
        
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
