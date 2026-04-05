#!/usr/bin/env python3
import sys
import argparse
from cassandra.cluster import Cluster

TABLES = ['vocabulary', 'inverted_index', 'document_stats', 'collection_stats']


def verify_data(host, port, keyspace):
    try:
        cluster = Cluster(contact_points=[host], port=port)
        session = cluster.connect(keyspace)
        
        print(f"\nTable row counts in keyspace '{keyspace}':")
        print("─" * 52)
        
        total_rows = 0
        for table in TABLES:
            try:
                result = session.execute(f"select count(*) as count from {table}")
                count = result[0].count
                print(f"  {table:<25} {count:>10,} rows")
                total_rows += count
            except Exception as e:
                print(f"  {table:<25} Error: {str(e)[:30]}")
        
        print("─" * 52)
        print(f"  {'Total':<25} {total_rows:>10,} rows")
        print()
        
        session.shutdown()
        cluster.shutdown()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Verify data loaded into ScyllaDB")
    parser.add_argument(
        "--host",
        default="localhost",
        help="ScyllaDB host (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9042,
        help="ScyllaDB port, default=9042"
    )
    parser.add_argument(
        "--keyspace",
        default="search_index",
        help="Keyspace name, default=search_index"
    )
    
    args = parser.parse_args()
    verify_data(args.host, args.port, args.keyspace)


if __name__ == "__main__":
    main()
