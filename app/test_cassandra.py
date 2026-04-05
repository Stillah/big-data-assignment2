#!/usr/bin/env python3
import sys
import argparse
from cassandra.cluster import Cluster


def test_connection(host, port):
    """Test connection to ScyllaDB."""
    try:
        cluster = Cluster(contact_points=[host],
                            port=port,
                            connect_timeout=10)
        session = cluster.connect()
        print(f"Connected to ScyllaDB at {host}:{port}")
        session.shutdown()
        cluster.shutdown()
        return True
    
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Test ScyllaDB connectivity")
    parser.add_argument(
        "--host",
        default="localhost",
        help="ScyllaDB host. default=localhost"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9042,
        help="ScyllaDB port. default: 9042"
    )
    
    args = parser.parse_args()
    
    if not test_connection(args.host, args.port):
        sys.exit(1)


if __name__ == "__main__":
    main()
