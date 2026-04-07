set -e

# Configuration
INDEX_PATH="${INDEX_PATH:-/indexer/final_index}"
CASSANDRA_HOST="${CASSANDRA_HOST:-scylla-master}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
KEYSPACE_NAME="${KEYSPACE_NAME:-search_index}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
CREATE_KEYSPACE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helper functions
header() { echo ""; echo "--- $1 ---"; }
status() { echo "Done: $1"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --index-path) INDEX_PATH="$2"; shift 2 ;;
        --cassandra-host) CASSANDRA_HOST="$2"; shift 2 ;;
        --cassandra-port) CASSANDRA_PORT="$2"; shift 2 ;;
        --keyspace) KEYSPACE_NAME="$2"; shift 2 ;;
        --replication-factor) REPLICATION_FACTOR="$2"; shift 2 ;;
        --create-keyspace) CREATE_KEYSPACE=true; shift ;;
        --help) echo "Usage: $(basename "$0") [--index-path PATH] [--cassandra-host HOST] [--cassandra-port PORT] [--keyspace NAME] [--replication-factor N] [--create-keyspace]"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Create schema if requested
if [ "$CREATE_KEYSPACE" = true ]; then
    header "Creating Keyspace and Tables"
    python3 "$SCRIPT_DIR/create_cassandra_schema.py" \
        --host "$CASSANDRA_HOST" \
        --port "$CASSANDRA_PORT" \
        --keyspace "$KEYSPACE_NAME" \
        --replication-factor "$REPLICATION_FACTOR" || exit 1
fi

# Load data with PySpark
header "Loading Index Data"
echo "Index path: $INDEX_PATH"
echo "Cassandra: $CASSANDRA_HOST:$CASSANDRA_PORT"
echo "Keyspace: $KEYSPACE_NAME"
echo ""

spark-submit \
    --packages com.scylladb:spark-scylladb-connector_2.12:4.0.0,com.github.jnr:jnr-posix:3.1.16 \
    --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
    --conf spark.cassandra.connection.port="$CASSANDRA_PORT" \
    --conf spark.cassandra.connection.timeout_ms=60000 \
    --conf spark.cassandra.read.timeout_ms=60000 \
    --conf spark.cassandra.output.batch.size.bytes=65536 \
    --conf spark.network.timeout=180s \
    --conf spark.rpc.askTimeout=180s \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=1g \
    --conf spark.memory.offHeap.enabled=false \
    "$SCRIPT_DIR/load_index_to_cassandra.py" \
    --index-path "$INDEX_PATH" \
    --keyspace "$KEYSPACE_NAME" \
    --cassandra-host "$CASSANDRA_HOST" \
    --cassandra-port "$CASSANDRA_PORT" || exit 1

# Verify data
header "Verifying Data"
python3 "$SCRIPT_DIR/verify_cassandra_data.py" \
    --host "$CASSANDRA_HOST" \
    --port "$CASSANDRA_PORT" \
    --keyspace "$KEYSPACE_NAME"

echo ""
echo "Index successfully stored in Cassandra/ScyllaDB"
echo ""
