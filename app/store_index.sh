set -e

# Color codes
COLOR_RESET='\033[0m'
COLOR_GREEN='\033[0;32m'
COLOR_YELLOW='\033[1;33m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'

# Configuration
INDEX_PATH="${INDEX_PATH:-/indexer/final_index}"
CASSANDRA_HOST="${CASSANDRA_HOST:-scylla-master}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
KEYSPACE_NAME="${KEYSPACE_NAME:-search_index}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
CREATE_KEYSPACE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helper functions
header() { echo -e "\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}\n${COLOR_BLUE}$1${COLOR_RESET}\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}"; }
status() { echo -e "${COLOR_GREEN}✓${COLOR_RESET} $1"; }
error() { echo -e "${COLOR_RED}✗${COLOR_RESET} $1"; }
warn() { echo -e "${COLOR_YELLOW}⚠${COLOR_RESET} $1"; }
info() { echo "  $1"; }
help() { grep "^# " "$0" | head -25; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --index-path) INDEX_PATH="$2"; shift 2 ;;
        --cassandra-host) CASSANDRA_HOST="$2"; shift 2 ;;
        --cassandra-port) CASSANDRA_PORT="$2"; shift 2 ;;
        --keyspace) KEYSPACE_NAME="$2"; shift 2 ;;
        --replication-factor) REPLICATION_FACTOR="$2"; shift 2 ;;
        --create-keyspace) CREATE_KEYSPACE=true; shift ;;
        --help) help; exit 0 ;;
        *) error "Unknown option: $1"; help; exit 1 ;;
    esac
done

# Validate prerequisites
header "Validating Prerequisites"
command -v spark-submit &>/dev/null || { error "spark-submit not found"; exit 1; }
status "Spark found"
command -v python3 &>/dev/null || { error "Python3 not found"; exit 1; }
status "Python3 found"
[ -f "$SCRIPT_DIR/load_index_to_cassandra.py" ] || { error "Missing load_index_to_cassandra.py"; exit 1; }
status "Load script found"
hadoop fs -test -d "$INDEX_PATH" 2>/dev/null || { error "Index not found at $INDEX_PATH"; exit 1; }
status "Index found at $INDEX_PATH"

# Test Cassandra connection
header "Testing Cassandra/ScyllaDB Connection"
python3 "$SCRIPT_DIR/test_cassandra.py" --host "$CASSANDRA_HOST" --port "$CASSANDRA_PORT" || exit 1

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
header "Loading Index Data with PySpark"
info "Index path: $INDEX_PATH"
info "Cassandra: $CASSANDRA_HOST:$CASSANDRA_PORT"
info "Keyspace: $KEYSPACE_NAME"

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

echo -e "\n${COLOR_GREEN}✓ Index successfully stored in Cassandra/ScyllaDB${COLOR_RESET}\n"
