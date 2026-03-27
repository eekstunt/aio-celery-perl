#!/bin/bash
set -e

cd "$(dirname "$0")"
BENCH_DIR="$(pwd)"
PROJECT_DIR="$(dirname "$BENCH_DIR")"
OUTPUT_DIR="$BENCH_DIR/output"
VENV="$BENCH_DIR/.venv/bin/python3"
PERL5LIB="/Users/eekstunt/perl5/lib/perl5:$PROJECT_DIR/lib"

NUM_TASKS="${1:-10000}"
NUM_RUNS="${2:-5}"

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo " Python aio-celery vs Perl AIO::Celery"
echo " $NUM_TASKS tasks x $NUM_RUNS runs"
echo "============================================================"
echo ""

# Start Docker services
echo "[1/4] Starting Docker (RabbitMQ)..."
docker compose down -v 2>/dev/null || true
docker compose up -d --wait 2>&1 | tail -3
echo ""

# Wait for RabbitMQ
echo "[2/4] Waiting for RabbitMQ..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:15673/api/overview -u guest:guest > /dev/null 2>&1; then
        echo "  RabbitMQ is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: RabbitMQ not ready."
        exit 1
    fi
    sleep 1
done
echo ""

# Run Python benchmark
echo "[3/4] Running Python aio-celery benchmark..."
echo ""
$VENV "$BENCH_DIR/bench_celery_python.py" "$NUM_TASKS" "$NUM_RUNS" 2>&1 | tee "$OUTPUT_DIR/python_bench.txt"

# Purge queues between runs
curl -sf -XDELETE http://127.0.0.1:15673/api/queues/%2F/celery -u guest:guest > /dev/null 2>&1 || true
sleep 1

# Run Perl benchmark
echo ""
echo "[4/4] Running Perl AIO::Celery benchmark..."
echo ""
PERL5LIB="$PERL5LIB" perl "$BENCH_DIR/bench_celery_perl.pl" "$NUM_TASKS" "$NUM_RUNS" 2>&1 | tee "$OUTPUT_DIR/perl_bench.txt"

# Extract results
echo ""
echo "============================================================"
echo " COMPARISON"
echo "============================================================"

PY_SER=$(grep PYTHON_SERIALIZE_AVG "$OUTPUT_DIR/python_bench.txt" | cut -d= -f2)
PY_DES=$(grep PYTHON_DESERIALIZE_AVG "$OUTPUT_DIR/python_bench.txt" | cut -d= -f2)
PY_PUB=$(grep PYTHON_PUBLISH_AVG "$OUTPUT_DIR/python_bench.txt" | cut -d= -f2)

PL_SER=$(grep PERL_SERIALIZE_AVG "$OUTPUT_DIR/perl_bench.txt" | cut -d= -f2)
PL_DES=$(grep PERL_DESERIALIZE_AVG "$OUTPUT_DIR/perl_bench.txt" | cut -d= -f2)
PL_PUB=$(grep PERL_PUBLISH_AVG "$OUTPUT_DIR/perl_bench.txt" | cut -d= -f2)

echo ""
printf "%-20s %12s %12s %10s\n" "Benchmark" "Python (s)" "Perl (s)" "Winner"
printf "%-20s %12s %12s %10s\n" "--------------------" "----------" "----------" "--------"

for BENCH in SERIALIZE DESERIALIZE PUBLISH; do
    PY_VAL=$(grep "PYTHON_${BENCH}_AVG" "$OUTPUT_DIR/python_bench.txt" | cut -d= -f2)
    PL_VAL=$(grep "PERL_${BENCH}_AVG" "$OUTPUT_DIR/perl_bench.txt" | cut -d= -f2)
    WINNER=$(python3 -c "
py, pl = $PY_VAL, $PL_VAL
if py < pl: print(f'Python {pl/py:.1f}x')
else: print(f'Perl {py/pl:.1f}x')
")
    BENCH_LOWER=$(echo "$BENCH" | tr '[:upper:]' '[:lower:]')
    printf "%-20s %12.3f %12.3f %10s\n" "$BENCH_LOWER" "$PY_VAL" "$PL_VAL" "$WINNER"
done

echo ""
printf "Tasks: %d, Runs: %d\n" "$NUM_TASKS" "$NUM_RUNS"
echo "============================================================"

# Save comparison
{
    echo "PYTHON_SERIALIZE=$PY_SER"
    echo "PYTHON_DESERIALIZE=$PY_DES"
    echo "PYTHON_PUBLISH=$PY_PUB"
    echo "PERL_SERIALIZE=$PL_SER"
    echo "PERL_DESERIALIZE=$PL_DES"
    echo "PERL_PUBLISH=$PL_PUB"
} > "$OUTPUT_DIR/comparison.env"

# Cleanup
echo ""
echo "Stopping Docker..."
docker compose down -v 2>/dev/null
echo "Done."
