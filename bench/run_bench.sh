#!/bin/bash
set -e

cd "$(dirname "$0")"
BENCH_DIR="$(pwd)"
PROJECT_DIR="$(dirname "$BENCH_DIR")"
OUTPUT_DIR="$BENCH_DIR/output"

NUM_TASKS="${1:-10000}"
CONCURRENCY="${2:-10000}"

echo "============================================"
echo " AIO::Celery Async Benchmark"
echo " Tasks: $NUM_TASKS  Concurrency: $CONCURRENCY"
echo "============================================"
echo ""

# Cleanup
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Start services
echo "[1/5] Starting Docker services (RabbitMQ + Nginx)..."
docker compose down -v 2>/dev/null || true
docker compose up -d --wait 2>&1 | tail -3

echo ""
echo "[2/5] Waiting for Nginx to be ready..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:8888/ > /dev/null 2>&1; then
        echo "  Nginx is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: Nginx did not start in time."
        docker compose logs nginx
        exit 1
    fi
    sleep 1
done

echo ""
echo "[3/5] Running benchmark ($NUM_TASKS tasks, concurrency $CONCURRENCY)..."
echo ""

PERL5LIB="/Users/eekstunt/perl5/lib/perl5:$PROJECT_DIR/lib" \
    perl "$BENCH_DIR/bench_async.pl" "$NUM_TASKS" "$CONCURRENCY" 2>&1 | tee "$OUTPUT_DIR/bench_output.txt"

echo ""
echo "[4/5] Collecting logs..."

# Nginx access log
docker compose exec -T nginx cat /var/log/nginx/access.log > "$OUTPUT_DIR/nginx_access.log" 2>/dev/null || true
docker compose exec -T nginx cat /var/log/nginx/error.log > "$OUTPUT_DIR/nginx_error.log" 2>/dev/null || true

# RabbitMQ logs
docker compose logs rabbitmq > "$OUTPUT_DIR/rabbitmq.log" 2>&1 || true

# Count unique seconds in Nginx log to show parallelism
echo ""
echo "[5/5] Analyzing concurrency from Nginx logs..."
echo ""

TOTAL_LINES=$(wc -l < "$OUTPUT_DIR/nginx_access.log" | tr -d ' ')
echo "  Total Nginx access log entries: $TOTAL_LINES"

if [ "$TOTAL_LINES" -gt 0 ]; then
    echo ""
    echo "  Requests per second (top 10 busiest seconds):"
    awk '{print $4}' "$OUTPUT_DIR/nginx_access.log" | \
        sed 's/\[//' | \
        cut -d: -f1-4 | \
        sort | uniq -c | sort -rn | head -10 | \
        while read count ts; do
            echo "    $ts  →  $count requests"
        done

    echo ""
    # Check how many distinct seconds had requests
    DISTINCT_SECONDS=$(awk '{print $4}' "$OUTPUT_DIR/nginx_access.log" | \
        sed 's/\[//' | cut -d: -f1-4 | sort -u | wc -l | tr -d ' ')
    echo "  Distinct seconds with activity: $DISTINCT_SECONDS"

    if [ "$DISTINCT_SECONDS" -le 5 ] && [ "$TOTAL_LINES" -ge 1000 ]; then
        echo ""
        echo "  ✅ HIGH CONCURRENCY CONFIRMED: $TOTAL_LINES requests in $DISTINCT_SECONDS seconds"
        echo "     This proves requests arrive massively in parallel."
    fi
fi

echo ""
echo "============================================"
echo " Output files:"
echo "   $OUTPUT_DIR/bench_output.txt      — Benchmark results"
echo "   $OUTPUT_DIR/nginx_access.log      — Nginx access log"
echo "   $OUTPUT_DIR/nginx_error.log       — Nginx error log"
echo "   $OUTPUT_DIR/rabbitmq.log          — RabbitMQ log"
echo "============================================"

# Cleanup docker
echo ""
echo "Stopping Docker services..."
docker compose down -v 2>/dev/null
echo "Done."
