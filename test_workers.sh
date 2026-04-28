#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

PIDS=()
cleanup() {
    echo -e "\n${YELLOW}Cleaning up background processes...${NC}"
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT

echo -e "${BLUE}=== Starting FastAPI server ===${NC}"
python3 -m uvicorn app.main:app --port 8000 --log-level warning &
PIDS+=($!)
sleep 2

echo -e "${BLUE}=== Starting 3 workers ===${NC}"
for i in 1 2 3; do
    python3 -m app.worker --id "worker-$i" &
    PIDS+=($!)
done
sleep 1

echo -e "\n${BLUE}=== Submitting 5 dummy tasks ===${NC}"
TASK_IDS=()
for i in 1 2 3 4 5; do
    RESPONSE=$(curl -s -X POST http://localhost:8000/tasks \
        -H "Content-Type: application/json" \
        -d "{\"task_type\": \"dummy\", \"args\": [$i]}")
    TASK_ID=$(python3 -c "import sys,json; print(json.load(sys.stdin)['task_id'])" <<< "$RESPONSE")
    TASK_IDS+=("$TASK_ID")
    echo "  Submitted task $i: ${TASK_ID:0:8}..."
done

echo -e "\n${BLUE}=== Submitting 2 add tasks ===${NC}"
for pair in "10,20" "100,200"; do
    IFS=',' read -r a b <<< "$pair"
    RESPONSE=$(curl -s -X POST http://localhost:8000/tasks \
        -H "Content-Type: application/json" \
        -d "{\"task_type\": \"add\", \"args\": [$a, $b]}")
    TASK_ID=$(python3 -c "import sys,json; print(json.load(sys.stdin)['task_id'])" <<< "$RESPONSE")
    TASK_IDS+=("$TASK_ID")
    echo "  Submitted add($a, $b): ${TASK_ID:0:8}..."
done

echo -e "\n${YELLOW}Waiting 5 seconds for tasks to complete...${NC}"
sleep 5

echo -e "\n${BLUE}=== Final task statuses ===${NC}"
for TASK_ID in "${TASK_IDS[@]}"; do
    RESULT=$(curl -s http://localhost:8000/tasks/"$TASK_ID")
    STATUS=$(python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" <<< "$RESULT")
    TASK_TYPE=$(python3 -c "import sys,json; print(json.load(sys.stdin)['task_type'])" <<< "$RESULT")
    TASK_RESULT=$(python3 -c "import sys,json; r=json.load(sys.stdin)['result']; print(r if r else 'N/A')" <<< "$RESULT")
    echo "  ${TASK_ID:0:8}... | ${TASK_TYPE} | status=${STATUS} | result=${TASK_RESULT}"
done

echo -e "\n${GREEN}=== Test complete! ===${NC}"
echo "Check the worker logs above to see which worker handled which task."
