#!/usr/bin/env bash
# Full integration test for the distributed KV store.
# Prerequisites: docker compose up -d (all 3 nodes running)

set -euo pipefail

BASE1="localhost:8081"
BASE2="localhost:8082"
BASE3="localhost:8083"
PASS=0
FAIL=0

ok()   { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

check_value() {
  local url="$1"
  local expected="$2"
  local label="$3"
  local actual
  actual=$(curl -sf "$url" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value',''))" 2>/dev/null || echo "__error__")
  if [ "$actual" = "$expected" ]; then
    ok "$label"
  else
    fail "$label (expected '$expected', got '$actual')"
  fi
}

echo ""
echo "============================================"
echo " Distributed KV Store — Integration Tests"
echo "============================================"
echo ""

# ── 1. Basic write and read ───────────────────────────────────────────────────
echo "1. Basic write and read"

curl -sf -X PUT "$BASE1/key/city" \
  -H "Content-Type: application/json" \
  -d '{"value":"Hanoi"}' > /dev/null
sleep 0.3

check_value "$BASE1/key/city" "Hanoi" "read from node1 after write to node1"
check_value "$BASE2/key/city" "Hanoi" "read from node2 after write to node1"
check_value "$BASE3/key/city" "Hanoi" "read from node3 after write to node1"

# ── 2. Write via a different node (coordinator anywhere) ──────────────────────
echo ""
echo "2. Write via node2 (coordinator can be any node)"

curl -sf -X PUT "$BASE2/key/country" \
  -H "Content-Type: application/json" \
  -d '{"value":"Vietnam"}' > /dev/null
sleep 0.3

check_value "$BASE1/key/country" "Vietnam" "read country from node1"
check_value "$BASE3/key/country" "Vietnam" "read country from node3"

# ── 3. Survive node3 failure — write ─────────────────────────────────────────
echo ""
echo "3. Node3 failure — write with W=2 should succeed"

docker compose stop node3 2>/dev/null
sleep 0.5

STATUS=$(curl -sf -o /dev/null -w "%{http_code}" -X PUT "$BASE1/key/city" \
  -H "Content-Type: application/json" \
  -d '{"value":"Hue"}' 2>/dev/null || echo "000")
if [ "$STATUS" = "200" ]; then
  ok "PUT succeeds with 1 node down (W=2)"
else
  fail "PUT returned HTTP $STATUS with 1 node down"
fi

check_value "$BASE1/key/city" "Hue" "read from node1 with node3 down"
check_value "$BASE2/key/city" "Hue" "read from node2 with node3 down"

# ── 4. Survive node3 failure — read ──────────────────────────────────────────
echo ""
echo "4. Node3 failure — read with R=2 should succeed"

STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$BASE1/key/city" 2>/dev/null || echo "000")
if [ "$STATUS" = "200" ]; then
  ok "GET succeeds with 1 node down (R=2)"
else
  fail "GET returned HTTP $STATUS with 1 node down"
fi

# ── 5. Read repair after node3 recovers ──────────────────────────────────────
echo ""
echo "5. Read repair — node3 recovers and gets healed"

docker compose start node3 2>/dev/null
sleep 1

# node3 was down when city was updated to "Hue" — it should be stale
STALE=$(curl -sf "localhost:8083/internal/get/city" 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value',''))" 2>/dev/null || echo "")
if [ "$STALE" = "Hanoi" ] || [ "$STALE" = "" ]; then
  ok "node3 is stale before read repair (value=$STALE)"
else
  echo "  INFO: node3 already has Hue (=$STALE) — read repair may have already run"
fi

# Trigger a coordinator GET — this should fire read repair to node3
curl -sf "$BASE1/key/city" > /dev/null
sleep 0.5

check_value "localhost:8083/internal/get/city" "Hue" "node3 healed via read repair"

# ── 6. Delete ─────────────────────────────────────────────────────────────────
echo ""
echo "6. Delete"

curl -sf -X DELETE "$BASE1/key/city" > /dev/null
sleep 0.3

STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$BASE1/key/city" 2>/dev/null || echo "000")
if [ "$STATUS" = "404" ]; then
  ok "GET returns 404 after DELETE"
else
  fail "GET returned HTTP $STATUS after DELETE (expected 404)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo " Results: $PASS passed, $FAIL failed"
echo "============================================"
echo ""
if [ $FAIL -eq 0 ]; then
  echo "All tests passed."
  exit 0
else
  echo "Some tests failed."
  exit 1
fi
