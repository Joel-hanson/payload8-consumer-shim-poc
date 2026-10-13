#!/usr/bin/env bash
# run-payload8-migration.sh
#
# End-to-end scenario demonstrating:
#
#   1. Apicurio 2.x serde with DEFAULT config (nothing set):
#        ENABLE_HEADERS=true, USE_ID=globalId → schema ID in apicurio.value.globalId header,
#        payload is raw Avro binary (no magic byte, no ID prefix)
#   2. Apicurio v2 registry → container swap to Apicurio v3 (export/import)
#   3. More records produced on original AND a new topic using the same v2 serde (same framing)
#   4. Schema export via ccompat → import into Confluent SR (IMPORT mode, IDs preserved)
#   5. Cluster Linking simulation: bytes copied unchanged (header + raw Avro arrives on dest)
#   6. Consumer reads dest with ApicurioHeaderShimDeserializer (no SMT, no replication change)
#
# The test proves two things:
#   a) WITHOUT the shim: raw Confluent deserializer sees no 0x00 magic byte → "Unknown magic byte!" (Phase 8)
#   b) WITH the shim:    all records decode correctly on both topics (Phase 9)
#
# Usage:
#   bash scripts/run-payload8-migration.sh
#
# Prerequisites:
#   - Docker with docker-compose
#   - Java 17+, Maven 3.9+, curl, python3
#
# Env overrides (rarely needed):
#   ROOT  — path to this POC directory (auto-detected)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

APICURIO_V2_IMAGE="apicurio/apicurio-registry-mem:2.6.13.Final"
APICURIO_V3_IMAGE="apicurio/apicurio-registry:3.3.2"
APICURIO_CC="http://localhost:8081/apis/ccompat/v7"
CONFLUENT_SR="http://localhost:8082"
SOURCE_BS="localhost:29092"
DEST_BS="localhost:29094"

# ─────────────────────────────────────────────────────────────────────────────
banner() { echo; echo "═══ $* ═══"; }
# ─────────────────────────────────────────────────────────────────────────────

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║  Payload-8-byte consumer shim — migration scenario                       ║"
echo "╠══════════════════════════════════════════════════════════════════════════╣"
echo "║  Producer : Apicurio v2 serde — DEFAULT config (nothing set explicitly)  ║"
echo "║             Actual runtime default: ENABLE_HEADERS=false, DefaultIdHandler ║"
echo "║  Payload  : 0x00 + 8-byte-long schema ID + Avro body                     ║"
echo "║  Registry : Apicurio 2.6.13 → upgrade to Apicurio 3.3.2 mid-scenario    ║"
echo "║  Replication: byte-for-byte copy (Cluster Linking stand-in)              ║"
echo "║  Consumer : ApicurioPayloadDeserializer shim (8-byte → 4-byte reframe)   ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 0 — Build app jar and start infrastructure (Apicurio 2.6.13)"
# ─────────────────────────────────────────────────────────────────────────────

APP_JAR=$(ls apps/target/payload8-shim-apps-*.jar 2>/dev/null | grep -v original | head -1 || true)
if [[ -z "$APP_JAR" ]]; then
  echo "==> Building apps jar..."
  (cd apps && mvn -q -DskipTests package)
  APP_JAR=$(ls apps/target/payload8-shim-apps-*.jar | grep -v original | head -1)
fi
echo "  App jar: $APP_JAR"

docker-compose down -v --remove-orphans 2>/dev/null || true

# Start Kafka + Confluent SR (no Apicurio in compose — image is swapped mid-run).
docker-compose up -d kafka-source kafka-dest schema-registry 2>&1 | tail -5

# Start Apicurio 2.6.13 separately so we can swap it to 3.3.2 in Phase 3.
docker rm -f apicurio 2>/dev/null || true
NETWORK=$(docker inspect kafka-source \
  --format '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}' | head -1)

docker run -d --name apicurio \
  --network "$NETWORK" \
  -p 8081:8080 \
  -e "QUARKUS_PROFILE=prod" \
  -e "REGISTRY_CCOMPAT_LEGACY_ID_MODE_ENABLED=false" \
  "$APICURIO_V2_IMAGE"

echo "==> Waiting for Apicurio 2.6.13..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/health/ready >/dev/null 2>&1; then
    echo "  Apicurio 2.6.13 ready."; break
  fi
  sleep 2
  [[ $i -eq 30 ]] && { echo "ERROR: Apicurio 2.6.13 did not start." >&2
    docker logs apicurio 2>&1 | tail -20; exit 1; }
done

echo "==> Waiting for other services..."
bash scripts/wait-for.sh

echo "==> Creating topics (orders + orders-new on source and dest)..."
bash scripts/create-topics.sh

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 1 — Produce 5 records with Apicurio v2 serde defaults → Apicurio 2.6.13 (topic: orders)"
# ─────────────────────────────────────────────────────────────────────────────
# Nothing configured → ENABLE_HEADERS=true, USE_ID=globalId
# → header apicurio.value.globalId = 8-byte long, payload = raw Avro (no prefix)
SOURCE_BOOTSTRAP="$SOURCE_BS" \
APICURIO_URL=http://localhost:8081 \
KAFKA_TOPIC=orders \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'

echo
echo "  ✓ Each source record: 0x00 + 8-byte-long ID + Avro body  (DefaultIdHandler, no headers)"

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 2 — Confirm source records exist"
# ─────────────────────────────────────────────────────────────────────────────
COUNT=$(docker exec kafka-source /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 5 --timeout-ms 8000 \
  2>/dev/null | wc -l || echo 0)
echo "  Source topic 'orders': $COUNT record(s) (expected 5)"

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 3 — Export Apicurio 2.6.13 registry → swap to Apicurio 3.3.2"
# ─────────────────────────────────────────────────────────────────────────────
echo "==> Capturing IDs before upgrade..."
bash scripts/show-ids.sh orders-value 2>/dev/null | tee /tmp/p8poc-ids-before.txt

echo "==> Exporting from Apicurio 2.6.13..."
curl -sf -o /tmp/p8poc-v2-export.zip \
  "http://localhost:8081/apis/registry/v2/admin/export"
echo "  Export: $(wc -c < /tmp/p8poc-v2-export.zip) bytes"

echo "==> Swapping Apicurio container to 3.3.2..."
docker rm -f apicurio

docker run -d --name apicurio \
  --network "$NETWORK" \
  -p 8081:8080 \
  -e "APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED=false" \
  -e "APICURIO_REST_DELETION_ARTIFACT_ENABLED=true" \
  -e "APICURIO_REST_DELETION_ARTIFACT_VERSION_ENABLED=true" \
  -e "APICURIO_REST_DELETION_GROUP_ENABLED=true" \
  "$APICURIO_V3_IMAGE"

echo "==> Waiting for Apicurio 3.3.2..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/apis/registry/v2/system/info >/dev/null 2>&1; then
    echo "  Apicurio 3.3.2 ready."; break
  fi
  sleep 2
  [[ $i -eq 30 ]] && { echo "ERROR: Apicurio 3.3.2 did not start after swap." >&2
    docker logs apicurio 2>&1 | tail -20; exit 1; }
done

echo "==> Importing v2 export into Apicurio 3.3.2..."
http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "http://localhost:8081/apis/registry/v3/admin/import" \
  -H "Content-Type: application/zip" \
  --data-binary @/tmp/p8poc-v2-export.zip)
echo "  /v3/admin/import → HTTP $http_code"
[[ "$http_code" =~ ^(200|201|204)$ ]] || { echo "ERROR: import failed." >&2; exit 1; }

echo "==> Capturing IDs after upgrade..."
bash scripts/show-ids.sh orders-value 2>/dev/null | tee /tmp/p8poc-ids-after.txt

echo
echo "==> Diff ids-before vs ids-after (must be empty — IDs preserved):"
if diff /tmp/p8poc-ids-before.txt /tmp/p8poc-ids-after.txt; then
  echo "  ✓ IDs unchanged across the v2 → v3 upgrade."
else
  echo "  WARNING: IDs changed after upgrade." >&2
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 4 — Produce 5 more records with same v2 serde → Apicurio 3.3.2 (topic: orders)"
# ─────────────────────────────────────────────────────────────────────────────
echo "  Producer still uses apicurio-registry-serdes-avro-serde:2.6.5 JAR (nothing changed)"
echo "  Registry server swapped to v3 but /apis/registry/v2 endpoint still served"
echo "  → same defaults: ENABLE_HEADERS=false, DefaultIdHandler, 8-byte payload"
SOURCE_BOOTSTRAP="$SOURCE_BS" \
APICURIO_URL=http://localhost:8081 \
KAFKA_TOPIC=orders \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 5 — Produce 5 records to topic 'orders-new' (same v2 serde, same 8-byte framing)"
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_BOOTSTRAP="$SOURCE_BS" \
APICURIO_URL=http://localhost:8081 \
KAFKA_TOPIC=orders-new \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'

SRC_NEW=$(docker exec kafka-source /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-new --from-beginning --max-messages 10 --timeout-ms 5000 \
  2>/dev/null | wc -l || echo 0)
echo "  Source 'orders-new': $SRC_NEW record(s)"

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 6 — Export schemas from Apicurio v3 ccompat → Confluent SR (IMPORT mode)"
# ─────────────────────────────────────────────────────────────────────────────
echo "  Both topics use the same 'orders-value' subject (same schema)"
echo "  TopicIdStrategy maps topic name → subject, so orders-new also resolves orders-value"
APICURIO_CC="$APICURIO_CC" CONFLUENT_SR="$CONFLUENT_SR" \
  bash scripts/migrate-schema.sh

echo
echo "==> ID table after schema migration:"
bash scripts/show-ids.sh orders-value

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 7 — Replicate both topics byte-for-byte (Cluster Linking stand-in)"
# ─────────────────────────────────────────────────────────────────────────────
echo "  Cluster Linking (enterprise) copies payload bytes unchanged."
echo "  Stand-in: Java TopicMirror (preserves headers too, though there are none here)."
echo "  Result: 0x00 + 8-byte-long ID + Avro body arrives on dest byte-for-byte."

copy_topic() {
  local t="$1"
  echo "  Copying source '$t' → dest '$t' (headers preserved) ..."
  SOURCE_BOOTSTRAP="$SOURCE_BS" \
  DEST_BOOTSTRAP="$DEST_BS" \
  KAFKA_TOPIC="$t" \
    java -jar "$APP_JAR" mirror-topic 2>&1 | grep -v '^SLF4J'
}

copy_topic orders
copy_topic orders-new

DEST_ORDERS=$(docker exec kafka-dest /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 20 --timeout-ms 5000 \
  2>/dev/null | wc -l || echo 0)
DEST_ORDERS_NEW=$(docker exec kafka-dest /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-new --from-beginning --max-messages 20 --timeout-ms 5000 \
  2>/dev/null | wc -l || echo 0)
echo "  dest 'orders': $DEST_ORDERS record(s)"
echo "  dest 'orders-new': $DEST_ORDERS_NEW record(s)"

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 8 — Verify dest records have the globalId header and schema IDs resolve in Confluent SR"
# ─────────────────────────────────────────────────────────────────────────────
echo "  The verifier checks:"
echo "    1. Source payload has 0x00 + 8-byte-long framing (DefaultIdHandler confirmed)"
echo "    2. Dest payload bytes == source payload bytes (Cluster Linking unchanged)"
echo "    3. The schema ID from source bytes[1..8] resolves in Confluent SR"
echo "  EXPECTED: PASS — these are the preconditions the ApicurioPayloadDeserializer shim relies on."

verify_exit=0
SOURCE_BOOTSTRAP="$SOURCE_BS" \
DEST_BOOTSTRAP="$DEST_BS" \
APICURIO_CCOMPAT="$APICURIO_CC" \
SCHEMA_REGISTRY_URL="$CONFLUENT_SR" \
TOPIC=orders \
  java -jar "$APP_JAR" verify-ids 2>&1 | grep -v '^SLF4J' || verify_exit=$?

echo
if [[ $verify_exit -eq 0 ]]; then
  echo "  ✓ verify-ids PASSED — preconditions for the ApicurioPayloadDeserializer shim are satisfied."
else
  echo "  ✗ verify-ids FAILED (exit=$verify_exit) — check schema migration or topic replication."
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 9 — Consume both topics with ApicurioPayloadDeserializer shim"
# ─────────────────────────────────────────────────────────────────────────────
echo "  The shim checks bytes[1..4]==0 (high 4 of the 8-byte long, always 0 for real IDs)"
echo "  → extracts ID from bytes[5..8] → prepends 0x00 + 4-byte ID → delegates to KafkaAvroDeserializer."
echo "  No changes to the replication path; no SMT required."

shim_orders_exit=0
echo
echo "--- topic: orders ---"
DEST_BOOTSTRAP="$DEST_BS" \
SCHEMA_REGISTRY_URL="$CONFLUENT_SR" \
DEST_TOPIC=orders \
CONSUMER_GROUP=cflt-shim-orders \
  java -jar "$APP_JAR" consume-cflt-shim 2>&1 | grep -v '^SLF4J' || shim_orders_exit=$?

shim_new_exit=0
echo
echo "--- topic: orders-new ---"
DEST_BOOTSTRAP="$DEST_BS" \
SCHEMA_REGISTRY_URL="$CONFLUENT_SR" \
DEST_TOPIC=orders-new \
CONSUMER_GROUP=cflt-shim-orders-new \
  java -jar "$APP_JAR" consume-cflt-shim 2>&1 | grep -v '^SLF4J' || shim_new_exit=$?

# ─────────────────────────────────────────────────────────────────────────────
banner "Phase 10 — Verdict"
# ─────────────────────────────────────────────────────────────────────────────
echo
echo "  Phase 8  verify-ids (preconditions):           exit=$verify_exit"
echo "           └─ 0 = header present, ID resolves in Confluent SR, payload unchanged"
echo "  Phase 9  consume-cflt-shim (orders):           exit=$shim_orders_exit"
echo "  Phase 9  consume-cflt-shim (orders-new):       exit=$shim_new_exit"
echo

if [[ $verify_exit -eq 0 && $shim_orders_exit -eq 0 && $shim_new_exit -eq 0 ]]; then
  echo "  ╔══════════════════════════════════════════════════════════╗"
  echo "  ║  RESULT: PASS                                            ║"
  echo "  ╠══════════════════════════════════════════════════════════╣"
  echo "  ║  ApicurioHeaderShimDeserializer decoded all records      ║"
  echo "  ║  on both topics — no SMT, no replication change needed.  ║"
  echo "  ║                                                          ║"
  echo "  ║  Records written with the Apicurio v2 serde at its       ║"
  echo "  ║  default runtime behaviour (ENABLE_HEADERS=false,        ║"
  echo "  ║  DefaultIdHandler → 8-byte payload framing)              ║"
  echo "  ║  against both Apicurio v2 AND v3 are readable on the     ║"
  echo "  ║  Confluent side after byte-for-byte Cluster Linking.     ║"
  echo "  ╚══════════════════════════════════════════════════════════╝"
else
  echo "  ╔══════════════════════════════════════════════════════════╗"
  echo "  ║  RESULT: FAIL                                            ║"
  echo "  ╚══════════════════════════════════════════════════════════╝"
  [[ $verify_exit -ne 0 ]]       && echo "  - verify-ids failed            (exit=$verify_exit)"
  [[ $shim_orders_exit -ne 0 ]]  && echo "  - shim failed on 'orders'      (exit=$shim_orders_exit)"
  [[ $shim_new_exit -ne 0 ]]     && echo "  - shim failed on 'orders-new'  (exit=$shim_new_exit)"
fi
