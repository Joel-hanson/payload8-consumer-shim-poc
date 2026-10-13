#!/usr/bin/env bash
# End-to-end: Apicurio 2.x 8-byte payload → Cluster Linking stand-in → consumer shim.
#
# Producer: ENABLE_HEADERS=false, DefaultIdHandler (0x00 + 8-byte ID + Avro)
# Registry: Apicurio 2.6.13 → upgrade to 3.3.2 mid-run, then ccompat → Confluent IMPORT
# Consumer: ApicurioPayloadDeserializer (reframe to Confluent 4-byte prefix)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

APICURIO_V2_IMAGE="apicurio/apicurio-registry-mem:2.6.13.Final"
APICURIO_V3_IMAGE="apicurio/apicurio-registry:3.3.2"
APICURIO_CC="http://localhost:8081/apis/ccompat/v7"
CONFLUENT_SR="http://localhost:8082"
SOURCE_BS="localhost:29092"
DEST_BS="localhost:29094"

banner() { echo; echo "==> $*"; }

echo "Payload-8 consumer shim demo"
echo "  Producer : ENABLE_HEADERS=false, DefaultIdHandler (8-byte payload ID)"
echo "  Registry : Apicurio 2.6.13 → 3.3.2, then ccompat → Confluent IMPORT"
echo "  Copy     : byte-for-byte (Cluster Linking stand-in)"
echo "  Consumer : ApicurioPayloadDeserializer"

banner "0. Build + start infrastructure (Apicurio 2.6.13)"
APP_JAR=$(ls apps/target/payload8-shim-apps-*.jar 2>/dev/null | grep -v original | head -1 || true)
if [[ -z "$APP_JAR" ]]; then
  (cd apps && mvn -q -DskipTests package)
  APP_JAR=$(ls apps/target/payload8-shim-apps-*.jar | grep -v original | head -1)
fi
echo "  App jar: $APP_JAR"

docker-compose down -v --remove-orphans 2>/dev/null || true
docker-compose up -d kafka-source kafka-dest schema-registry 2>&1 | tail -5

docker rm -f apicurio 2>/dev/null || true
NETWORK=$(docker inspect kafka-source \
  --format '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}' | head -1)

docker run -d --name apicurio \
  --network "$NETWORK" \
  -p 8081:8080 \
  -e "QUARKUS_PROFILE=prod" \
  -e "REGISTRY_CCOMPAT_LEGACY_ID_MODE_ENABLED=false" \
  "$APICURIO_V2_IMAGE"

echo "  Waiting for Apicurio 2.6.13..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/health/ready >/dev/null 2>&1; then
    echo "  Apicurio 2.6.13 ready."; break
  fi
  sleep 2
  [[ $i -eq 30 ]] && { echo "ERROR: Apicurio 2.6.13 did not start." >&2
    docker logs apicurio 2>&1 | tail -20; exit 1; }
done

bash scripts/wait-for.sh
bash scripts/create-topics.sh

banner "1. Produce 5 records → Apicurio 2.6.13 (topic: orders)"
SOURCE_BOOTSTRAP="$SOURCE_BS" \
APICURIO_URL=http://localhost:8081 \
KAFKA_TOPIC=orders \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'
echo "  Expected framing: 0x00 + 8-byte ID + Avro"

banner "2. Confirm source records"
COUNT=$(docker exec kafka-source /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 5 --timeout-ms 8000 \
  2>/dev/null | wc -l || echo 0)
echo "  Source topic 'orders': $COUNT record(s) (expected 5)"

banner "3. Export Apicurio 2.6.13 → swap to Apicurio 3.3.2"
bash scripts/show-ids.sh orders-value 2>/dev/null | tee /tmp/p8poc-ids-before.txt

curl -sf -o /tmp/p8poc-v2-export.zip \
  "http://localhost:8081/apis/registry/v2/admin/export"
echo "  Export: $(wc -c < /tmp/p8poc-v2-export.zip) bytes"

docker rm -f apicurio
docker run -d --name apicurio \
  --network "$NETWORK" \
  -p 8081:8080 \
  -e "APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED=false" \
  -e "APICURIO_REST_DELETION_ARTIFACT_ENABLED=true" \
  -e "APICURIO_REST_DELETION_ARTIFACT_VERSION_ENABLED=true" \
  -e "APICURIO_REST_DELETION_GROUP_ENABLED=true" \
  "$APICURIO_V3_IMAGE"

echo "  Waiting for Apicurio 3.3.2..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/apis/registry/v2/system/info >/dev/null 2>&1; then
    echo "  Apicurio 3.3.2 ready."; break
  fi
  sleep 2
  [[ $i -eq 30 ]] && { echo "ERROR: Apicurio 3.3.2 did not start after swap." >&2
    docker logs apicurio 2>&1 | tail -20; exit 1; }
done

http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "http://localhost:8081/apis/registry/v3/admin/import" \
  -H "Content-Type: application/zip" \
  --data-binary @/tmp/p8poc-v2-export.zip)
echo "  /v3/admin/import → HTTP $http_code"
[[ "$http_code" =~ ^(200|201|204)$ ]] || { echo "ERROR: import failed." >&2; exit 1; }

bash scripts/show-ids.sh orders-value 2>/dev/null | tee /tmp/p8poc-ids-after.txt
if diff /tmp/p8poc-ids-before.txt /tmp/p8poc-ids-after.txt; then
  echo "  IDs unchanged across the v2 → v3 upgrade."
else
  echo "  WARNING: IDs changed after upgrade." >&2
fi

banner "4. Produce 5 more records → Apicurio 3.3.2 (same 2.x serde, topic: orders)"
SOURCE_BOOTSTRAP="$SOURCE_BS" \
APICURIO_URL=http://localhost:8081 \
KAFKA_TOPIC=orders \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'

banner "5. Produce 5 records → topic orders-new"
SOURCE_BOOTSTRAP="$SOURCE_BS" \
APICURIO_URL=http://localhost:8081 \
KAFKA_TOPIC=orders-new \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'

banner "6. ccompat export → Confluent IMPORT"
APICURIO_CC="$APICURIO_CC" CONFLUENT_SR="$CONFLUENT_SR" \
  bash scripts/migrate-schema.sh
bash scripts/show-ids.sh orders-value

banner "7. Replicate both topics byte-for-byte (Cluster Linking stand-in)"
copy_topic() {
  local t="$1"
  echo "  Copying source '$t' → dest '$t' ..."
  SOURCE_BOOTSTRAP="$SOURCE_BS" \
  DEST_BOOTSTRAP="$DEST_BS" \
  KAFKA_TOPIC="$t" \
    java -jar "$APP_JAR" mirror-topic 2>&1 | grep -v '^SLF4J'
}
copy_topic orders
copy_topic orders-new

banner "8. Verify framing + ID resolution on dest"
verify_exit=0
SOURCE_BOOTSTRAP="$SOURCE_BS" \
DEST_BOOTSTRAP="$DEST_BS" \
APICURIO_CCOMPAT="$APICURIO_CC" \
SCHEMA_REGISTRY_URL="$CONFLUENT_SR" \
TOPIC=orders \
  java -jar "$APP_JAR" verify-ids 2>&1 | grep -v '^SLF4J' || verify_exit=$?

if [[ $verify_exit -eq 0 ]]; then
  echo "  verify-ids PASSED"
else
  echo "  verify-ids FAILED (exit=$verify_exit)" >&2
fi

banner "9. Consume with ApicurioPayloadDeserializer"
shim_orders_exit=0
echo "--- topic: orders ---"
DEST_BOOTSTRAP="$DEST_BS" \
SCHEMA_REGISTRY_URL="$CONFLUENT_SR" \
DEST_TOPIC=orders \
CONSUMER_GROUP=cflt-shim-orders \
  java -jar "$APP_JAR" consume-cflt-shim 2>&1 | grep -v '^SLF4J' || shim_orders_exit=$?

shim_new_exit=0
echo "--- topic: orders-new ---"
DEST_BOOTSTRAP="$DEST_BS" \
SCHEMA_REGISTRY_URL="$CONFLUENT_SR" \
DEST_TOPIC=orders-new \
CONSUMER_GROUP=cflt-shim-orders-new \
  java -jar "$APP_JAR" consume-cflt-shim 2>&1 | grep -v '^SLF4J' || shim_new_exit=$?

banner "10. Verdict"
echo "  verify-ids:              exit=$verify_exit"
echo "  consume-cflt-shim orders: exit=$shim_orders_exit"
echo "  consume-cflt-shim orders-new: exit=$shim_new_exit"

if [[ $verify_exit -eq 0 && $shim_orders_exit -eq 0 && $shim_new_exit -eq 0 ]]; then
  echo
  echo "RESULT: PASS"
  echo "  ApicurioPayloadDeserializer decoded 8-byte payload records on both topics"
  echo "  after byte-for-byte copy (no SMT)."
else
  echo
  echo "RESULT: FAIL"
  [[ $verify_exit -ne 0 ]]       && echo "  - verify-ids failed (exit=$verify_exit)"
  [[ $shim_orders_exit -ne 0 ]]  && echo "  - shim failed on 'orders' (exit=$shim_orders_exit)"
  [[ $shim_new_exit -ne 0 ]]     && echo "  - shim failed on 'orders-new' (exit=$shim_new_exit)"
  exit 1
fi
