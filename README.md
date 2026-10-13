# 8-byte payload → Confluent (consumer shim PoC)

Apicurio 2.x in **payload mode** puts the schema ID in the message as an **8-byte** long. Confluent expects a **4-byte** ID. Cluster Linking copies bytes unchanged, so stock `KafkaAvroDeserializer` fails. This PoC fixes that with a **consumer-side shim** — no SMT, no rewrite of the topic.

## The problem

```text
Source (Apicurio 2.x payload mode)     Dest after Cluster Linking
──────────────────────────────────     ─────────────────────────
0x00 + 8-byte id + Avro                same bytes (unchanged)

Confluent expects:  0x00 + 4-byte id + Avro
```

For small IDs the high 4 bytes of the long are `0x00000000`. Confluent reads that as schema ID **0** → `Schema 0 not found`.

## The idea

```text
Producer (ENABLE_HEADERS=false, DefaultIdHandler)
        │
        ▼
  kafka-source  ──byte-for-byte copy──►  kafka-dest
         │         (Cluster Linking stand-in)     │
         │                                        ▼
    Apicurio 2→3                         ApicurioPayloadDeserializer
    then ccompat → Confluent IMPORT      (reframe 8-byte → 4-byte, then
                                          KafkaAvroDeserializer)
```

The shim checks whether bytes `[1..4]` are zero. If they are, it takes the ID from bytes `[5..8]` and rewrites the prefix to Confluent form. Already-Confluent records pass through unchanged.

## Defaults this demo uses

| Setting | Value |
|---|---|
| `ENABLE_HEADERS` | `false` (ID in payload) |
| IdHandler | serde default (`DefaultIdHandler` → 8-byte ID) |
| `USE_ID` | `contentId` (matches ccompat with legacy mode off) |
| `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` | `false` |
| Replication | byte-for-byte copy (no Connect SMT) |

## Prerequisites

- Docker + Docker Compose
- JDK 17+
- Maven 3.9+

## Run

```bash
cd payload8-consumer-shim-poc
chmod +x scripts/*.sh
./scripts/run-demo.sh
```

What happens:

1. Start source/dest Kafka + Confluent SR; start Apicurio **2.6.13**
2. Produce 8-byte-payload Avro to `orders`
3. Export Apicurio 2 → import into Apicurio **3.3.2** (IDs preserved)
4. Produce more records (same 2.x serde) to `orders` and `orders-new`
5. ccompat export → Confluent IMPORT (IDs preserved)
6. Copy both topics byte-for-byte to dest
7. Verify framing + ID resolution
8. Consume with `ApicurioPayloadDeserializer`

Success looks like: `SUCCESS: shim consumer read N record(s)` and a final `RESULT: PASS`.

## Ports

| Service | Host |
|---|---|
| Source Kafka | `localhost:29092` |
| Dest Kafka | `localhost:29094` |
| Apicurio | `http://localhost:8081` |
| Confluent Schema Registry | `http://localhost:8082` |

## Layout

| Path | Role |
|---|---|
| `apps/.../ApicurioPayloadDeserializer.java` | The shim |
| `apps/` | Produce / mirror / verify / consume helpers |
| `scripts/run-demo.sh` | End-to-end scenario |
| `docker-compose.yml` | Kafka + Confluent SR (Apicurio started by the demo script) |

## Cleanup

```bash
docker-compose down -v
docker rm -f apicurio 2>/dev/null || true
```
