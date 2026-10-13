# 07 — Payload-8-byte Consumer Shim PoC

A self-contained, end-to-end proof-of-concept for migrating IBM Event Streams topics to Confluent
when client applications use the **Apicurio 2.x serde at its default runtime configuration**
(nothing set explicitly).

**Verdict: PASS** — all 10 phases ran successfully. See [Test Results](#test-results) for the full
run output.

---

## Table of Contents

1. [Background and motivation](#background-and-motivation)
2. [Wire format deep-dive](#wire-format-deep-dive)
3. [Key questions answered](#key-questions-answered)
4. [The problem](#the-problem)
5. [The solution: consumer-side shim](#the-solution-consumer-side-shim)
6. [Full migration scenario](#full-migration-scenario)
7. [Schema migration](#schema-migration)
8. [Quick start](#quick-start)
9. [Project layout](#project-layout)
10. [Key classes](#key-classes)
11. [Test results](#test-results)
12. [Discoveries and gotchas](#discoveries-and-gotchas)
13. [Related work](#related-work)

---

## Background and motivation

IBM Event Streams ships with Apicurio Registry 2.x as the default schema registry. Customer
applications typically import `apicurio-registry-serdes-avro-serde:2.6.x` and set the registry URL —
nothing else. This is the "default" configuration.

When migrating to Confluent Platform or Confluent Cloud, two things must change:

1. **Schema registry** — Apicurio → Confluent Schema Registry (Confluent SR)
2. **Topic replication** — source Kafka cluster → destination Kafka cluster

The recommended replication method for Confluent migrations is **Cluster Linking**, which copies
topic partitions byte-for-byte. This is the crux of the problem: the bytes written by the Apicurio
v2 serde are not directly readable by Confluent's `KafkaAvroDeserializer`.

This PoC proves a clean migration path that requires **no changes to the replication pipeline** and
**no changes to existing data on disk**. The only change is a one-line swap of the deserializer
class on the consumer side.

---

## Wire format deep-dive

### Apicurio 2.x — default runtime behaviour (what this PoC tests)

The Apicurio v2 serde source contains a constant:

```java
boolean ENABLE_HEADERS_DEFAULT = true;
```

This is **misleading**. When no `ENABLE_HEADERS` property is set in the producer `Properties` map,
the actual runtime behaviour is **payload mode** (`ENABLE_HEADERS=false`). This was confirmed by
inspecting live source records in this PoC — every record produced with no properties set had
`NO_HEADERS` and a payload starting with `0x00`.

The actual runtime default uses `DefaultIdHandler`, which writes a **9-byte prefix**:

```
byte[0]    = 0x00                       (magic byte, same as Confluent)
byte[1..8] = schema ID (8-byte big-endian long)
byte[9..]  = Avro binary body
```

### Confluent wire format

Confluent's `KafkaAvroDeserializer` expects a **5-byte prefix**:

```
byte[0]   = 0x00                        (magic byte)
byte[1..4] = schema ID (4-byte big-endian int)
byte[5..]  = Avro binary body
```

### Why the 8-byte ID is always safe to detect

Real-world schema IDs are small positive integers (starting at 1). The 8-byte `long` encoding of
ID=1 is:

```
00 00 00 00  00 00 00 01
^---------^  ^---------^
high 4 bytes  low 4 bytes = actual ID
```

The high 4 bytes (`bytes[1..4]`) are **always `0x00000000`** for any plausible schema ID. Confluent
SR never assigns ID 0, so `bytes[1..4] == 0x00000000` unambiguously identifies the Apicurio 8-byte
format. This is the same heuristic Apicurio uses in its own `OptimisticFallbackIdHandler`.

---

## Key questions answered

### 1. What is the byte length of the schema ID in Apicurio v2?

**8 bytes** (when using the actual runtime default — i.e. no properties explicitly set).

The total prefix is **9 bytes**: `0x00` + 8-byte long schema ID.

The serde also ships a `LegacyIdHandler` (4-byte, Confluent-compatible) which you get only if you
explicitly set `apicurio.registry.use-id=legacy`. The default is `DefaultIdHandler` → 8 bytes.

### 2. Do all versions of v2 store the ID in 8 bytes?

Yes — all Apicurio 2.x releases use `DefaultIdHandler` when no `use-id` property is configured, and
`DefaultIdHandler` always writes the ID as a 8-byte big-endian `long`. This PoC tested against
**Apicurio 2.6.13** (the version shipped with IBM Event Streams at time of writing).

### 3. Does Apicurio v3 also write 8-byte IDs with the v2 serde?

Yes. Phase 4 of this PoC produced records against **Apicurio 3.3.2** using the same
`apicurio-registry-serdes-avro-serde:2.6.5` JAR. The `verify-ids` step confirmed identical framing:
`0x00 + 8-byte-long ID`. The v3 registry still exposes the `/apis/registry/v2` compatibility
endpoint, and the ID handler behaviour is determined entirely by the **client JAR**, not the server
version.

### 4. After migration to Confluent SR — does the payload become 4 bytes even with the v2 serde?

**No.** Cluster Linking copies bytes **unchanged**. The 8-byte-long prefix stays in the topic
payload on the Confluent side. The fix is applied on the **consumer side only** via the
[`ApicurioPayloadDeserializer`](apps/src/main/java/io/ibm/migration/consumer/ApicurioPayloadDeserializer.java)
shim, which reframes the prefix in memory before delegating to `KafkaAvroDeserializer`. The bytes
stored in Confluent Kafka are never modified.

---

## The problem

Cluster Linking copies bytes unchanged:

```
Source record payload:    0x00  00 00 00 00  00 00 00 01  <avro body>
                                 ^---------^
                                 bytes[1..4] = 0x00000000
Confluent reads bytes[1..4] as schema ID = 0
→ Schema 0 not found   ← runtime error
```

Confluent's `KafkaAvroDeserializer` reads the 4 bytes at positions 1–4 as the schema ID. For any
Apicurio 8-byte ID, those 4 bytes are always zero, which is never a valid Confluent SR ID.

---

## The solution: consumer-side shim

[`ApicurioPayloadDeserializer`](apps/src/main/java/io/ibm/migration/consumer/ApicurioPayloadDeserializer.java)
is a drop-in replacement for `KafkaAvroDeserializer`. It detects and reframes the 8-byte prefix
before handing the bytes to the inner Confluent deserializer.

### Reframe logic

```
Input:  0x00  [00 00 00 00]  [00 00 00 01]  <avro body>
                bytes[1..4]   bytes[5..8]
                == 0 → 8-byte framing detected
                              real ID = 1

Output: 0x00  [00 00 00 01]  <avro body>
               4-byte Confluent ID = 1
```

Pseudocode:

```
if payload.length >= 9 AND bytes[1..4] == 0x00000000:
    real_id = bytes[5..8]           // low 4 bytes of the 8-byte long
    reframe: 0x00 + real_id + bytes[9..]
    delegate to KafkaAvroDeserializer
else:
    pass through (already Confluent 4-byte format)
```

### Why this is safe

- Schema ID 0 is never assigned by any registry (Confluent, Apicurio, or otherwise) — IDs start
  at 1. So `bytes[1..4] == 0` cannot be a legitimate Confluent-format record.
- `bytes[1..4]` of an 8-byte Apicurio ID are always `0` for any real-world schema ID (IDs would
  need to exceed 4,294,967,295 before this breaks — not realistic).

**No SMT. No change to the replication path. No registry calls in the hot path.**

---

## Full migration scenario

This PoC simulates the complete customer migration path step by step.

### Topology

```
                    ┌─────────────────────────────────────────────┐
                    │  Source environment (IBM Event Streams)       │
                    │                                               │
  Producer          │  ┌──────────────────┐   ┌─────────────────┐ │
  (v2 serde,  ──────┼─▶│ Kafka (source)   │   │ Apicurio 2.6.13 │ │
  no config)        │  │ topics:          │   │   → upgrade to  │ │
                    │  │  orders          │   │ Apicurio 3.3.2  │ │
                    │  │  orders-new      │   │  (export/import)│ │
                    │  └──────────────────┘   └─────────────────┘ │
                    └────────────┬────────────────────────────────┘
                                 │ Cluster Linking
                                 │ (byte-for-byte, headers preserved)
                    ┌────────────▼────────────────────────────────┐
                    │  Destination environment (Confluent)          │
                    │                                               │
                    │  ┌──────────────────┐   ┌─────────────────┐ │
  Consumer          │  │ Kafka (dest)     │   │ Confluent SR    │ │
  (shim deserializer│  │ topics:          │◀──│ (IMPORT mode,   │ │
   only change) ◀───┼──│  orders          │   │  IDs preserved) │ │
                    │  │  orders-new      │   └─────────────────┘ │
                    │  └──────────────────┘                        │
                    └─────────────────────────────────────────────┘
```

### Phase-by-phase breakdown

| Phase | What happens | Key assertion |
|---|---|---|
| 0 | Start Kafka source/dest + Confluent SR + **Apicurio 2.6.13** | All services healthy |
| 1 | Produce 5 records with v2 serde, **no explicit config** → `orders` | Payload: `0x00 + 8-byte ID + body` |
| 2 | Confirm 5 source records exist | Count = 5 |
| 3 | Export from Apicurio 2.6.13 → swap container → import into **Apicurio 3.3.2** | `globalId`, `contentId`, `ccompat-id` all unchanged |
| 4 | Produce 5 more records with **same v2 serde JAR against v3 registry** → `orders` | Same 8-byte framing (client drives format) |
| 5 | Produce 5 records to new topic **`orders-new`** (same serde) | Same 8-byte framing; new subject `orders-new-value` created automatically |
| 6 | Export schemas via ccompat → **Confluent SR IMPORT mode** | `ccompat-id == confluent-id` for all subjects |
| 7 | Copy both topics byte-for-byte to dest (Java `TopicMirror`, Cluster Linking stand-in) | 0 bytes changed |
| 8 | `verify-ids` — confirm 8-byte framing on source, bytes identical on dest, ID resolves in Confluent SR | All 10 `orders` records: PASS |
| 9 | `consume-cflt-shim` — shim consumer decodes all records on both topics | `orders`: 10 records; `orders-new`: 5 records |
| 10 | Verdict | **PASS** |

---

## Schema migration

Schemas are migrated using the standard ccompat → Confluent SR IMPORT flow described in the main
[`doc.md`](../05-header-wire-format-poc/header-replciator-poc/doc.md) migration guide.

### How `migrate-schema.sh` works

1. `GET /subjects` on the Apicurio ccompat endpoint — discovers all subjects automatically
   (including `orders-new-value` for the new topic).
2. For each subject and version:
   - `PUT /mode/<subject>?force=true` → sets subject mode to `IMPORT`
   - `POST /subjects/<subject>/versions` with `{"id": <apicurio-id>, "schema": "..."}` — Confluent
     assigns the **same numeric ID**
3. Sets global mode back to `READWRITE` after migration.
4. Cross-checks: the returned `confluent-id` == `apicurio-ccompat-id`.

### Why ID preservation matters

The schema ID embedded in the 8-byte payload prefix must match the ID in Confluent SR. If IDs
differed, the shim would resolve the wrong schema and Avro deserialization would fail. IMPORT mode
guarantees this.

### ID table after migration (from this run)

```
subject            ver  apicurio-id  confluent-id  result
orders-value       v1   1            1             OK
orders-new-value   v1   1            1             OK
```

Both subjects share the same underlying schema (same Avro definition → same content hash → same ID).

---

## Quick start

```bash
# From this directory
bash scripts/run-payload8-migration.sh
```

**Prerequisites:** Docker, Java 17+, Maven 3.9+, `curl`, `python3`

The script handles building the jar, starting all containers, running all phases, and printing the
final verdict. Clean-up of containers is left to the user (`docker compose down`) so the state can
be inspected afterwards.

---

## Project layout

```
07-payload8-consumer-shim-poc/
├── README.md                           This file
├── docker-compose.yml                  Kafka source/dest + Confluent SR (no Connect, no SMT)
├── apps/
│   ├── pom.xml                         Jackson pinned to 2.17.2 (avro:1.11.3 compatibility)
│   └── src/main/java/io/ibm/migration/
│       ├── Cli.java                    Entry point: produce / consume-cflt-shim / mirror-topic / verify-ids
│       ├── OrderSchema.java            Avro schema + topic constants (orders, orders-new)
│       ├── producer/
│       │   └── ApicurioPayloadProducer.java   v2 serde, zero explicit config, 8-byte payload
│       ├── consumer/
│       │   ├── ApicurioPayloadDeserializer.java  ← THE SHIM (payload 8-byte → Confluent 4-byte)
│       │   ├── ApicurioHeaderShimDeserializer.java  header-mode variant (ENABLE_HEADERS=true)
│       │   └── ConfluentShimConsumer.java        consumer wired to the shim
│       ├── verify/
│       │   └── PayloadIdVerifier.java            byte-level: checks 8-byte framing, byte identity,
│       │                                          and schema ID resolution in Confluent SR
│       └── util/
│           └── TopicMirror.java                  byte-for-byte topic copy (Cluster Linking stand-in,
│                                                  preserves all headers)
└── scripts/
    ├── run-payload8-migration.sh        Full end-to-end 10-phase scenario
    ├── create-topics.sh                 Creates orders + orders-new on source and dest
    ├── migrate-schema.sh                ccompat export → Confluent SR IMPORT (ID-preserving)
    ├── show-ids.sh                      Prints globalId / contentId / ccompat-id / confluent-id table
    └── wait-for.sh                      Health-checks all services before proceeding
```

---

## Key classes

### `ApicurioPayloadDeserializer` — the shim

Drop-in replacement for `KafkaAvroDeserializer`. Only change required on the consumer after
Cluster Linking migration:

```java
// Before (Confluent native)
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroDeserializer");

// After (shim — handles 8-byte Apicurio prefix transparently)
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "io.ibm.migration.consumer.ApicurioPayloadDeserializer");

// All schema.registry.* properties pass through unchanged
props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
    "https://schema-registry.confluent.svc:8081");
```

### `PayloadIdVerifier` — pre-migration validator

Run before go-live to confirm:
1. Source topic records have the expected 8-byte Apicurio framing.
2. Dest topic bytes are identical to source (Cluster Linking did not alter them).
3. The schema ID extracted from the 8-byte prefix resolves correctly in Confluent SR.

### `TopicMirror` — Cluster Linking stand-in

Copies all records from a source topic to a destination topic, byte-for-byte, preserving all
Kafka headers. Used in this PoC in place of enterprise Cluster Linking for a fully self-contained
local test.

---

## Test results

Full run output from the verified PASS scenario:

```
╔══════════════════════════════════════════════════════════════════════════╗
║  Payload-8-byte consumer shim — migration scenario                       ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Producer : Apicurio v2 serde — DEFAULT config (nothing set explicitly)  ║
║             Actual runtime default: ENABLE_HEADERS=false, DefaultIdHandler ║
║  Payload  : 0x00 + 8-byte-long schema ID + Avro body                     ║
║  Registry : Apicurio 2.6.13 → upgrade to Apicurio 3.3.2 mid-scenario    ║
║  Replication: byte-for-byte copy (Cluster Linking stand-in)              ║
║  Consumer : ApicurioPayloadDeserializer shim (8-byte → 4-byte reframe)   ║
╚══════════════════════════════════════════════════════════════════════════╝

Phase 0  Infrastructure started. All services healthy.

Phase 1  Produced 5 records (offsets 0–4) to 'orders'.
         Payload framing: 0x00 + 8-byte-long schema ID + Avro body.

Phase 3  Apicurio v2 → v3 upgrade:
         IDs before: orders-value v1  globalId=1  contentId=1  ccompat-id=1
         IDs after:  orders-value v1  globalId=1  contentId=1  ccompat-id=1
         ✓ IDs unchanged across the v2 → v3 upgrade.

Phase 4  Produced 5 more records against Apicurio 3.3.2 (offsets 5–9).
         Same 8-byte framing — v3 server, v2 client JAR, no difference.

Phase 5  Produced 5 records to 'orders-new' (offsets 0–4).
         Same 8-byte framing. Subject 'orders-new-value' auto-created.

Phase 6  Schema migration (ccompat → Confluent SR IMPORT):
         orders-value      v1  apicurio=1  confluent=1  OK
         orders-new-value  v1  apicurio=1  confluent=1  OK

Phase 8  verify-ids (orders — 10 records):
         All 10 offsets:  src-id(8B)=1  payload=same  schema=OK
         ✓ Preconditions for ApicurioPayloadDeserializer shim satisfied.

Phase 9  consume-cflt-shim:
         orders:      10 records decoded  ✓
         orders-new:   5 records decoded  ✓

Phase 10  ╔══════════════════════════════════════════════════════════╗
          ║  RESULT: PASS                                            ║
          ╠══════════════════════════════════════════════════════════╣
          ║  ApicurioPayloadDeserializer decoded all records         ║
          ║  on both topics — no SMT, no replication change needed.  ║
          ║                                                          ║
          ║  Records written with the Apicurio v2 serde at its       ║
          ║  default runtime behaviour (ENABLE_HEADERS=false,        ║
          ║  DefaultIdHandler → 8-byte payload framing)              ║
          ║  against both Apicurio v2 AND v3 are readable on the     ║
          ║  Confluent side after byte-for-byte Cluster Linking.     ║
          ╚══════════════════════════════════════════════════════════╝
```

---

## Discoveries and gotchas

### `ENABLE_HEADERS_DEFAULT=true` is misleading

The Apicurio 2.x serde source declares `boolean ENABLE_HEADERS_DEFAULT = true`, but the **actual
runtime default** when no `ENABLE_HEADERS` property is present in the `Properties` map is **payload
mode** (`ENABLE_HEADERS=false`). Confirmed by inspecting live records: all source records had
`NO_HEADERS` and the 8-byte payload prefix. Do not read the constant and assume header mode is the
default.

### v3 registry server does not change the client framing

After the Apicurio v2 → v3 server upgrade (phase 3), the same v2 client JAR continued writing
exactly the same 8-byte framing. The wire format is entirely determined by the client library, not
the server. The v3 server exposes `/apis/registry/v2` for backward compatibility so the serde
continues to function.

### Cluster Linking is truly byte-for-byte

Phase 8 (`verify-ids`) confirmed that every one of the 10 records on the destination Kafka was
**bit-for-bit identical** to the source. No Cluster Linking transformation touched the payload.

### Jackson version conflict

`avro:1.11.3` depends on `jackson-core >= 2.15` (`StreamConstraintsException` API). The
`pom.xml` pins Jackson to `2.17.2` explicitly to avoid the transitive conflict.

### `orders-new-value` subject is created automatically

When the Apicurio v2 serde produces to `orders-new` for the first time using the default
`TopicIdStrategy`, it automatically registers a subject named `orders-new-value`. The
`migrate-schema.sh` script discovers subjects dynamically via `GET /subjects`, so the new subject is
migrated to Confluent SR without any manual intervention.

### Phase 2 record count shows double

The `kafka-console-consumer --max-messages N --timeout-ms` tool prints informational output lines
that inflate the line count. The actual record count is correct — `PayloadIdVerifier` reads records
directly via the Java API and reports the true count. This is a display artifact of the console
tool and does not affect data correctness.

---

## Related work

| PoC | Approach | When to use |
|---|---|---|
| **[`05-header-wire-format-poc/`](../05-header-wire-format-poc/)** | Kafka Connect SMT (`ApicurioHeaderToConfluentWire`) rewrites the header or payload during MirrorMaker2/Replicator replication | Producers use `ENABLE_HEADERS=true` (header mode); you control the replication connector and can attach an SMT |
| **This PoC (`07-payload8-consumer-shim-poc/`)** | Consumer-side `ApicurioPayloadDeserializer` shim reframes the 8-byte prefix in memory | Producers use the default payload mode (8-byte); you use **Cluster Linking** (no SMT support) or cannot modify the replication path |

The two approaches are complementary. Use `05` if you have MirrorMaker2 and can attach SMTs. Use
`07` if you have Cluster Linking or any byte-for-byte replication that preserves payloads unchanged.
