package io.ibm.migration.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ibm.migration.OrderSchema;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Per-record byte-level verification of the Confluent-side payloads.
 *
 * <p>Source records (Apicurio 2.x serde, {@code ENABLE_HEADERS=false}, DefaultIdHandler):
 * <pre>
 *   byte[0]    = 0x00  (magic byte)
 *   byte[1..8] = schema ID as an 8-byte big-endian long  (DefaultIdHandler)
 *   byte[9..]  = raw Avro binary body
 * </pre>
 *
 * <p>Cluster Linking copies bytes unchanged.  Without the shim the Confluent deserializer
 * reads bytes [1..4] = {@code 0x00000000} = ID 0 → {@code Schema 0 not found}.
 *
 * <p>This verifier checks the preconditions for the shim:
 * <ol>
 *   <li>Source payload starts with {@code 0x00} + 8-byte long ID.</li>
 *   <li>Dest payload is byte-for-byte equal to the source (Cluster Linking did not modify it).</li>
 *   <li>The schema ID extracted from the source resolves in Confluent SR with the correct schema.</li>
 * </ol>
 *
 * <p>When these pass the {@link io.ibm.migration.consumer.ApicurioPayloadDeserializer} shim will
 * correctly reframe bytes [1..8] → bytes [1..4] + strip bytes [5..8] and delegate to
 * {@code KafkaAvroDeserializer}.
 *
 * <p>Env vars:
 * <ul>
 *   <li>{@code SOURCE_BOOTSTRAP}    (default {@code localhost:29092})</li>
 *   <li>{@code DEST_BOOTSTRAP}      (default {@code localhost:29094})</li>
 *   <li>{@code APICURIO_CCOMPAT}    (default {@code http://localhost:8081/apis/ccompat/v7})</li>
 *   <li>{@code SCHEMA_REGISTRY_URL} (default {@code http://localhost:8082})</li>
 *   <li>{@code TOPIC}               (default {@code orders})</li>
 * </ul>
 */
public final class PayloadIdVerifier {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    public static int run() throws Exception {
        String srcBootstrap    = env("SOURCE_BOOTSTRAP",    "localhost:29092");
        String dstBootstrap    = env("DEST_BOOTSTRAP",      "localhost:29094");
        String apicurioCcompat = env("APICURIO_CCOMPAT",    "http://localhost:8081/apis/ccompat/v7");
        String confluentSr     = env("SCHEMA_REGISTRY_URL", "http://localhost:8082");
        String topic           = env("TOPIC",               OrderSchema.TOPIC);

        System.out.printf("verify: topic=%s  source=%s  dest=%s%n", topic, srcBootstrap, dstBootstrap);
        System.out.println("  Source framing:  0x00 + 8-byte-long ID + Avro body (DefaultIdHandler)");
        System.out.println("  Dest framing:    identical bytes (Cluster Linking unchanged)");
        System.out.println("  Checking: ID from source bytes[1..8] resolves in Confluent SR,");
        System.out.println("            dest bytes == source bytes, schema cross-checked");

        Map<Long, ConsumerRecord<byte[], byte[]>> src = readAll(srcBootstrap, topic);
        Map<Long, ConsumerRecord<byte[], byte[]>> dst = readAll(dstBootstrap, topic);
        System.out.printf("source records=%d  dest records=%d%n", src.size(), dst.size());

        int failures = 0;
        if (dst.isEmpty()) {
            System.out.println("FAIL: no records on dest topic '" + topic + "'");
            return 1;
        }
        if (src.size() != dst.size()) {
            System.out.println("FAIL: record count differs (source=" + src.size()
                    + ", dest=" + dst.size() + ")");
            failures++;
        }

        Map<Integer, String> schemaCheckCache = new HashMap<>();
        Map<Integer, Integer> idCounts = new TreeMap<>();

        System.out.printf("%-6s %-10s %-14s %-10s %s%n",
                "offset", "key", "src-id(8B)", "payload", "schema check");

        for (var e : dst.entrySet()) {
            long offset = e.getKey();
            ConsumerRecord<byte[], byte[]> d = e.getValue();
            ConsumerRecord<byte[], byte[]> s = src.get(offset);
            String key = d.key() == null ? "null" : new String(d.key());

            if (s == null) {
                System.out.printf("%-6d %-10s FAIL: no source record at this offset%n", offset, key);
                failures++;
                continue;
            }
            if (!Arrays.equals(s.key(), d.key())) {
                System.out.printf("%-6d %-10s FAIL: key differs%n", offset, key);
                failures++;
                continue;
            }

            // ── Read ID from source 8-byte payload prefix ─────────────────────
            byte[] sp = s.value();
            Long expectedId = null;
            String problem = null;

            if (sp == null || sp.length < 9 || sp[0] != 0x00) {
                problem = "source payload too short or wrong magic byte (expected 0x00 + 8-byte long)";
            } else {
                expectedId = ByteBuffer.wrap(sp, 1, 8).getLong();
            }

            // ── Dest payload must be byte-for-byte identical ───────────────────
            String payloadCheck = "-";
            if (problem == null) {
                boolean same = Arrays.equals(sp, d.value());
                payloadCheck = same ? "same" : "DIFF";
                if (!same) {
                    problem = "dest payload differs from source (Cluster Linking modified bytes?)";
                }
            }

            // ── Schema ID must resolve in Confluent SR ─────────────────────────
            String schemaResult = "-";
            if (problem == null && expectedId != null) {
                int idInt = (int) (long) expectedId;
                schemaResult = schemaCheckCache.computeIfAbsent(idInt,
                        id -> checkSchema(apicurioCcompat, confluentSr, id));
                if (!schemaResult.startsWith("OK")) {
                    problem = schemaResult;
                }
                idCounts.merge(idInt, 1, Integer::sum);
            }

            System.out.printf("%-6d %-10s %-14s %-10s %s%n",
                    offset, key,
                    expectedId == null ? "-" : expectedId,
                    payloadCheck,
                    problem == null ? schemaResult : "FAIL: " + problem);
            if (problem != null) {
                failures++;
            }
        }

        System.out.println();
        System.out.println("Schema IDs seen (src 8-byte ID → record count): " + idCounts);
        if (failures == 0) {
            System.out.println("Verification PASSED: source has 8-byte framing, "
                    + "dest bytes unchanged, schema ID resolves in Confluent SR.");
            System.out.println("  → ApicurioPayloadDeserializer shim can decode these records.");
            return 0;
        }
        System.out.println("Verification FAILED: " + failures + " problem(s).");
        return 1;
    }

    private static String checkSchema(String apicurioCcompat, String confluentSr, int id) {
        try {
            HttpResponse<String> c = get(confluentSr + "/schemas/ids/" + id);
            if (c.statusCode() != 200) {
                return "Confluent /schemas/ids/" + id + " → HTTP " + c.statusCode();
            }
            HttpResponse<String> a = get(apicurioCcompat + "/schemas/ids/" + id);
            if (a.statusCode() != 200) {
                return "Apicurio ccompat /schemas/ids/" + id + " → HTTP " + a.statusCode();
            }
            Schema cs = new Schema.Parser().parse(JSON.readTree(c.body()).get("schema").asText());
            Schema as = new Schema.Parser().parse(JSON.readTree(a.body()).get("schema").asText());
            if (!cs.equals(as)) {
                return "schema for id=" + id + " differs between Confluent and Apicurio";
            }
            HttpResponse<String> subj = get(confluentSr + "/schemas/ids/" + id + "/versions");
            String where = "";
            if (subj.statusCode() == 200) {
                StringBuilder sb = new StringBuilder();
                for (JsonNode n : JSON.readTree(subj.body())) {
                    sb.append(sb.length() == 0 ? "" : ",")
                      .append(n.get("subject").asText()).append(" v").append(n.get("version").asInt());
                }
                where = " [" + sb + "]";
            }
            return "OK schema matches" + where;
        } catch (Exception ex) {
            return "schema lookup error id=" + id + ": " + ex.getMessage();
        }
    }

    private static HttpResponse<String> get(String url) throws Exception {
        return HTTP.send(
                HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static Map<Long, ConsumerRecord<byte[], byte[]>> readAll(String bootstrap, String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        Map<Long, ConsumerRecord<byte[], byte[]>> out = new TreeMap<>();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(List.of(tp));
            c.seekToBeginning(List.of(tp));
            long end = c.endOffsets(List.of(tp)).get(tp);
            long deadline = System.currentTimeMillis() + 30_000;
            while (c.position(tp) < end && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofSeconds(1))) {
                    out.put(r.offset(), r);
                }
            }
        }
        return out;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
