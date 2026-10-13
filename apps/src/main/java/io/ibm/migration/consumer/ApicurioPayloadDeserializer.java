package io.ibm.migration.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Consumer-side shim that transparently handles the Apicurio 2.x default payload framing
 * after Cluster Linking replication — no changes to the replication path required.
 *
 * <h2>The problem</h2>
 * <p>A producer using the Apicurio 2.x serde with {@code ENABLE_HEADERS=false} and the default
 * {@code DefaultIdHandler} writes a 9-byte prefix:
 * <pre>
 *   byte[0]    = 0x00  (magic byte)
 *   byte[1..8] = schema ID as an 8-byte big-endian long
 *   byte[9..]  = raw Avro binary body
 * </pre>
 * Cluster Linking copies these bytes unchanged to the Confluent side.
 * The Confluent {@code KafkaAvroDeserializer} expects a 5-byte prefix:
 * <pre>
 *   byte[0]   = 0x00
 *   byte[1..4] = schema ID as a 4-byte big-endian int
 *   byte[5..]  = raw Avro binary body
 * </pre>
 * Because real-world schema IDs are small integers, the high 4 bytes of the 8-byte long are
 * always {@code 0x00 0x00 0x00 0x00}.  The Confluent deserializer reads those as ID&nbsp;0,
 * which no registry assigns, and throws {@code Schema 0 not found}.
 *
 * <h2>The fix: OptimisticFallback detection</h2>
 * <p>This class applies the same heuristic as Apicurio's own
 * {@code OptimisticFallbackIdHandler.readId()}:
 * <ol>
 *   <li>Read bytes [1..4] as a 4-byte int ({@code firstFour}).</li>
 *   <li>If {@code firstFour == 0} and there are at least 4 more bytes, reframe:
 *       {@code 0x00 + bytes[5..8] + bytes[9..]}.</li>
 *   <li>Otherwise pass through unchanged (already Confluent 4-byte format).</li>
 * </ol>
 *
 * <h2>Safety</h2>
 * <p>Schema ID&nbsp;0 is never assigned by any registry (IDs start at 1), so
 * {@code firstFour == 0} in a genuine Confluent-format record is impossible.
 * The heuristic also handles already-migrated records (3.x serde or
 * {@code Legacy4ByteIdHandler}) transparently — they have non-zero {@code firstFour}
 * and are passed through untouched.
 *
 * <h2>Configuration</h2>
 * <p>Drop-in replacement for {@code KafkaAvroDeserializer}.  Pass all
 * {@code schema.registry.*} properties unchanged.
 *
 * <pre>{@code
 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
 *     ApicurioPayloadDeserializer.class.getName());
 * props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *     "https://schema-registry.confluent.svc:8081");
 * }</pre>
 */
public final class ApicurioPayloadDeserializer implements Deserializer<Object> {

    private static final byte MAGIC = 0x00;

    private final KafkaAvroDeserializer inner = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return inner.deserialize(topic, reframe(data));
    }

    @Override
    public void close() {
        inner.close();
    }

    /**
     * Reframe an Apicurio 2.x 8-byte payload prefix to the Confluent 4-byte prefix.
     *
     * <p>8-byte layout (9-byte total prefix):
     * <pre>
     *   [0]      0x00 (magic)
     *   [1..4]   0x00 0x00 0x00 0x00  ← high 4 bytes of the long, always zero
     *   [5..8]   actual schema ID as a 4-byte big-endian int (low 4 bytes of the long)
     *   [9..]    Avro binary body
     * </pre>
     *
     * <p>4-byte layout (5-byte total prefix — Confluent standard, pass-through):
     * <pre>
     *   [0]      0x00 (magic)
     *   [1..4]   schema ID as a 4-byte big-endian int
     *   [5..]    Avro binary body
     * </pre>
     */
    public static byte[] reframe(byte[] data) {
        if (data == null || data.length < 5 || data[0] != MAGIC) {
            return data;
        }

        // Read bytes [1..4] — high 4 bytes of the 8-byte long in Apicurio framing.
        int firstFour = ByteBuffer.wrap(data, 1, 4).getInt();

        if (firstFour == 0 && data.length >= 9) {
            // 8-byte framing detected: bytes [5..8] hold the actual schema ID.
            // Rewrite to Confluent 5-byte prefix: 0x00 + bytes[5..8] + bytes[9..].
            ByteBuffer out = ByteBuffer.allocate(data.length - 4);
            out.put(MAGIC);
            out.put(data, 5, 4);                    // real schema ID
            out.put(data, 9, data.length - 9);      // Avro body unchanged
            return out.array();
        }

        // firstFour != 0 → already Confluent 4-byte format; pass through unchanged.
        return data;
    }
}
