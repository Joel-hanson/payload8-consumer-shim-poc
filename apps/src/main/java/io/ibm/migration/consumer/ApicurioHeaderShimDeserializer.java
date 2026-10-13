package io.ibm.migration.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Consumer-side shim for the Apicurio 2.x serde default configuration after Cluster Linking.
 *
 * <h2>Apicurio 2.x serde defaults (nothing configured explicitly)</h2>
 * <pre>
 *   ENABLE_HEADERS = true   (SerdeConfig.ENABLE_HEADERS_DEFAULT)
 *   USE_ID         = globalId
 *   ID_HANDLER     = DefaultIdHandler (only active when ENABLE_HEADERS=false)
 * </pre>
 *
 * <p>With these defaults the producer writes:
 * <ul>
 *   <li>Kafka header {@code apicurio.value.globalId} = the schema ID as an <b>8-byte big-endian long</b></li>
 *   <li>Kafka payload = <b>raw Avro binary — no magic byte, no ID prefix</b></li>
 * </ul>
 *
 * <p>Cluster Linking copies both headers and payload bytes unchanged to the Confluent-side topic.
 *
 * <h2>Why the standard Confluent deserializer fails</h2>
 * <p>{@code KafkaAvroDeserializer} expects the payload to start with magic byte {@code 0x00}.
 * Because the payload is raw Avro binary there is no {@code 0x00} prefix, so Confluent throws
 * {@code SerializationException: Unknown magic byte!} on every record.
 *
 * <h2>The fix: header-to-prefix reframe</h2>
 * <p>This shim overrides {@code deserialize(topic, headers, data)}:
 * <ol>
 *   <li>Looks for the {@code apicurio.value.globalId} header (8-byte big-endian long).</li>
 *   <li>Narrows the 8-byte long to a 4-byte int (safe: real schema IDs are well below 2^32).</li>
 *   <li>Prepends the Confluent 5-byte prefix ({@code 0x00} + 4-byte int) to the raw Avro body.</li>
 *   <li>Delegates to the inner {@code KafkaAvroDeserializer} with the reframed bytes.</li>
 * </ol>
 * <p>Records that already carry a {@code 0x00} magic byte in the payload (e.g. produced by a
 * Confluent serde or Apicurio 3.x serde) are passed through unchanged via the 2-arg fallback.
 *
 * <h2>Safety</h2>
 * <p>Schema IDs start at 1 and increment. Reaching 2^32 (≈ 4 billion) would require billions of
 * schema registrations — impossible in any real deployment. Narrowing from {@code long} to
 * {@code int} is therefore always safe, and IDs in the range [1, 2^31−1] are handled correctly
 * as signed Java ints because {@code KafkaAvroDeserializer} reads bytes [1..4] as a signed int
 * and schema IDs in that range are positive.
 *
 * <h2>Configuration</h2>
 * <p>Drop-in replacement for {@code KafkaAvroDeserializer}. All {@code schema.registry.*}
 * properties are forwarded unchanged.
 *
 * <pre>{@code
 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
 *     ApicurioHeaderShimDeserializer.class.getName());
 * props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *     "https://schema-registry.confluent.svc:8081");
 * }</pre>
 */
public final class ApicurioHeaderShimDeserializer implements Deserializer<Object> {

    /** Header written by Apicurio 2.x serde with USE_ID=globalId (the 2.x default). */
    static final String GLOBAL_ID_HEADER = "apicurio.value.globalId";

    private static final byte MAGIC = 0x00;

    private final KafkaAvroDeserializer inner = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    /**
     * Primary entry point — called by {@code KafkaConsumer} on all modern Kafka client versions.
     * Extracts the schema ID from the Apicurio header, prepends the Confluent 5-byte prefix,
     * and delegates to {@code KafkaAvroDeserializer}.
     */
    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return inner.deserialize(topic, reframe(headers, data));
    }

    /**
     * Fallback for callers that do not supply headers (e.g. unit tests, older client paths).
     * If the payload already starts with {@code 0x00} it is already Confluent format; otherwise
     * deserialization will fail with the standard Confluent error, which is the correct behaviour
     * (no headers = no way to find the schema ID).
     */
    @Override
    public Object deserialize(String topic, byte[] data) {
        return inner.deserialize(topic, data);
    }

    @Override
    public void close() {
        inner.close();
    }

    /**
     * Reframe an Apicurio header-mode record to Confluent wire format.
     *
     * <p>If the {@code apicurio.value.globalId} header is present:
     * <pre>
     *   result = 0x00 + (int) globalId (4 bytes) + data (entire raw Avro body)
     * </pre>
     *
     * <p>If the header is absent but the payload starts with {@code 0x00} (already Confluent
     * format), return {@code data} unchanged.
     *
     * <p>If neither applies, return {@code data} unchanged (the inner deserializer will fail
     * with a clear error).
     */
    public static byte[] reframe(Headers headers, byte[] data) {
        if (headers != null) {
            Header h = headers.lastHeader(GLOBAL_ID_HEADER);
            if (h != null && h.value() != null && h.value().length == 8) {
                long globalId = ByteBuffer.wrap(h.value()).getLong();
                if (globalId < 0 || globalId > Integer.MAX_VALUE) {
                    throw new IllegalStateException(
                            "Schema globalId " + globalId + " does not fit in Confluent's 4-byte wire ID");
                }
                // Prepend 5-byte Confluent prefix: magic + 4-byte int schema ID.
                ByteBuffer out = ByteBuffer.allocate(5 + (data == null ? 0 : data.length));
                out.put(MAGIC);
                out.putInt((int) globalId);
                if (data != null) {
                    out.put(data);
                }
                return out.array();
            }
        }
        // No Apicurio header — return as-is (pass-through for already-Confluent records).
        return data;
    }
}
