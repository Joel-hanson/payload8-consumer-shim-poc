package io.ibm.migration.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.ibm.migration.OrderSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Post-migration consumer for Apicurio 2.x default-config header-mode records after Cluster Linking.
 *
 * <p>Uses {@link ApicurioHeaderShimDeserializer} as the value deserializer.  The shim reads the
 * {@code apicurio.value.globalId} header (written by the Apicurio 2.x serde with default settings),
 * prepends the Confluent 5-byte wire-format prefix ({@code 0x00} + 4-byte int schema ID), and
 * delegates to {@code KafkaAvroDeserializer} for actual deserialization.
 *
 * <p>Env vars:
 * <ul>
 *   <li>{@code DEST_BOOTSTRAP}      — Confluent-side bootstrap (default {@code localhost:29094})</li>
 *   <li>{@code SCHEMA_REGISTRY_URL} — Confluent Schema Registry URL (default {@code http://localhost:8082})</li>
 *   <li>{@code DEST_TOPIC}          — topic to consume (default {@code orders})</li>
 *   <li>{@code CONSUMER_GROUP}      — consumer group ID (default {@code cflt-shim-consumer})</li>
 * </ul>
 */
public final class ConfluentShimConsumer {

    public static void run() {
        String bootstrap = env("DEST_BOOTSTRAP",      "localhost:29094");
        String registry  = env("SCHEMA_REGISTRY_URL", "http://localhost:8082");
        String topic     = env("DEST_TOPIC",           OrderSchema.TOPIC);
        String group     = env("CONSUMER_GROUP",       "cflt-shim-consumer");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        // Shim: detects 8-byte Apicurio payload prefix → reframes to Confluent 4-byte prefix.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ApicurioPayloadDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            System.out.printf(
                    "Listening on dest topic '%s' with ApicurioPayloadDeserializer shim ...%n", topic);
            long deadline = System.currentTimeMillis() + 45_000;
            int seen = 0;
            while (System.currentTimeMillis() < deadline && seen < 30) {
                for (ConsumerRecord<String, GenericRecord> r : consumer.poll(Duration.ofSeconds(2))) {
                    seen++;
                    System.out.printf(
                            "  shim-consume topic=%s offset=%d key=%s value=%s%n",
                            r.topic(), r.offset(), r.key(), r.value());
                }
            }
            if (seen == 0) {
                System.err.printf(
                        "FAILED: no records consumed from topic '%s'. "
                        + "Check schema IMPORT, topic replication, and DEST_TOPIC env var.%n", topic);
                System.exit(2);
            }
            System.out.printf(
                    "SUCCESS: shim consumer read %d record(s) from '%s' "
                    + "(8-byte payload prefix reframed to Confluent 4-byte, no SMT needed).%n",
                    seen, topic);
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
