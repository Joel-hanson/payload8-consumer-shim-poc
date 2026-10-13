package io.ibm.migration.producer;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.IdOption;
import io.ibm.migration.OrderSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Producer that uses the Apicurio 2.x serde with {@code ENABLE_HEADERS=false}.
 *
 * <p>With no explicit {@code ID_HANDLER} set, the 2.x serde uses its built-in
 * {@code DefaultIdHandler}, which writes the schema ID as a 9-byte prefix:
 * <pre>
 *   byte[0]    = 0x00  (magic byte)
 *   byte[1..8] = schema ID as an 8-byte big-endian long
 *   byte[9..]  = raw Avro binary body
 * </pre>
 *
 * <p>Env vars:
 * <ul>
 *   <li>{@code SOURCE_BOOTSTRAP} — Kafka bootstrap (default {@code localhost:29092})</li>
 *   <li>{@code APICURIO_URL}     — Apicurio base URL (default {@code http://localhost:8081})</li>
 *   <li>{@code KAFKA_TOPIC}      — topic to produce to (default {@code orders})</li>
 * </ul>
 */
public final class ApicurioPayloadProducer {

    public static void run(int count) throws Exception {
        String bootstrap = env("SOURCE_BOOTSTRAP", "localhost:29092");
        String registry  = env("APICURIO_URL",     "http://localhost:8081");
        String topic     = env("KAFKA_TOPIC",       OrderSchema.TOPIC);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(SerdeConfig.REGISTRY_URL,                     registry + "/apis/registry/v2");
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT,           "true");
        // Payload mode: ID written into the message payload, not a Kafka header.
        props.put(SerdeConfig.ENABLE_HEADERS,                   "false");
        // contentId is the ID type the ccompat export uses by default (LEGACY_ID_MODE=false).
        props.put(SerdeConfig.USE_ID,                           IdOption.contentId.name());
        // No ID_HANDLER set → DefaultIdHandler → 0x00 + 8-byte big-endian long.
        props.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY,
                "io.apicurio.registry.serde.strategy.TopicIdStrategy");

        System.out.printf("producer: topic=%s bootstrap=%s registry=%s%n"
                + "  ENABLE_HEADERS=false  USE_ID=contentId  ID_HANDLER=(serde default = 8-byte)%n",
                topic, bootstrap, registry);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                GenericRecord order = OrderSchema.order("ORD-" + i, "customer-" + i, 10.0 * i);
                ProducerRecord<String, GenericRecord> record =
                        new ProducerRecord<>(topic, "key-" + i, order);
                var meta = producer.send(record).get(30, TimeUnit.SECONDS);
                System.out.printf(
                        "  produced topic=%s partition=%d offset=%d orderId=%s%n",
                        meta.topic(), meta.partition(), meta.offset(), order.get("orderId"));
            }
            producer.flush();
        }
        System.out.printf("Done. %d record(s) produced to topic '%s'.%n", count, topic);
        System.out.println("  Payload framing: 0x00 + 8-byte-long schema ID + Avro body");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
