package io.ibm.migration.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Byte-for-byte topic mirror that preserves Kafka headers.
 *
 * <p>Stand-in for Cluster Linking (Confluent enterprise). Reads every record from the
 * source topic partition 0 from the beginning and produces it to the destination topic,
 * copying both the payload bytes and all Kafka headers unchanged.
 *
 * <p>Env vars:
 * <ul>
 *   <li>{@code SOURCE_BOOTSTRAP} — source Kafka (default {@code localhost:29092})</li>
 *   <li>{@code DEST_BOOTSTRAP}   — dest Kafka (default {@code localhost:29094})</li>
 *   <li>{@code KAFKA_TOPIC}      — topic name on both clusters (default {@code orders})</li>
 * </ul>
 */
public final class TopicMirror {

    public static void run() throws Exception {
        String srcBs  = env("SOURCE_BOOTSTRAP", "localhost:29092");
        String dstBs  = env("DEST_BOOTSTRAP",   "localhost:29094");
        String topic  = env("KAFKA_TOPIC",       "orders");

        System.out.printf("mirror: %s/%s → %s/%s (headers preserved)%n",
                srcBs, topic, dstBs, topic);

        // Consumer — raw bytes, partition 0, from beginning
        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        srcBs);
        cProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Producer — raw bytes
        Properties pProps = new Properties();
        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      dstBs);
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pProps.put(ProducerConfig.ACKS_CONFIG,                   "all");

        int copied = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cProps);
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(pProps)) {

            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long end = consumer.endOffsets(List.of(tp)).get(tp);

            if (end == 0) {
                System.out.println("  Source topic is empty — nothing to copy.");
                return;
            }

            long deadline = System.currentTimeMillis() + 60_000;
            while (consumer.position(tp) < end && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofSeconds(2))) {
                    ProducerRecord<byte[], byte[]> out =
                            new ProducerRecord<>(topic, null, r.key(), r.value());
                    // Copy all headers byte-for-byte
                    for (Header h : r.headers()) {
                        out.headers().add(h);
                    }
                    producer.send(out).get();
                    copied++;
                }
            }
            producer.flush();
        }
        System.out.printf("  Copied %d record(s) from %s → %s (headers preserved).%n",
                copied, srcBs, dstBs);
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
