package io.ibm.migration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** Avro schema helper and record factory for the Order type. */
public final class OrderSchema {

    /** Primary topic written by the v2-serde producer. */
    public static final String TOPIC = "orders";

    /** Additional topic produced after the Apicurio v2→v3 registry upgrade. */
    public static final String TOPIC_NEW = "orders-new";

    /** Confluent Schema Registry subject name for both topics (same schema). */
    public static final String SUBJECT = "orders-value";

    private static final Schema SCHEMA = load();

    private OrderSchema() {}

    public static Schema schema() { return SCHEMA; }

    public static GenericRecord order(String orderId, String customer, double amount) {
        GenericRecord r = new GenericData.Record(SCHEMA);
        r.put("orderId",  orderId);
        r.put("customer", customer);
        r.put("amount",   amount);
        return r;
    }

    private static Schema load() {
        try (InputStream in = OrderSchema.class.getClassLoader().getResourceAsStream("order.avsc")) {
            if (in == null) {
                return new Schema.Parser().parse(
                        "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"io.ibm.migration\","
                        + "\"fields\":["
                        + "{\"name\":\"orderId\",\"type\":\"string\"},"
                        + "{\"name\":\"customer\",\"type\":\"string\"},"
                        + "{\"name\":\"amount\",\"type\":\"double\"}]}");
            }
            return new Schema.Parser().parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load order.avsc", e);
        }
    }
}
