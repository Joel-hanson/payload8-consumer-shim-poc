package io.ibm.migration;

import io.ibm.migration.consumer.ConfluentShimConsumer;
import io.ibm.migration.producer.ApicurioPayloadProducer;
import io.ibm.migration.util.TopicMirror;
import io.ibm.migration.verify.PayloadIdVerifier;

/**
 * CLI entry point.
 *
 * <pre>
 *   java -jar apps.jar produce [count]       Produce with Apicurio v2 serde defaults
 *   java -jar apps.jar consume-cflt-shim     Consume with ApicurioHeaderShimDeserializer
 *   java -jar apps.jar mirror-topic          Copy topic src→dest preserving headers (Cluster Linking stand-in)
 *   java -jar apps.jar verify-ids            Pre-condition check: header present, ID resolves, payload intact
 * </pre>
 */
public final class Cli {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }
        switch (args[0]) {
            case "produce" -> {
                int count = args.length > 1 ? Integer.parseInt(args[1]) : 5;
                ApicurioPayloadProducer.run(count);
            }
            case "consume-cflt-shim" -> ConfluentShimConsumer.run();
            case "mirror-topic"      -> TopicMirror.run();
            case "verify-ids"        -> System.exit(PayloadIdVerifier.run());
            default -> {
                usage();
                System.exit(1);
            }
        }
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  produce [count]       Produce with Apicurio v2 serde default config
                                        (ENABLE_HEADERS=true, USE_ID=globalId — serde built-in defaults)
                  consume-cflt-shim     Consume from Confluent using ApicurioHeaderShimDeserializer
                                        (reads globalId header, prepends Confluent prefix)
                  mirror-topic          Copy source topic to dest preserving headers byte-for-byte
                                        (stand-in for Cluster Linking; set KAFKA_TOPIC, SOURCE_BOOTSTRAP,
                                        DEST_BOOTSTRAP)
                  verify-ids            Pre-condition check: globalId header present on dest, ID resolves
                                        in Confluent SR, payload bytes unchanged
                """);
    }
}
