package io.ibm.migration;

import io.ibm.migration.consumer.ConfluentShimConsumer;
import io.ibm.migration.producer.ApicurioPayloadProducer;
import io.ibm.migration.util.TopicMirror;
import io.ibm.migration.verify.PayloadIdVerifier;

/**
 * CLI entry point.
 *
 * <pre>
 *   java -jar apps.jar produce [count]    Produce 8-byte-payload Avro (ENABLE_HEADERS=false)
 *   java -jar apps.jar consume-cflt-shim  Consume with ApicurioPayloadDeserializer
 *   java -jar apps.jar mirror-topic       Byte-for-byte copy source→dest (Cluster Linking stand-in)
 *   java -jar apps.jar verify-ids         Check framing + ID resolution on dest
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
                  produce [count]       Produce with ENABLE_HEADERS=false (8-byte payload ID)
                  consume-cflt-shim     Consume with ApicurioPayloadDeserializer
                  mirror-topic          Copy source→dest byte-for-byte (Cluster Linking stand-in)
                  verify-ids            Check 8-byte framing, dest==source, ID resolves in Confluent
                """);
    }
}
