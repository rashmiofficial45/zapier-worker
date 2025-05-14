// Import the Kafka class from the 'kafkajs' library.
// This class is used to create Kafka clients (producers/consumers).
import { Kafka } from "kafkajs";

// Create a Kafka instance with configuration.
// - clientId: A user-defined identifier for this Kafka client, useful for logging and monitoring.
// - brokers: An array of Kafka broker addresses. Here, it's pointing to a local Kafka instance.
const kafka = new Kafka({
  clientId: "worker",
  brokers: ["localhost:9092"],
});

// Define the name of the Kafka topic the consumer will subscribe to.
// In this example, it's named 'zap-events'.
const TOPIC_NAME = "zap-events";

// Main async function to run the Kafka consumer logic.
async function main() {
  /**
   * Create a consumer instance and assign it to a consumer group.
   * - groupId: Consumers with the same group ID share the work of consuming messages from the topic.
   * - Changing the groupId will make Kafka treat this as a new consumer group, so it will start consuming messages from the beginning or from its offset (based on config).
   */
  const consumer = kafka.consumer({ groupId: "main-worker" });

  // Connect the consumer to the Kafka broker.
  await consumer.connect();

  /**
   * Subscribe the consumer to a specific topic.
   * - topic: The topic to subscribe to.
   * - fromBeginning: If true, the consumer will read messages from the start of the topic (offset 0).
   *   If false, it will start from the latest offset for that group.
   */
  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

  /**
   * Start consuming messages using the `run` method.
   * - autoCommit: false means we will manually commit offsets after processing each message.
   * - eachMessage: This function runs for every new message received by the consumer.
   */
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      // Log details of the received message:
      // - partition: Kafka topic partition number the message came from.
      // - offset: The offset (position) of the message in the partition.
      // - value: The actual payload of the message (converted from Buffer to string).
      console.log({
        partition,
        offset: message.offset,
        value: message.value?.toString(),
      });

      // Simulate some delay to represent message processing time (1 second here).
      await new Promise((r) => setTimeout(r, 1000));

      console.log("processing done");

      /**
       * After successfully processing the message, manually commit the offset.
       * IMPORTANT: Kafka expects the *next* offset to be committed.
       * - If you just processed offset 5, you must commit offset 6 to mark it as done.
       */
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: String(Number(message.offset) + 1),
        },
      ]);
    },
  });
}

// Call the main function and handle any uncaught errors.
main().catch((err) => {
  // If any error occurs during consumer execution, log it and exit the process with error code 1.
  console.error("Kafka consumer crashed", err);
  process.exit(1);
});
