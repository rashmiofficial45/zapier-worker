"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
// Import the Kafka class from the 'kafkajs' library.
// This class is used to create Kafka clients (producers/consumers).
const kafkajs_1 = require("kafkajs");
// Create a Kafka instance with configuration.
// - clientId: A user-defined identifier for this Kafka client, useful for logging and monitoring.
// - brokers: An array of Kafka broker addresses. Here, it's pointing to a local Kafka instance.
const kafka = new kafkajs_1.Kafka({
    clientId: "worker",
    brokers: ["localhost:9092"],
});
// Define the name of the Kafka topic the consumer will subscribe to.
// In this example, it's named 'zap-events'.
const TOPIC_NAME = "zap-events";
// Main async function to run the Kafka consumer logic.
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        /**
         * Create a consumer instance and assign it to a consumer group.
         * - groupId: Consumers with the same group ID share the work of consuming messages from the topic.
         * - Changing the groupId will make Kafka treat this as a new consumer group, so it will start consuming messages from the beginning or from its offset (based on config).
         */
        const consumer = kafka.consumer({ groupId: "main-worker" });
        // Connect the consumer to the Kafka broker.
        yield consumer.connect();
        /**
         * Subscribe the consumer to a specific topic.
         * - topic: The topic to subscribe to.
         * - fromBeginning: If true, the consumer will read messages from the start of the topic (offset 0).
         *   If false, it will start from the latest offset for that group.
         */
        yield consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
        /**
         * Start consuming messages using the `run` method.
         * - autoCommit: false means we will manually commit offsets after processing each message.
         * - eachMessage: This function runs for every new message received by the consumer.
         */
        yield consumer.run({
            autoCommit: false,
            eachMessage: (_a) => __awaiter(this, [_a], void 0, function* ({ topic, partition, message }) {
                var _b;
                // Log details of the received message:
                // - partition: Kafka topic partition number the message came from.
                // - offset: The offset (position) of the message in the partition.
                // - value: The actual payload of the message (converted from Buffer to string).
                console.log({
                    partition,
                    offset: message.offset,
                    value: (_b = message.value) === null || _b === void 0 ? void 0 : _b.toString(),
                });
                // Simulate some delay to represent message processing time (1 second here).
                yield new Promise((r) => setTimeout(r, 1000));
                console.log("processing done");
                /**
                 * After successfully processing the message, manually commit the offset.
                 * IMPORTANT: Kafka expects the *next* offset to be committed.
                 * - If you just processed offset 5, you must commit offset 6 to mark it as done.
                 */
                yield consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: String(Number(message.offset) + 1),
                    },
                ]);
            }),
        });
    });
}
// Call the main function and handle any uncaught errors.
main().catch((err) => {
    // If any error occurs during consumer execution, log it and exit the process with error code 1.
    console.error("Kafka consumer crashed", err);
    process.exit(1);
});
