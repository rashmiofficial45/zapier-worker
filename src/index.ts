import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "worker",
  brokers: ["localhost:9092"],
});

const TOPIC_NAME = "zap-events";

async function main() {
  const consumer = kafka.consumer({ groupId: "main-worker" });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value?.toString(),
      });

      // Simulate some processing time
      await new Promise((r) => setTimeout(r, 1000));

      console.log("processing done");

      // IMPORTANT: Kafka expects the offset to be the *next one to process*, not the current one.
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

main().catch((err) => {
  console.error("Kafka consumer crashed", err);
  process.exit(1);
});
