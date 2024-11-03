const { Kafka } = require("kafkajs")

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "my-app",
      brokers: ["localhost:9092"],
    })

    const consumer = kafka.consumer({ groupId: "test-group" })

    console.log("Connecting to Kafka broker...")
    await consumer.connect()
    console.log("Connected to Kafka broker")

    await consumer.subscribe({ topic: "Users", fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          value: message.value.toString(),
        })
      },
    })
  } catch (e) {
    console.error(`[example/consumer] ${e.message}`, e)
  }
}

run()