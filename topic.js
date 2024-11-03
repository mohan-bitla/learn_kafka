const { Kafka } = require("kafkajs")

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "my-app",
      brokers: ["localhost:9092"],
    })

    const admin = kafka.admin()

    console.log("Connecting to Kafka broker...")
    await admin.connect()
    console.log("Connected to Kafka broker")

    // Create a new topic
    // A-M and N-Z
    await admin.createTopics({
      topics: [
        {
          topic: "Users",
          numPartitions: 2,
        },
      ],
    })
    
    console.log("Topic created successfully")
  } catch (e) {
    console.error(`[example/producer] ${e.message}`, e)
  } finally {
    process.exit(0)
  }
}

run()