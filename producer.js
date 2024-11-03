const { Kafka } = require("kafkajs")
const msg = process.argv[2]

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "my-app",
      brokers: ["localhost:9092"],
    })

    const producer = kafka.producer()

    console.log("Connecting to Kafka broker...")
    await producer.connect()
    console.log("Connected to Kafka broker")

    // A-M: 0 and N-Z: 1
    const result = await producer.send({
      topic: "Users",
      messages: [
        {
          value: msg,
          partition: msg[0] < "N" ? 0 : 1,
        },
      ],
    })
    
    
    console.log(`message produced ${JSON.stringify(result)} successfully`)
    await producer.disconnect()
  } catch (e) {
    console.error(`[example/producer] ${e.message}`, e)
  } finally {
    process.exit(0)
  }
}

run()