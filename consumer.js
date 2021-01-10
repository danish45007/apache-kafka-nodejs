const { Kafka } = require("kafkajs");

const run = async () => {
  try {
    // establish a tcp conn with the broker for comm.
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });
    // create a consumer with groupId("test")
    const consumer = kafka.consumer({
      groupId: "test",
    });
    console.log("Connecting....................");

    await consumer.connect();
    console.log("Connected Successfully!");
    // subscribe to a topic
    await consumer.subscribe({
      topic: "Users",
      fromBeginning: true,
    });
    // poll messages from topic("Users")
    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `Received message ${result.message.value} on partition ${result.partition}`
        );
      },
    });
  } catch (err) {
    console.error(`Something went wrong ${err}`);
  }
};

run();
