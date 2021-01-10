const { Kafka } = require("kafkajs");

const run = async () => {
  try {
    // establish a tcp conn with the broker for comm.
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });
    // call an admin
    const admin = kafka.admin();
    console.log("Connecting....................");
    // connect to an admin
    await admin.connect();
    console.log("Connected Successfully!");

    // create a topic with two partions (A-M) & (N-Z)
    await admin.createTopics({
      topics: [
        {
          topic: "Users",
          numPartitions: 2,
        },
      ],
    });
    console.log("Users topic created successfully!");
    // disconnect after topic creation done
    await admin.disconnect();
  } catch (err) {
    console.error(`Something went wrong ${err}`);
  } finally {
    process.exit(0);
  }
};

run();
