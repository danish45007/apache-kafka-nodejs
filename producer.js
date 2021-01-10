const { Kafka } = require("kafkajs");

// ask user to send message to topic
const msg = process.argv[2];

const run = async () => {
  try {
    // establish a tcp conn with the broker for comm.
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });
    // call a producer
    const producer = kafka.producer();
    console.log("Connecting....................");
    // connect to an producer
    // A-M -> partition 0 && N-Z -> partition 1
    await producer.connect();
    console.log("Connected Successfully!");
    const partition = msg[0] < "N" ? 0 : 1;
    const res = await producer.send({
      topic: "Users",
      messages: [
        {
          value: msg,
          partition: partition,
        },
      ],
    });

    console.log(
      `Message sent to user topic created successfully! ${JSON.stringify(res)}`
    );
    // disconnect after topic creation done
    await producer.disconnect();
  } catch (err) {
    console.error(`Something went wrong ${err}`);
  } finally {
    process.exit(0);
  }
};

run();
