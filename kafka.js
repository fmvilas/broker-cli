const { Kafka } = require('kafkajs');

module.exports.publish = async ({ host, channel, message }) => {
  const client = new Kafka({
    brokers: [host],
    clientId: 'broker-cli',
  });
  const producer = client.producer();

  try {
    await producer.connect();
    await producer.send({
      topic: channel,
      messages: [{
        value: message,
      }],
    });
    console.log('Message sent!');
    await producer.disconnect();
  } catch (e) {
    console.error(e);
  }
};

module.exports.subscribe = async ({ host, channel }) => {
  const client = new Kafka({
    brokers: [host],
    clientId: 'broker-cli',
  });
  const consumer = client.consumer({ groupId: 'broker-cli-group-id' });
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: channel });
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        console.log(String(message.value));
      },
    });
  } catch (e) {
    console.error(e);
  }
};