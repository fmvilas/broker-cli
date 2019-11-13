const { Kafka } = require('kafkajs');

module.exports.publish = async ({ host, username, password, auth, channel, message }) => {
  const client = new Kafka({
    brokers: [host],
    clientId: 'broker-cli',
    ssl: username && password && auth ? {
      rejectUnauthorized: true,
    } : undefined,
    sasl: username && password && auth ? {
      mechanism: auth,
      username,
      password,
    } : undefined,
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

module.exports.subscribe = async ({ host, username, password, auth, channel }) => {
  const client = new Kafka({
    brokers: [host],
    clientId: 'broker-cli',
    ssl: username && password && auth ? {
      rejectUnauthorized: true,
    } : undefined,
    sasl: username && password && auth ? {
      mechanism: auth,
      username,
      password,
    } : undefined,
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