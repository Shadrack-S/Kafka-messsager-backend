const express = require('express');
const { Kafka } = require('kafkajs');
const bodyParser = require('body-parser');

const app = express();
const port = 3000;

app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'my-group' });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

  app.get('/', (req, res) => {
    res.send('Hello from Express!');
  });

  app.post('/send-message', async (req, res) => {
    const { message } = req.body;
    await producer.send({
      topic: 'my-topic',
      messages: [{ value: message }],
    });
    res.send('Message sent to Kafka');
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });

  app.listen(port, () => {
    console.log(`Server listening on port ${port}`);
  });
}

run().catch(console.error);
