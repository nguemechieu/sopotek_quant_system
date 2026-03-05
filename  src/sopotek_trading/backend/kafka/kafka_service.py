import json
import logging

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


logger = logging.getLogger(__name__)


class KafkaService:

    def __init__(self, bootstrap_servers="localhost:9092"):

        self.bootstrap_servers = bootstrap_servers

        self.producer = None
        self.consumers = {}

    # ------------------------------------------------
    # PRODUCER
    # ------------------------------------------------

    async def start_producer(self):

        if self.producer:
            return

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        await self.producer.start()

        logger.info("Kafka producer started")

    async def stop_producer(self):

        if self.producer:

            await self.producer.stop()

            logger.info("Kafka producer stopped")

    async def publish(self, topic: str, data: dict):

        if not self.producer:
            raise RuntimeError("Kafka producer not started")

        await self.producer.send_and_wait(topic, data)

    # ------------------------------------------------
    # CONSUMER
    # ------------------------------------------------

    async def start_consumer(self, topic, group_id="trading-engine"):

        consumer = AIOKafkaConsumer(

            topic,

            bootstrap_servers=self.bootstrap_servers,

            group_id=group_id,

            auto_offset_reset="latest",

            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        await consumer.start()

        self.consumers[topic] = consumer

        logger.info(f"Kafka consumer started for topic: {topic}")

        return consumer

    async def stop_consumer(self, topic):

        consumer = self.consumers.get(topic)

        if consumer:

            await consumer.stop()

            logger.info(f"Kafka consumer stopped: {topic}")

            del self.consumers[topic]

    # ------------------------------------------------
    # MESSAGE LOOP
    # ------------------------------------------------

    async def consume(self, topic):

        consumer = self.consumers.get(topic)

        if not consumer:
            raise RuntimeError(f"Consumer not started for {topic}")

        async for msg in consumer:

            yield msg.value