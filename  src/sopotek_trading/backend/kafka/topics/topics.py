from kafka.admin import KafkaAdminClient, NewTopic

# Kafka broker
BOOTSTRAP_SERVERS = "localhost:9092"

# Topics for Sopotek Trading AI
TOPICS = [
    "market.candles",
    "market.tickers",
    "market.orderbook",
    "strategy.signals",
    "orders.execution",
    "portfolio.updates",
    "training.status"
]

def create_topics():

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="sopotek-admin"
    )

    existing_topics = admin.list_topics()

    topics_to_create = []

    for topic in TOPICS:

        if topic not in existing_topics:

            topics_to_create.append(
                NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=1
                )
            )

    if topics_to_create:

        admin.create_topics(new_topics=topics_to_create)
        print("Topics created successfully")

    else:

        print("All topics already exist")

    admin.close()


if __name__ == "__main__":
    create_topics()