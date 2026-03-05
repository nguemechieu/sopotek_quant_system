from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

data = {
    "symbol": "BTC/USDT",
    "price": 65000
}

producer.send("market.tickers", data)
producer.flush()
producer.send("market.candles", data)

producer.flush()