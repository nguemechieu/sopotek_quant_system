import json
import pandas as pd
from confluent_kafka import Consumer, Producer

from sopotek_trading.backend.strategy.trend_strategy import TrendStrategy

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "strategy-engine",
    "auto.offset.reset": "earliest"
})

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

consumer.subscribe(["market.candles"])

strategy = TrendStrategy()

candles = []

while True:

    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))

    candles.append(data)

    df = pd.DataFrame(candles)

    if len(df) < 200:
        continue

    signal = strategy.generate_signal("BTCUSDT", df)

    if signal:

        producer.produce(
            "strategy.signals",
            json.dumps(signal).encode("utf-8")
        )

        print("SIGNAL SENT:", signal)
        for strategy in  strategy.get_strategies():
            strategy.append(signal)