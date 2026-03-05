# import asyncio
# import joblib
# import shutil
#
#
#
# class RetrainingScheduler:
#
#     def __init__(self, broker, symbol="ETH/USDT", interval_hours=168):
#         self.interval_hours = interval_hours
#         self.broker = broker
#         self.symbol = symbol
#         self.interval = interval_hours * 3600
#
#     async def start(self):
#
#         while True:
#             try:
#                 print("Starting shadow retraining...")
#
#                 # 1️⃣ Pull historical data
#                 raw = await self.broker.get_ohlcv(self.symbol, timeframe=self.interval_hours)
#
#                 import pandas as pd
#                 df = pd.DataFrame(
#                     raw,
#                     columns=["timestamp", "open", "high", "low", "close", "volume"]
#                 )
#
#                 # 2️⃣ Build features + labels
#                 X, y = build_features_and_labels(df)
#
#                 # 3️⃣ Train shadow model
#                 await train(X, y)
#
#                 # 4️⃣ Evaluate shadow model
#                 if self._validate_shadow(X, y):
#                     shutil.copy(
#                         "models/shadow_model.pkl",
#                         "models/production_model.pkl"
#                     )
#                     print("Production model updated.")
#
#                 print("Retraining cycle complete.")
#
#             except Exception as e:
#                 print("Retraining failed:", e)
#
#             await asyncio.sleep(self.interval)
#
#     def _validate_shadow(self, X, y):
#
#         from sklearn.metrics import accuracy_score
#         model = joblib.load("src/sopotek_trading/backend/models/shadow_model.h5")
#
#         preds = model.predict(X)
#
#         accuracy = accuracy_score(y, preds)
#
#         print("Shadow accuracy:", accuracy)
#
#         # Prop threshold
#         return accuracy > 0.55