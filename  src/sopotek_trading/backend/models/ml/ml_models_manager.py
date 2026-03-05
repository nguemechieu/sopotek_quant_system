import asyncio
import os
import traceback

import joblib

from sopotek_trading.backend.models.ml.ml_signal import MLSignal


class MLModelManager:

    def __init__(self, controller=None, model_dir="../../models/ml"):

        self.controller = controller
        self.logger = controller.logger if controller else None
        self.models = {}
        self.training_status = {}
        self.locks = {}

        self.model_dir = model_dir
        os.makedirs(self.model_dir, exist_ok=True)

    def register_symbol(self, symbol):

        if symbol in self.models:
            return

        self.models[symbol] = None
        self.training_status[symbol] = False
        self.locks[symbol] = asyncio.Lock()

        self._load_model(symbol)

    def save_model(self, symbol: str):

        path = self._model_path(symbol)

        try:
            self.models[symbol].save(path)
            if self.logger:
                self.logger.info(f"Model saved for {symbol}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed saving model {symbol}: {e}")

    def _model_path(self, symbol):

        self.model_dir = "../../models/ml"
        if not  os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)

        return os.path.join(self.model_dir, symbol.replace("/","_") + ".keras")

    def _load_model(self, symbol):

        if  symbol  is None:
            if self.logger:
                self.logger.warning(
                    "Initializing model not trained for symbol {}".format(symbol)
                )
                return None
        path = self._model_path(symbol)
        if os.path.exists(path):
            self.logger.info("Loading model from %s", symbol)
            self.model = joblib.load(path)
            if self.logger:
                self.logger.info(f"Model loaded from {symbol}")
        return None

    async def predict(self, symbol: str, df):

        model = self.models.get(symbol)

        if model is None:
            if self.logger:
                self.logger.warning(
                    f"Prediction requested but model not trained for {symbol}"
                )
            return {}

        loop = asyncio.get_running_loop()

        try:
            result = await loop.run_in_executor(
                None,
                model.predict,
                df
            )

            return result

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Prediction failed for {symbol}: {e}"
                )
            return {

            }

    async def train(self, symbol: str, df):

        if df is None:
            if self.logger:
                self.logger.warning("Training requested but df not trained for {symbol}")
                return None

        if symbol not in self.locks:
            self.register_symbol(symbol)

        async with self.locks[symbol]:

            # 🟡 Emit TRAINING status
            self.controller.training_status_signal.emit(symbol, "training")

            model = self.models.get(symbol)

            if model is None:
                model = MLSignal(lookback=100)

            loop = asyncio.get_running_loop()

            try:
                await loop.run_in_executor(
                    None,
                    model.train,
                    df
                )

                self.models[symbol] = model
                self.training_status[symbol] = True

                self.save_model(symbol)

                # 🟢 Emit READY status
                self.controller.training_status_signal.emit(symbol, "ready")

            except Exception as e:
                self.controller.training_status_signal.emit(symbol, "error")
                self.logger.error(str(e))
                traceback.print_exc()

    def is_trained(self, symbol: str) -> bool:
        return self.training_status.get(symbol, False)
