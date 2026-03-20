from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from backtesting.experiment_tracker import ExperimentTracker
from quant.ml_dataset import MLDataset, MLDatasetBuilder
from quant.ml_models import LinearSignalClassifier, SequenceLinearClassifier, TreeThresholdEnsembleClassifier
from quant.model_registry import ModelRegistry


@dataclass
class MLResearchResult:
    model_name: str
    model: object
    metrics: dict = field(default_factory=dict)
    dataset_metadata: dict = field(default_factory=dict)
    experiment_id: str | None = None


class MLResearchPipeline:
    VERSION = "ml-research-v1"

    def __init__(self, model_registry=None, experiment_tracker=None, dataset_builder=None):
        self.model_registry = model_registry or ModelRegistry()
        self.experiment_tracker = experiment_tracker or ExperimentTracker()
        self.dataset_builder = dataset_builder or MLDatasetBuilder()

    def build_dataset(self, candles, **kwargs):
        return self.dataset_builder.build_from_candles(candles, **kwargs)

    def _build_model(self, model_family="linear", sequence_length=4):
        family = str(model_family or "linear").strip().lower()
        if family in {"linear", "logistic"}:
            return LinearSignalClassifier(model_name="linear_signal_classifier"), "linear"
        if family in {"tree", "tree_ensemble", "stump_ensemble"}:
            return TreeThresholdEnsembleClassifier(model_name="tree_threshold_ensemble"), "tree"
        if family in {"sequence", "sequence_linear", "temporal"}:
            return SequenceLinearClassifier(sequence_length=max(2, int(sequence_length or 4)), model_name="sequence_linear_classifier"), "sequence"
        raise ValueError(f"Unsupported model family: {model_family}")

    def _classification_metrics(self, actual, predicted, probabilities):
        actual = np.asarray(actual, dtype=int)
        predicted = np.asarray(predicted, dtype=int)
        probabilities = np.asarray(probabilities, dtype=float).reshape(-1)
        if len(actual) == 0:
            return {
                "samples": 0,
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "positive_rate": 0.0,
                "avg_confidence": 0.0,
            }

        accuracy = float((actual == predicted).mean())
        predicted_positive = predicted == 1
        actual_positive = actual == 1
        tp = int(np.logical_and(predicted_positive, actual_positive).sum())
        fp = int(np.logical_and(predicted_positive, ~actual_positive).sum())
        fn = int(np.logical_and(~predicted_positive, actual_positive).sum())
        precision = float(tp / max(1, tp + fp))
        recall = float(tp / max(1, tp + fn))
        return {
            "samples": int(len(actual)),
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "positive_rate": float(actual_positive.mean()),
            "avg_confidence": float(np.mean(np.maximum(probabilities, 1.0 - probabilities))),
        }

    def train_classifier(
        self,
        dataset: MLDataset,
        model_name="ml_model_v1",
        model_family="linear",
        sequence_length=4,
        test_size=0.25,
        experiment_name="ml_research",
        notes="",
    ):
        if dataset is None or dataset.empty:
            raise ValueError("Dataset is empty; unable to train ML classifier")

        model, normalized_family = self._build_model(model_family=model_family, sequence_length=sequence_length)
        working_dataset = dataset.to_sequence_dataset(sequence_length=sequence_length) if normalized_family == "sequence" else dataset
        train_frame, test_frame = working_dataset.train_test_split(test_size=test_size)
        if train_frame.empty or test_frame.empty:
            raise ValueError("Dataset split produced an empty train or test window")

        feature_columns = list(working_dataset.feature_columns)
        x_train = train_frame[feature_columns].to_numpy(dtype=float)
        y_train = train_frame[working_dataset.target_column].to_numpy(dtype=int)
        x_test = test_frame[feature_columns].to_numpy(dtype=float)
        y_test = test_frame[working_dataset.target_column].to_numpy(dtype=int)

        model.model_name = str(model_name or "ml_model_v1")
        model.fit(x_train, y_train, feature_names=feature_columns)
        train_prob = model.predict_proba(x_train)[:, 1]
        test_prob = model.predict_proba(x_test)[:, 1]
        train_pred = (train_prob >= 0.5).astype(int)
        test_pred = (test_prob >= 0.5).astype(int)

        metrics = {
            "version": self.VERSION,
            "train_accuracy": self._classification_metrics(y_train, train_pred, train_prob)["accuracy"],
            "test_accuracy": self._classification_metrics(y_test, test_pred, test_prob)["accuracy"],
            "test_precision": self._classification_metrics(y_test, test_pred, test_prob)["precision"],
            "test_recall": self._classification_metrics(y_test, test_pred, test_prob)["recall"],
            "avg_test_confidence": self._classification_metrics(y_test, test_pred, test_prob)["avg_confidence"],
            "train_samples": int(len(train_frame)),
            "test_samples": int(len(test_frame)),
        }
        metadata = {
            "dataset": dict(working_dataset.metadata or {}),
            "feature_columns": feature_columns,
            "model_family": normalized_family,
        }
        self.model_registry.register(model_name, model, metadata={**metadata, **metrics})
        record = self.experiment_tracker.add_record(
            name=experiment_name,
            strategy_name="ML Model",
            symbol=str((working_dataset.metadata or {}).get("symbol") or "BACKTEST"),
            timeframe=str((working_dataset.metadata or {}).get("timeframe") or "1h"),
            parameters={
                "model_name": model_name,
                "model_family": normalized_family,
                "test_size": test_size,
                "feature_count": len(feature_columns),
                "sequence_length": sequence_length if normalized_family == "sequence" else 1,
            },
            dataset_metadata=dict(working_dataset.metadata or {}),
            metrics=dict(metrics),
            notes=str(notes or "").strip(),
        )
        return MLResearchResult(
            model_name=str(model_name),
            model=model,
            metrics=metrics,
            dataset_metadata=metadata,
            experiment_id=record.experiment_id,
        )

    def run_walk_forward(
        self,
        dataset: MLDataset,
        model_family="linear",
        sequence_length=4,
        train_size=80,
        test_size=30,
        step_size=None,
    ):
        if dataset is None or dataset.empty:
            return pd.DataFrame(), pd.DataFrame()

        model_template, normalized_family = self._build_model(model_family=model_family, sequence_length=sequence_length)
        working_dataset = dataset.to_sequence_dataset(sequence_length=sequence_length) if normalized_family == "sequence" else dataset
        frame = working_dataset.frame.reset_index(drop=True)
        feature_columns = list(working_dataset.feature_columns)
        target_column = working_dataset.target_column
        if frame.empty or len(frame) < max(3, int(train_size) + int(test_size)):
            return pd.DataFrame(), pd.DataFrame()

        train_size = max(8, int(train_size))
        test_size = max(4, int(test_size))
        step = max(1, int(step_size or test_size))
        summary_rows = []
        prediction_rows = []

        for window_index, train_start in enumerate(range(0, len(frame) - train_size - test_size + 1, step)):
            train_end = train_start + train_size
            test_end = train_end + test_size
            train_frame = frame.iloc[train_start:train_end].copy()
            test_frame = frame.iloc[train_end:test_end].copy()
            if train_frame.empty or test_frame.empty:
                continue

            model, _ = self._build_model(model_family=normalized_family, sequence_length=sequence_length)
            model.model_name = getattr(model_template, "model_name", "ml_model")
            x_train = train_frame[feature_columns].to_numpy(dtype=float)
            y_train = train_frame[target_column].to_numpy(dtype=int)
            x_test = test_frame[feature_columns].to_numpy(dtype=float)
            y_test = test_frame[target_column].to_numpy(dtype=int)
            model.fit(x_train, y_train, feature_names=feature_columns)
            test_prob = model.predict_proba(x_test)[:, 1]
            test_pred = (test_prob >= 0.5).astype(int)
            metrics = self._classification_metrics(y_test, test_pred, test_prob)
            summary_rows.append(
                {
                    "window_index": window_index,
                    "train_rows": len(train_frame),
                    "test_rows": len(test_frame),
                    "train_start": train_frame.iloc[0].get("timestamp"),
                    "train_end": train_frame.iloc[-1].get("timestamp"),
                    "test_start": test_frame.iloc[0].get("timestamp"),
                    "test_end": test_frame.iloc[-1].get("timestamp"),
                    "accuracy": metrics["accuracy"],
                    "precision": metrics["precision"],
                    "recall": metrics["recall"],
                    "avg_confidence": metrics["avg_confidence"],
                    "model_family": normalized_family,
                }
            )
            for row_idx, probability in enumerate(test_prob):
                prediction_rows.append(
                    {
                        "window_index": window_index,
                        "timestamp": test_frame.iloc[row_idx].get("timestamp"),
                        "actual": int(y_test[row_idx]),
                        "predicted": int(test_pred[row_idx]),
                        "probability": float(probability),
                        "regime": test_frame.iloc[row_idx].get("regime"),
                    }
                )

        summary_df = pd.DataFrame(summary_rows)
        prediction_df = pd.DataFrame(prediction_rows)
        if not summary_df.empty:
            metrics = {
                "walk_forward_windows": int(len(summary_df)),
                "walk_forward_accuracy": float(summary_df["accuracy"].mean()),
                "walk_forward_precision": float(summary_df["precision"].mean()),
                "walk_forward_recall": float(summary_df["recall"].mean()),
            }
            self.experiment_tracker.add_record(
                name=f"walk-forward-{normalized_family}",
                strategy_name="ML Model",
                symbol=str((working_dataset.metadata or {}).get("symbol") or "BACKTEST"),
                timeframe=str((working_dataset.metadata or {}).get("timeframe") or "1h"),
                parameters={
                    "model_family": normalized_family,
                    "train_size": train_size,
                    "test_size": test_size,
                    "step_size": step,
                    "sequence_length": sequence_length if normalized_family == "sequence" else 1,
                },
                dataset_metadata=dict(working_dataset.metadata or {}),
                metrics=metrics,
                notes="ml_walk_forward",
            )
        return summary_df, prediction_df

    def deploy_to_strategy_registry(self, strategy_registry, model_name, strategy_name="ML Model"):
        from strategy.strategy import Strategy

        model = self.model_registry.get(model_name)
        if model is None:
            raise KeyError(f"Model '{model_name}' is not registered")

        deployed_strategy = Strategy(model=model, strategy_name=strategy_name)
        strategy_registry.register(strategy_name, deployed_strategy)
        if hasattr(strategy_registry, "set_active"):
            strategy_registry.set_active(strategy_name)
        return deployed_strategy
