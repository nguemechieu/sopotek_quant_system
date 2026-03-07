import os

structure = {
    "sopotek-trading-ai": {

        "README.md": "",
        "pyproject.toml": "",
        "requirements.txt": "",
        ".env": "",
        "Dockerfile": "",
        "docker-compose.yml": "",

        "config": {
            "settings.py": "",
            "logging.yaml": "",
            "exchanges.yaml": "",
            "strategies.yaml": "",
            "risk.yaml": "",
        },

        "scripts": {
            "run_live.py": "",
            "run_backtest.py": "",
            "train_models.py": "",
            "data_download.py": "",
        },

        "data": {
            "raw": {},
            "processed": {},
            "features": {},
        },

        "models": {
            "trained": {},
            "checkpoints": {},
            "balance.csv": "",
        },

        "logs": {
            "system.log": "",
            "trades.log": "",
            "errors.log": "",
        },

        "docs": {
            "architecture.md": "",
            "api.md": "",
            "strategy_docs.md": "",
        },

        "tests": {
            "test_broker.py": "",
            "test_strategy.py": "",
            "test_risk.py": "",
            "test_execution.py": "",
        },

        "src": {

            "sopotek_trading": {

                "__init__.py": "",

                "main.py": "",

                "event_bus": {
                    "__init__.py": "",
                    "event_bus.py": "",
                    "event_types.py": "",
                    "event.py": "",
                    "event_engine.py": "",
                },

                "engines": {
                    "__init__.py": "",
                    "market_data_engine.py": "",
                    "strategy_engine.py": "",
                    "risk_engine.py": "",
                    "execution_engine.py": "",
                    "portfolio_engine.py": "",
                },

                "core": {
                    "__init__.py": "",
                    "orchestrator.py": "",
                    "trading_engine.py": "",
                    "scheduler.py": "",
                    "system_state.py": "",
                },

                "broker": {
                    "__init__.py": "",
                    "base_broker.py": "",
                    "broker_factory.py": "",
                    "ccxt_broker.py": "",
                    "oanda_broker.py": "",
                    "rate_limiter.py": "",
                },

                "market_data": {
                    "__init__.py": "",
                    "candle_buffer.py": "",
                    "orderbook_buffer.py": "",
                    "ticker_stream.py": "",
                    "websocket": {
                        "__init__.py": "",
                        "binance_ws.py": "",
                        "oanda_ws.py": "",
                    },
                },

                "execution": {
                    "__init__.py": "",
                    "execution_manager.py": "",
                    "order_router.py": "",
                    "slippage_model.py": "",
                    "smart_execution.py": "",
                },

                "portfolio": {
                    "__init__.py": "",
                    "portfolio.py": "",
                    "portfolio_manager.py": "",
                    "position.py": "",
                    "pnl_engine.py": "",
                },

                "risk": {
                    "__init__.py": "",
                    "institutional_risk.py": "",
                    "exposure_manager.py": "",
                    "drawdown_guard.py": "",
                    "risk_models.py": "",
                },

                "strategy": {
                    "__init__.py": "",
                    "base_strategy.py": "",
                    "strategy_registry.py": "",
                    "momentum_strategy.py": "",
                    "mean_reversion.py": "",
                    "arbitrage_strategy.py": "",
                },

                "quant": {

                    "__init__.py": "",

                    "features": {
                        "__init__.py": "",
                        "feature_engineering.py": "",
                        "indicators.py": "",
                    },

                    "ml": {
                        "__init__.py": "",
                        "ml_signal.py": "",
                        "model_manager.py": "",
                        "training_pipeline.py": "",
                    },

                    "analytics": {
                        "__init__.py": "",
                        "performance_engine.py": "",
                        "metrics.py": "",
                        "risk_metrics.py": "",
                    },
                },

                "backtesting": {
                    "__init__.py": "",
                    "backtest_engine.py": "",
                    "simulator.py": "",
                    "report_generator.py": "",
                },

                "storage": {
                    "__init__.py": "",
                    "database.py": "",
                    "trade_repository.py": "",
                    "market_data_repository.py": "",
                },

                "utils": {
                    "__init__.py": "",
                    "utils.py": "",
                    "time_utils.py": "",
                    "async_utils.py": "",
                },

                "frontend": {

                    "__init__.py": "",

                    "ui": {
                        "__init__.py": "",
                        "app_controller.py": "",
                        "main_window.py": "",

                        "chart": {
                            "__init__.py": "",
                            "chart_widget.py": "",
                        },

                        "panels": {
                            "__init__.py": "",
                            "orderbook_panel.py": "",
                            "trades_panel.py": "",
                            "portfolio_panel.py": "",
                        },

                        "report_generator.py": "",
                    },

                    "console": {
                        "__init__.py": "",
                        "system_console.py": "",
                    },
                },
            }
        }
    }
}


def create_structure(base, tree):

    for name, content in tree.items():

        path = os.path.join(base, name)

        if isinstance(content, dict):

            os.makedirs(path, exist_ok=True)

            create_structure(path, content)

        else:

            os.makedirs(base, exist_ok=True)

            if not os.path.exists(path):

                with open(path, "w") as f:
                    f.write(content)


create_structure(".", structure)

print("✅ Sopotek Trading AI folder structure created!")