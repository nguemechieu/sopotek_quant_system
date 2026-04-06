from __future__ import annotations

from fastapi import Request


def get_settings(request: Request):
    return request.app.state.settings


def get_state_store(request: Request):
    return request.app.state.platform_state


def get_kafka_gateway(request: Request):
    return request.app.state.kafka_gateway


def get_control_service(request: Request):
    return request.app.state.control_service
