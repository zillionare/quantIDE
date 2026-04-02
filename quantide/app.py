#!/usr/bin/env python3

"""Main application entry point for the Quantide system."""

from __future__ import annotations

from fasthtml.common import serve
from quantide.app_factory import create_app


def init(
    app_config_dir=None,
    enforce_single_instance: bool = True,
):
    return create_app(
        app_config_dir=app_config_dir,
        enforce_single_instance=enforce_single_instance,
    )


app = init()

if __name__ == "__main__":
    serve()
