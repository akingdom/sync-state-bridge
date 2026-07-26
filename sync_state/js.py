"""
sync_state/js.py
Handles memory-cached delivery of packaged JavaScript client assets.
"""

from importlib import resources

# 1. Read the asset into RAM instantly when the module is imported.
# This prevents synchronous disk I/O blocking during live web requests.
try:
    CLIENT_JS_CONTENT: str = (
        resources.files("sync_state.client").joinpath("stateClient.js").read_text(encoding="utf-8")
    )
except Exception as e:
    # Safe fallback wrapper to prevent system crashes if the PyPI package asset is corrupted
    CLIENT_JS_CONTENT = f"/* Error loading client asset: {e} */"


def get_client_js_content() -> str:
    """Return the pre-loaded JavaScript asset string from the memory cache."""
    return CLIENT_JS_CONTENT


# ==============================================================================
# FASTAPI IMPLEMENTATION EXAMPLE
# ==============================================================================
# Copy and paste this blueprint into your main application routing module:
#
# from fastapi import FastAPI
# from fastapi.responses import HTMLResponse
# from sync_state.js import get_client_js_content
#
# app = FastAPI()
#
# @app.get("/client/stateClient.js")
# def serve_client_js():
#     """
#     Serves the client asset instantly from system RAM.
#     """
#     return HTMLResponse(
#         content=get_client_js_content(),
#         media_type="application/javascript"
#     )
# ==============================================================================
