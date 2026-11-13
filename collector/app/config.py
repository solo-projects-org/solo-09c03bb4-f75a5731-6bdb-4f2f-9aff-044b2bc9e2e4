from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Runtime configuration, overridable through environment variables."""

    source_http_base: str = field(
        default_factory=lambda: os.getenv("SOURCE_HTTP_BASE", "http://localhost:9000")
    )
    source_ws_base: str = field(
        default_factory=lambda: os.getenv("SOURCE_WS_BASE", "ws://localhost:9000")
    )
    # Depth levels requested from the source snapshot (source serves at most 10).
    book_limit: int = field(default_factory=lambda: int(os.getenv("BOOK_LIMIT", "10")))
    # Levels exposed by the query API.
    serve_depth: int = field(default_factory=lambda: int(os.getenv("SERVE_DEPTH", "5")))
    reconnect_delay_seconds: float = field(
        default_factory=lambda: float(os.getenv("RECONNECT_DELAY_SECONDS", "1.0"))
    )
    # No depth data at all for this long -> treat the stream as dead, resync.
    stale_timeout_seconds: float = field(
        default_factory=lambda: float(os.getenv("STALE_TIMEOUT_SECONDS", "3.0"))
    )
    snapshot_http_timeout_seconds: float = field(
        default_factory=lambda: float(os.getenv("SNAPSHOT_HTTP_TIMEOUT_SECONDS", "60.0"))
    )
