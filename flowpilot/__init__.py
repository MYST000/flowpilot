"""FlowPilot gateway, conservative reuse, and deferred-context control plane."""

from flowpilot.protocol import (
    DCS_PROTOCOL_VERSION,
    PHASE4_PROTOCOL_VERSION,
    PROTOCOL_VERSION,
    SEMANTIC_REUSE_PROTOCOL_VERSION,
    TRACE_SCHEMA_VERSION,
)

__all__ = [
    "DCS_PROTOCOL_VERSION",
    "PHASE4_PROTOCOL_VERSION",
    "PROTOCOL_VERSION",
    "SEMANTIC_REUSE_PROTOCOL_VERSION",
    "TRACE_SCHEMA_VERSION",
]

__version__ = "0.4.0"
