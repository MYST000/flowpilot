from __future__ import annotations

import pytest
from pydantic import ValidationError

from flowpilot.protocol import (
    ContextDeltaAppend,
    DCSReference,
    DelegationPolicy,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
)


def _reference() -> DCSReference:
    return DCSReference(
        job_id="job-1",
        line_id="line-1",
        context_epoch=1,
        lease_id="lease-1",
        base_context_cursor="cursor-0",
        delta_digest="a" * 64,
    )


def test_nested_legacy_identity_fields_are_rejected() -> None:
    with pytest.raises(ValidationError, match="legacy tenant identity"):
        DelegationPolicy(
            policy_version=1,
            lease_id="lease-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            base_context_digest="a" * 64,
            issued_at="2026-09-02T00:00:00Z",
            expires_at="2026-09-02T00:01:00Z",
            allowed_tool_names=("web_search",),
            api_kind="chat",
            request_snapshot={"messages": [{"tenant_id": "legacy"}]},
        )


def test_dcs_message_and_reuse_argument_legacy_fields_are_rejected() -> None:
    identity = ToolReuseIdentity(
        job_id="job-1",
        line_id="line-1",
        tail_request_id="tail-1",
        llm_call_id="call-1",
        action_id="action-1",
        tool_call_id="tool-1",
    )
    with pytest.raises(ValidationError, match="legacy tenant identity"):
        ToolReuseResolveRequest(
            identity=identity,
            tool_name="web_search",
            arguments={"tenant_id": "legacy"},
            scope={},
        )
    with pytest.raises(ValidationError, match="legacy tenant identity"):
        ContextDeltaAppend(
            reference=_reference(),
            expected_last_seq=0,
            parent_llm_call_id="call-1",
            messages=(
                {"role": "assistant", "tool_calls": []},
                {"role": "tool", "tenant_id": "legacy"},
            ),
            tool_call_ids=("tool-1",),
            resolution_receipts=("receipt-1",),
            result_digests=("b" * 64,),
        )
