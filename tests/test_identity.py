from __future__ import annotations

import pytest

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.protocol import JobRegistration, LineRegistration, RequestIdentity


def _digest() -> str:
    return "a" * 64


@pytest.mark.anyio
async def test_request_namespace_and_conversation_are_bound_to_registered_line() -> (
    None
):
    frontier = LineTailFrontier()
    await frontier.register_job(
        JobRegistration(
            job_id="job-identity",
            root_conversation_id="conversation-root",
            deployment_id="deployment-a",
            namespace_id="tenant-a",
        )
    )
    await frontier.register_line(
        LineRegistration(
            job_id="job-identity",
            line_id="line-root",
            conversation_id="conversation-root",
            context_epoch=1,
            base_context_cursor="root",
            context_digest=_digest(),
        )
    )
    identity = RequestIdentity(
        job_id="job-identity",
        line_id="line-root",
        tail_request_id=f"tail-{'line-root'}",
        attempt=1,
        request_id="request-1",
        llm_call_id="call-1",
        expected_tail_version=0,
        context_epoch=1,
        context_sequence=0,
        base_context_cursor="root",
        context_digest=_digest(),
        conversation_id="conversation-other",
        deployment_id="deployment-a",
        namespace_id="tenant-a",
    )
    with pytest.raises(FrontierConflict, match="conversation_id"):
        await frontier.begin_request(identity, "model")


@pytest.mark.anyio
async def test_child_line_requires_parent_line_in_the_same_job() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-identity"))
    with pytest.raises(FrontierConflict, match="parent line"):
        await frontier.register_line(
            LineRegistration(
                job_id="job-identity",
                line_id="line-child",
                conversation_id=f"conversation-{'line-child'}",
                parent_conversation_id="conversation-root",
                parent_line_id="line-missing",
                context_epoch=1,
                base_context_cursor="root",
                context_digest=_digest(),
            )
        )
