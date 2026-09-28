from pathlib import Path

import pytest
from reuse_support import execution, registry, request, service

from flowpilot.reuse.adapters.terminal_url import (
    normalize_terminal_url_command,
    terminal_input,
)
from flowpilot.reuse.contracts import digest


def terminal_registry(**updates):
    return registry(
        **{
            "tool_name": "terminal",
            "canonical_tool_family": "terminal_url",
            "adapter_id": "terminal_url_fetch_v1",
            "input_schema_digest": None,
            "tool_version": "1",
            "result_schema_version": "terminal-observation-v1",
            "command_line_reuse": "url_exact",
            **updates,
        }
    )


def terminal_observation(command):
    return {
        "kind": "TerminalObservation",
        "command": command,
        "exit_code": 0,
        "is_error": False,
        "timeout": False,
        "content": [{"type": "text", "text": "page body", "cache_prompt": False}],
        "metadata": {
            "exit_code": 0,
            "pid": 123,
            "working_dir": "/leader",
            "hostname": "leader",
        },
        "full_output_save_dir": "/leader/output",
    }


@pytest.mark.parametrize(
    "command",
    [
        "curl https://example.com",
        "curl -sSL https://example.com",
        "curl --request GET --url=https://example.com",
        "curl -I --max-time 10 https://example.com",
        "curl -o- 'https://example.com/?q=1&q=2'",
        "curl -g 'https://example.com?filter[0]=x'",
        "wget -qO- https://example.com",
        "wget --output-document=- https://example.com",
        "/usr/bin/curl -sS http://127.0.0.1:8000/page",
    ],
)
def test_url_families_within_terminal(command):
    assert normalize_terminal_url_command({"command": command}) is not None


@pytest.mark.parametrize(
    "command",
    [
        "wget https://example.com",
        "wget -O page https://example.com",
        "curl -o page https://example.com",
        "curl -O https://example.com",
        "curl --data=x https://example.com",
        "curl -X POST https://example.com",
        "curl -H 'Cookie: x' https://example.com",
        "curl -b cookies https://example.com",
        "curl -K config https://example.com",
        "curl https://example.com | head",
        "curl https://example.com > page",
        "curl https://example.com; pwd",
        "curl https://example.com?q=1&x=2",
        "curl https://example.com &",
        "curl 'https://example.com/$TOKEN'",
        "curl https://example.com/$(id)",
        "curl 'https://example.com/{a,b}'",
        "curl 'https://example.com/a[1-2]'",
        "curl 'https://example.com?x=[1-2]'",
        "curl https://example.com https://example.org",
        "python -c 'import requests; print(requests.get(\"https://example.com\").text)'",
        "git clone https://example.com/repo",
        "env curl https://example.com",
        "curl file:///tmp/page",
        "curl https://user:password@example.com",
    ],
)
def test_non_reusable_commands_remain_local(command):
    assert normalize_terminal_url_command({"command": command}) is None


@pytest.mark.parametrize(
    "extra", [{"is_input": True}, {"reset": True}, {"timeout": -1}]
)
def test_terminal_state_actions_are_not_url_fetch(extra):
    assert (
        normalize_terminal_url_command({"command": "curl https://example.com", **extra})
        is None
    )


@pytest.mark.parametrize("historical", [True, False])
@pytest.mark.parametrize("family", ["curl -sS", "wget -qO-"])
async def test_terminal_original_action_receipts_and_reuse(
    tmp_path: Path, historical, family
):
    svc = service(tmp_path / "reuse.sqlite", entry=terminal_registry())
    command = f"{family} https://EXAMPLE.com:443"
    first = await request(
        svc, "a", tool_name="terminal", arguments={"command": command, "timeout": 10}
    )
    leader = await svc.resolve(first)
    assert leader.decision == "sync_and_execute_as_leader"
    assert leader.input_digest == digest(
        {"command": command, "is_input": False, "reset": False, "timeout": 10.0}
    )
    report = await execution(svc, first, leader, result=terminal_observation(command))
    if historical:
        await svc.publish(report)
    follower_command = f"{family} 'https://example.com/'"
    second = await request(
        svc,
        "b",
        tool_name="terminal",
        arguments={"command": follower_command, "timeout": 10.0},
    )
    decision = await svc.resolve(second)
    assert decision.descriptor_digest == leader.descriptor_digest
    assert decision.input_digest == digest(terminal_input(second.arguments))
    assert decision.input_digest != leader.input_digest
    if not historical:
        assert decision.binding_id == leader.binding_id
        await svc.publish(report)
        decision = await svc.poll(
            decision.binding_id,
            second.identity.model_copy(update={"action_id": "action-b"}),
        )
    assert decision.result["command"] == follower_command
    assert decision.result["metadata"] == {"exit_code": 0}
    assert decision.result["full_output_save_dir"] is None
    assert decision.result["content"] == report.result["content"]
    assert decision.provenance.result_digest == digest(decision.result)
    assert decision.provenance.original_size > decision.provenance.returned_size


@pytest.mark.parametrize(
    "updates",
    [
        {"exit_code": 22},
        {"timeout": True},
        {"is_error": True},
        {"metadata": {"exit_code": -1}},
        {"command": "curl https://different.example"},
    ],
)
async def test_terminal_incomplete_failed_or_mismatched_observation_not_published(
    tmp_path: Path, updates
):
    svc = service(tmp_path / "reuse.sqlite", entry=terminal_registry())
    command = "curl https://example.com"
    req = await request(svc, "a", tool_name="terminal", arguments={"command": command})
    report = await execution(
        svc,
        req,
        await svc.resolve(req),
        result={**terminal_observation(command), **updates},
    )
    with pytest.raises(ValueError, match="adapter rejected|does not match Action"):
        await svc.publish(report)
    assert await svc.controller._cache.count() == 0


async def test_terminal_policy_family_options_and_timeout_are_hard_constraints(
    tmp_path: Path,
):
    svc = service(tmp_path / "reuse.sqlite", entry=terminal_registry())
    commands = [
        "curl https://example.com",
        "curl -L https://example.com",
        "curl -I https://example.com",
        "wget -qO- https://example.com",
    ]
    decisions = [
        await svc.resolve(
            await request(svc, str(i), tool_name="terminal", arguments={"command": cmd})
        )
        for i, cmd in enumerate(commands)
    ]
    assert len({d.binding_id for d in decisions}) == len(commands)
    assert all(d.decision == "sync_and_execute_as_leader" for d in decisions)
    timed = await svc.resolve(
        await request(
            svc,
            "timed",
            tool_name="terminal",
            arguments={"command": commands[0], "timeout": 10},
        )
    )
    assert timed.binding_id not in {d.binding_id for d in decisions}
    svc.controller._registry["terminal"] = terminal_registry(
        command_line_reuse="disabled"
    )
    assert (
        await svc.resolve(
            await request(
                svc,
                "disabled",
                tool_name="terminal",
                arguments={"command": commands[0]},
            )
        )
    ).decision == "execute_locally"
    svc.controller._registry["terminal"] = terminal_registry(
        command_line_reuse="curl_url_exact"
    )
    assert (
        await svc.resolve(
            await request(
                svc, "wget", tool_name="terminal", arguments={"command": commands[-1]}
            )
        )
    ).decision == "execute_locally"
