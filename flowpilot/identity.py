"""Canonical workflow identity and namespace checks.

The frontier owns state and locking; this module owns the rules that relate
the job, conversation, line, request, and transport identities.
"""

from __future__ import annotations

from dataclasses import dataclass

from flowpilot.protocol import JobRegistration, LineRegistration, RequestIdentity


class IdentityConflict(ValueError):
    """An identity is not valid in the current deployment namespace."""


@dataclass(frozen=True, slots=True)
class Namespace:
    deployment_id: str | None
    namespace_id: str | None


def namespace_for(job: JobRegistration) -> Namespace:
    return Namespace(job.deployment_id, job.namespace_id)


def validate_namespace(identity: RequestIdentity, namespace: Namespace) -> None:
    if identity.deployment_id != namespace.deployment_id:
        raise IdentityConflict("deployment namespace does not match the job")
    if identity.namespace_id != namespace.namespace_id:
        raise IdentityConflict("request namespace does not match the job")


def validate_line_parent(
    registration: LineRegistration,
    *,
    parent_conversation_id: str | None,
) -> None:
    if registration.parent_line_id is None:
        return
    if parent_conversation_id is None:
        raise IdentityConflict("parent line is not registered in the job")
    if (
        registration.parent_conversation_id is not None
        and registration.parent_conversation_id != parent_conversation_id
    ):
        raise IdentityConflict("parent conversation does not match parent line")


def validate_request_line(
    identity: RequestIdentity,
    *,
    conversation_id: str | None,
    parent_conversation_id: str | None,
    parent_line_id: str | None,
    spawn_id: str | None,
) -> None:
    values = (
        (identity.conversation_id, conversation_id, "conversation_id"),
        (
            identity.parent_conversation_id,
            parent_conversation_id,
            "parent_conversation_id",
        ),
        (identity.parent_line_id, parent_line_id, "parent_line_id"),
        (identity.spawn_id, spawn_id, "spawn_id"),
    )
    for supplied, registered, name in values:
        if registered is not None and supplied != registered:
            raise IdentityConflict(f"{name} does not match the registered line")
