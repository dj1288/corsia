"""The current pull-request facts a GitHub review event carries.

A webhook's ``pull_request`` object supplies most of them. The current head and
mergeable state (a payload can be stale or redelivered), and the head's check
state, are read once each through the repo-activity poller's budget and backoff gate. A failed
read is never an error: the fact reads ``UNKNOWN`` or ``unreadable`` and the
event still goes out.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
from collections.abc import Mapping
from typing import Any

from events.connectors.github_label_events import label_names
from events.connectors.github_ownership import is_own

log = logging.getLogger(__name__)

# No head_repo: worker starts read that name as a request for a head tree (automation/worker_inputs.py).
FACT_NAMES = ("head_sha", "trigger_head", "base_ref", "base_sha", "draft", "mergeable", "labels", "check_state")
#: Open-PR event types whose facts are completed by a read; a terminal event keeps what its payload holds.
READ_TYPES = frozenset({"pr.opened", "pr.synchronize", "pr.ready_for_review", "pr.labeled", "pr.unlabeled"})
UNREADABLE = "unreadable"
#: The reviewer's own check never counts toward the state it is waiting on.
EXCLUDED_CHECKS = "qa/llm-reviewer"
_FAILING = frozenset({"failure", "timed_out", "cancelled", "startup_failure", "stale"})
_GREEN = frozenset({"success", "skipped", "neutral"})


def _object(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def mergeable_state(value: Any) -> str:
    """REST and webhook ``true``/``false``/``null`` as ``MERGEABLE``/``CONFLICTING``/``UNKNOWN``."""
    return {True: "MERGEABLE", False: "CONFLICTING"}[value] if isinstance(value, bool) else "UNKNOWN"


def pull_facts(pull: Mapping[str, Any]) -> dict[str, Any]:
    """Facts from a pull request object, a webhook's or a REST read's."""
    head, base = _object(pull.get("head")), _object(pull.get("base"))
    sha = _text(head.get("sha"))
    facts = {
        "head_sha": sha, "trigger_head": sha,
        "base_ref": _text(base.get("ref")), "base_sha": _text(base.get("sha")),
        "draft": pull.get("draft") if isinstance(pull.get("draft"), bool) else None,
        "mergeable": mergeable_state(pull.get("mergeable")),
        "labels": label_names(pull["labels"]) if isinstance(pull.get("labels"), list) else None,
    }
    return {key: value for key, value in facts.items() if value is not None}


def check_state(runs: list[Any], held: bool = False) -> str:
    """The head's state from its newest run per check name; ``held`` is a suite waiting for approval."""
    newest: dict[str, Mapping[str, Any]] = {}
    for run in map(_object, runs):
        name = str(run.get("name") or "")
        if name.startswith(EXCLUDED_CHECKS):
            continue
        order = (str(run.get("started_at") or ""), int(run.get("id") or 0))
        old = newest.get(name)
        if old is None or order > (str(old.get("started_at") or ""), int(old.get("id") or 0)):
            newest[name] = run
    conclusions = {str(run.get("conclusion") or "") for run in newest.values() if run.get("status") == "completed"}
    # A head waiting for a maintainer waits for them first, whatever else failed.
    if held or "action_required" in conclusions:
        return "action_required"
    if conclusions & _FAILING:
        return "failing"
    if any(run.get("status") != "completed" for run in newest.values()) or not newest:
        return "pending"
    return "green"


async def _read(runner: Any, repo: str, path: str) -> Any:
    from events.connectors import github_repo_activity as poller

    try:
        response, _reason = await asyncio.shield(poller.gated_read(runner, repo, path))  # counted even if cancelled
    except Exception:
        log.warning("github-pr-facts: read failed for %s", path, exc_info=True)
        return None
    return response.body if response is not None and response.status == 200 else None


async def current_facts(
    runner: Any, repo: str, number: int | None, facts: Mapping[str, Any], *, held: bool = False,
) -> dict[str, Any]:
    """``facts`` completed: one PR read for the current head and mergeable state, one checks read for that head."""
    out = dict(facts)
    out.setdefault("mergeable", "UNKNOWN")
    if number is not None:
        body = await _read(runner, repo, f"repos/{repo}/pulls/{number}")
        if isinstance(body, Mapping):
            out.update({key: value for key, value in pull_facts(body).items() if key != "trigger_head"})
            login = _text(_object(body.get("user")).get("login"))
            if login and "author_origin" not in out:  # a head-checks event: whose pull request it is
                out["author_origin"] = "inside" if is_own(login) else "outside"
    sha = out.get("head_sha")
    body = await _read(runner, repo, f"repos/{repo}/commits/{sha}/check-runs?per_page=100") if sha else None
    runs = _object(body).get("check_runs")
    out["check_state"] = check_state(runs, held) if isinstance(runs, list) else UNREADABLE
    return out


async def enriched(envelope: Any, runner: Any = None) -> Any:
    """An open-PR webhook envelope with its facts completed; any other envelope as it was."""
    if envelope is None or envelope.type not in READ_TYPES:
        return envelope
    if runner is None:
        if os.environ.get("CORSIA_TEST_MODE") == "1":  # a test never reaches the real GitHub
            return envelope
        from connections.github_broker import default_broker
        runner = default_broker()
    repo, number = envelope.facts.get("repo"), envelope.facts.get("pr_number")
    if not isinstance(repo, str):
        return envelope
    facts = await current_facts(runner, repo, number if isinstance(number, int) else None, envelope.facts)
    return dataclasses.replace(envelope, facts=facts)
