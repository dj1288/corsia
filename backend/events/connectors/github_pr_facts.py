"""The current pull-request facts a GitHub review event carries.

A webhook's ``pull_request`` object supplies most of them. On every event kind
``head_sha`` is the head the event is about (the payload's head, or the head whose
checks settled) and ``check_state`` describes that head. The pull request is read
once: it adds ``current_head``, the PR's live head, so a rule finds a stale event
with ``head_sha != current_head``, and ``base_ref``, ``base_sha``, ``labels``,
``draft`` and ``mergeable`` from that read describe the PR now. The head's check
suites are read once and its check runs page by page. Every read passes the
repo-activity poller's budget and backoff gate; a spent budget is what bounds the
paging. A failed read is never an error: the fact reads ``UNKNOWN`` or
``unreadable`` and the event still goes out. Check selection, the green
conclusions, the mergeable mapping and author origin come from their owners.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
from collections.abc import Mapping
from typing import Any

from events.connectors.github_content_facts import author_origin
from events.connectors.github_label_events import label_names
from reviewer.gh_helpers import GREEN_CONCLUSIONS, mergeable_state, newest_check_runs

log = logging.getLogger(__name__)

# No head_repo: worker starts read that name as a request for a head tree (automation/worker_inputs.py).
FACT_NAMES = ("head_sha", "trigger_head", "current_head", "base_ref", "base_sha", "draft", "mergeable", "labels",
              "check_state")
#: The facts the PR read refreshes: the pull request's own, not its head's.
_PR_NOW = ("base_ref", "base_sha", "draft", "mergeable", "labels")
#: Open-PR event types whose facts are completed by a read; a terminal event keeps what its payload holds.
READ_TYPES = frozenset({"pr.opened", "pr.synchronize", "pr.ready_for_review", "pr.labeled", "pr.unlabeled"})
UNREADABLE = "unreadable"


def _object(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


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


def counted_suites(suites: Any) -> list[dict[str, Any]]:
    """The suites a head waits on: those with runs, and those held for approval (no runs, ``action_required``)."""
    return [suite for suite in suites if isinstance(suite, dict) and (
        (suite.get("latest_check_runs_count") or 0) > 0 or suite.get("conclusion") == "action_required")
    ] if isinstance(suites, list) else []


def check_state(suites: list[Any], runs: list[Any]) -> str:
    """The head's one state from its check suites and its newest run per check name.

    A held suite has no runs, so only the suites show it. Only check suites and runs
    are read, never commit statuses, so the reviewer's ``qa/llm-reviewer*`` statuses
    never count. A completed suite or run is green only with a conclusion the reviewer
    gate also counts green; any other, an unknown or missing one included, is failing.
    """
    newest = newest_check_runs([run for run in runs if isinstance(run, dict)])
    items = [*counted_suites(suites), *newest]
    conclusions = {str(item.get("conclusion") or "").upper() for item in items if item.get("status") == "completed"}
    # A head waiting for a maintainer waits for them first, whatever else failed.
    if "ACTION_REQUIRED" in conclusions:
        return "action_required"
    if conclusions - GREEN_CONCLUSIONS:
        return "failing"
    if not newest or any(item.get("status") != "completed" for item in items):
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


async def _pull(runner: Any, repo: str, number: int) -> Mapping[str, Any] | None:
    body = await _read(runner, repo, f"repos/{repo}/pulls/{number}")
    return body if isinstance(body, Mapping) else None


async def _check_state(runner: Any, repo: str, sha: Any, suites: list[Any] | None) -> str:
    """``sha``'s check state from its suites (read here unless given) and every page of its runs.

    A failed suites read is ``unreadable``. Runs are read until ``total_count``: a failed or
    empty page before then, or no ``total_count``, is ``unreadable``; so is a page the read
    budget refuses, which is what bounds a head with very many runs.
    """
    if not isinstance(sha, str) or not sha:
        return UNREADABLE
    if suites is None:
        suites = _object(await _read(runner, repo, f"repos/{repo}/commits/{sha}/check-suites")).get("check_suites")
        if not isinstance(suites, list):
            return UNREADABLE
    runs: list[Any] = []
    page = 1
    while True:
        body = _object(await _read(runner, repo, f"repos/{repo}/commits/{sha}/check-runs?per_page=100&page={page}"))
        batch, total = body.get("check_runs"), body.get("total_count")
        if not isinstance(batch, list) or not isinstance(total, int):
            return UNREADABLE
        runs += batch
        if len(runs) >= total:
            return check_state(suites, runs)
        if not batch:
            return UNREADABLE
        page += 1


async def head_facts(
    runner: Any, repo: str, number: int | None, facts: Mapping[str, Any], suites: list[Any] | None = None,
) -> dict[str, Any]:
    """An event's ``facts`` completed for its ``head_sha``, which no read changes.

    ``suites`` are the head's check suites when the caller has already read them. When
    the PR read fails, ``current_head`` is absent and ``check_state`` is ``unreadable``.
    """
    out = dict(facts)
    out.setdefault("mergeable", "UNKNOWN")
    if number is not None:
        body = await _pull(runner, repo, number)
        if body is None:
            out["check_state"] = UNREADABLE
            return out
        live = pull_facts(body)
        out.update({name: live[name] for name in _PR_NOW if name in live})
        if "head_sha" in live:
            out["current_head"] = live["head_sha"]
        login = _text(_object(body.get("user")).get("login"))
        if login:
            out["author_origin"] = author_origin(login)
    out["check_state"] = await _check_state(runner, repo, out.get("head_sha"), suites)
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
    facts = await head_facts(runner, repo, number if isinstance(number, int) else None, envelope.facts)
    return dataclasses.replace(envelope, facts=facts)
