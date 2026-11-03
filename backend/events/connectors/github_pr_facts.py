"""The current pull-request facts a GitHub review event carries.

A webhook's ``pull_request`` object supplies most of them. The current head and
mergeable state (a payload can be stale or redelivered) are read once, and the
head's check runs page by page, through the repo-activity poller's budget and
backoff gate. A failed read is never an error: the fact reads ``UNKNOWN`` or
``unreadable`` and the event still goes out. Check selection, the mergeable
mapping, the reviewer's context and author origin come from their owners.
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
from reviewer.gh_helpers import mergeable_state, newest_check_runs
from reviewer.reviewer_categories import STATUS_CHECK_CONTEXT

log = logging.getLogger(__name__)

# No head_repo: worker starts read that name as a request for a head tree (automation/worker_inputs.py).
FACT_NAMES = ("head_sha", "trigger_head", "base_ref", "base_sha", "draft", "mergeable", "labels", "check_state")
#: A settled head's ``head_sha`` is the head whose checks settled; ``current_head`` is the PR's live head.
HEAD_CHECK_FACT_NAMES = (*FACT_NAMES, "current_head")
#: Open-PR event types whose facts are completed by a read; a terminal event keeps what its payload holds.
READ_TYPES = frozenset({"pr.opened", "pr.synchronize", "pr.ready_for_review", "pr.labeled", "pr.unlabeled"})
UNREADABLE = "unreadable"
#: A head with more check runs than this many pages of 100 reads ``unreadable``.
MAX_CHECK_PAGES = 5
_FAILING = frozenset({"failure", "timed_out", "cancelled", "startup_failure", "stale"})


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


def check_state(runs: list[Any], held: bool = False) -> str:
    """The head's state from its newest run per check name; ``held`` is a suite waiting for approval.

    The reviewer's own checks (every context it posts starts with its name) never count.
    """
    newest = [run for run in newest_check_runs([run for run in runs if isinstance(run, dict)])
              if not str(run.get("name") or "").startswith(STATUS_CHECK_CONTEXT)]
    conclusions = {str(run.get("conclusion") or "") for run in newest if run.get("status") == "completed"}
    # A head waiting for a maintainer waits for them first, whatever else failed.
    if held or "action_required" in conclusions:
        return "action_required"
    if conclusions & _FAILING:
        return "failing"
    if any(run.get("status") != "completed" for run in newest) or not newest:
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


async def _check_state(runner: Any, repo: str, sha: Any, held: bool = False) -> str:
    """``sha``'s check state from every page of its runs; a failed page, or too many, is ``unreadable``."""
    if not isinstance(sha, str) or not sha:
        return UNREADABLE
    runs: list[Any] = []
    for page in range(1, MAX_CHECK_PAGES + 1):
        body = _object(await _read(runner, repo, f"repos/{repo}/commits/{sha}/check-runs?per_page=100&page={page}"))
        batch, total = body.get("check_runs"), body.get("total_count")
        if not isinstance(batch, list) or not isinstance(total, int):
            return UNREADABLE
        runs += batch
        if len(runs) >= total:
            return check_state(runs, held)
    return UNREADABLE


async def current_facts(runner: Any, repo: str, number: int | None, facts: Mapping[str, Any]) -> dict[str, Any]:
    """An open-PR event's ``facts`` completed: ``head_sha`` becomes the PR's live head, with its check state."""
    out = dict(facts)
    out.setdefault("mergeable", "UNKNOWN")
    body = await _pull(runner, repo, number) if number is not None else None
    if body is not None:
        out.update({key: value for key, value in pull_facts(body).items() if key != "trigger_head"})
    out["check_state"] = await _check_state(runner, repo, out.get("head_sha"))
    return out


async def head_check_facts(
    runner: Any, repo: str, number: int | None, facts: Mapping[str, Any], *, held: bool = False,
) -> dict[str, Any]:
    """A settled head's ``facts`` completed; ``head_sha`` stays the head whose checks settled.

    The PR read adds ``current_head``, the PR's live head, which may already have
    moved on. When that read fails, ``check_state`` is ``unreadable``.
    """
    out = dict(facts)
    out.setdefault("mergeable", "UNKNOWN")
    if number is not None:
        body = await _pull(runner, repo, number)
        if body is None:
            out["check_state"] = UNREADABLE
            return out
        live = pull_facts(body)
        live.pop("trigger_head", None)
        current = live.pop("head_sha", None)
        if current is not None:
            out["current_head"] = current
        out.update(live)
        login = _text(_object(body.get("user")).get("login"))
        if login:
            out["author_origin"] = author_origin(login)
    out["check_state"] = await _check_state(runner, repo, out.get("head_sha"), held)
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
