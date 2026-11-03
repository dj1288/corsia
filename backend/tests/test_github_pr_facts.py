from __future__ import annotations

import pytest

from events.connectors import github_content_facts as content
from events.connectors import github_pr_facts as facts
from events.connectors import github_repo_activity as poller
from events.connectors.github import MANIFEST
from events.connectors.github_notifications_feed import GhResponse
from events.envelope import normalize
from reviewer import gh_helpers


def pull(**over):
    return {"number": 7, "draft": False, "mergeable": None, "labels": [{"name": "b"}, {"name": "a"}],
            "head": {"sha": "h1", "repo": {"full_name": "fork/r"}},
            "base": {"ref": "dev", "sha": "b1", "repo": {"full_name": "o/r"}},
            "user": {"login": "someone"}, **over}


def webhook(action="synchronize", **over):
    return {"action": action, "repository": {"full_name": "o/r"}, "pull_request": pull(**over)}


def run(name, status="completed", conclusion="success", started="2026-01-01T00:00:00Z", id=1, completed=None):
    return {"name": name, "status": status, "conclusion": conclusion, "started_at": started, "id": id,
            "completed_at": completed}


def page(*runs, total=None):
    return {"total_count": len(runs) if total is None else total, "check_runs": list(runs)}


@pytest.mark.parametrize("value, state", [(True, "MERGEABLE"), (False, "CONFLICTING"), (None, "UNKNOWN"),
                                          ("clean", "UNKNOWN"), (1, "UNKNOWN")])
def test_mergeable_is_a_string_enum_never_a_boolean(value, state):
    assert facts.mergeable_state is gh_helpers.mergeable_state
    assert facts.mergeable_state(value) == state


def test_pull_facts_come_from_the_pull_request_object():
    got = facts.pull_facts(pull())
    assert got == {"head_sha": "h1", "trigger_head": "h1", "base_ref": "dev", "base_sha": "b1", "draft": False,
                   "mergeable": "UNKNOWN", "labels": ["a", "b"]}


def test_no_fact_asks_a_started_worker_for_a_head_tree():
    from automation.worker_inputs import HEAD_REPO_FACT
    assert HEAD_REPO_FACT not in facts.pull_facts(pull()) and HEAD_REPO_FACT not in facts.HEAD_CHECK_FACT_NAMES


@pytest.mark.parametrize("runs, held, state", [
    ([run("a"), run("b", conclusion="skipped"), run("c", conclusion="neutral")], False, "green"),
    ([run("a"), run("b", status="in_progress", conclusion=None)], False, "pending"),
    ([], False, "pending"),
    ([run("a", conclusion="failure"), run("b", status="queued", conclusion=None)], False, "failing"),
    ([run("a", conclusion="action_required")], False, "action_required"),
    ([run("a")], True, "action_required"),
    ([run("a", conclusion="failure")], True, "action_required"),
    ([run("a", conclusion="failure", started="2026-01-01T00:00:00Z", id=1),
      run("a", conclusion="success", started="2026-01-02T00:00:00Z", id=2)], False, "green"),
    # A re-run that started after the old run completed is the newest, even before it completes.
    ([run("a", conclusion="failure", completed="2026-01-01T00:05:00Z", id=1),
      run("a", status="in_progress", conclusion=None, started="2026-01-01T00:06:00Z", id=2)], False, "pending"),
    ([run("qa/llm-reviewer", status="queued", conclusion=None), run("qa/llm-reviewer-present", conclusion="failure"),
      run("ci")], False, "green"),
])
def test_check_state_uses_the_newest_run_per_name_without_the_reviewer(runs, held, state):
    assert facts.check_state(runs, held) == state


def test_check_state_selects_the_run_the_reviewer_gate_selects():
    older = run("a", conclusion="failure", started="2026-01-01T00:00:00Z", completed="2026-01-01T00:09:00Z", id=1)
    newer = run("a", conclusion="success", started="2026-01-01T00:05:00Z", completed="2026-01-01T00:07:00Z", id=2)
    assert gh_helpers.newest_check_runs([older, newer]) == [older]  # completed_at decides, not started_at
    assert facts.check_state([older, newer]) == "failing"


def test_a_webhook_event_carries_the_pull_request_facts():
    envelope = normalize("github", webhook(), "pr.synchronize")
    for name in ("head_sha", "trigger_head", "base_ref", "base_sha", "draft", "mergeable", "labels"):
        assert name in envelope.facts
    assert envelope.facts["labels"] == ["a", "b"] and envelope.facts["sha"] == "h1"


def test_every_new_fact_is_declared_for_rules():
    for event in ("pr.opened", "pr.synchronize", "pr.ready_for_review", "pr.labeled", "pr.unlabeled",
                  "pr.merged", "pr.closed"):
        assert set(facts.FACT_NAMES) <= set(MANIFEST.decl(event).filterable_fields), event
        assert "current_head" not in MANIFEST.decl(event).filterable_fields, event
    assert set(facts.HEAD_CHECK_FACT_NAMES) <= set(MANIFEST.decl("head.checks_completed").filterable_fields)


class Reads:
    def __init__(self, replies):
        self.replies, self.paths = replies, []

    async def __call__(self, _runner, _repo, path):
        self.paths.append(path)
        reply = next((v for k, v in self.replies.items() if k in path), None)
        return (GhResponse(200, {}, reply) if reply is not None else None), None


@pytest.fixture
def reads(monkeypatch):
    def install(replies):
        reader = Reads(replies)
        monkeypatch.setattr(poller, "gated_read", reader)
        return reader
    return install


CHECKS = "check-runs?per_page=100&page="


@pytest.mark.asyncio
async def test_current_facts_reads_the_pull_request_once_and_the_checks_once(reads):
    reader = reads({"/pulls/7": pull(mergeable=True, head={"sha": "h2", "repo": {"full_name": "fork/r"}}),
                    "/check-runs": page(run("ci"))})
    got = await facts.current_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert reader.paths == ["repos/o/r/pulls/7", f"repos/o/r/commits/h2/{CHECKS}1"]
    assert got["mergeable"] == "MERGEABLE" and got["head_sha"] == "h2" and got["trigger_head"] == "h1"
    assert got["check_state"] == "green" and "current_head" not in got


@pytest.mark.asyncio
async def test_check_runs_are_read_page_by_page_until_the_total(reads):
    reader = reads({f"{CHECKS}1": page(*[run(f"c{n}") for n in range(100)], total=101),
                    f"{CHECKS}2": page(run("late", conclusion="failure"), total=101)})
    got = await facts.current_facts(None, "o/r", None, {"head_sha": "h1"})
    assert reader.paths == [f"repos/o/r/commits/h1/{CHECKS}1", f"repos/o/r/commits/h1/{CHECKS}2"]
    assert got["check_state"] == "failing"


@pytest.mark.asyncio
async def test_a_failed_page_or_too_many_pages_is_unreadable(reads):
    reads({f"{CHECKS}1": page(*[run(f"c{n}") for n in range(100)], total=101)})
    assert (await facts.current_facts(None, "o/r", None, {"head_sha": "h1"}))["check_state"] == "unreadable"
    reader = reads({"/check-runs": page(*[run(f"c{n}") for n in range(100)], total=10_000)})
    assert (await facts.current_facts(None, "o/r", None, {"head_sha": "h1"}))["check_state"] == "unreadable"
    assert len(reader.paths) == facts.MAX_CHECK_PAGES


@pytest.mark.asyncio
@pytest.mark.parametrize("own, origin", [(True, "inside"), (False, "outside")])
async def test_a_head_checks_event_learns_whose_pull_request_it_is(reads, monkeypatch, own, origin):
    monkeypatch.setattr(content, "is_own", lambda login: own and login == "someone")
    reads({"/pulls/7": pull(), "/check-runs": page()})
    got = await facts.head_check_facts(None, "o/r", 7, {"repo": "o/r", "head_sha": "h1"})
    assert got["author_origin"] == origin


@pytest.mark.asyncio
async def test_a_head_checks_event_keeps_its_settled_head_when_the_pull_request_moved(reads):
    reader = reads({"/pulls/7": pull(mergeable=True, head={"sha": "Y"}),
                    "/commits/X/check-runs": page(run("ci", conclusion="failure")),
                    "/commits/Y/check-runs": page(run("ci"))})
    settled = {"repo": "o/r", "head_sha": "X", "trigger_head": "X", "conclusion": "failure"}
    got = await facts.head_check_facts(None, "o/r", 7, settled)
    assert got["head_sha"] == "X" and got["trigger_head"] == "X" and got["current_head"] == "Y"
    assert got["check_state"] == "failing" and got["conclusion"] == "failure"  # both about X
    assert got["mergeable"] == "MERGEABLE" and got["base_ref"] == "dev"
    assert reader.paths == ["repos/o/r/pulls/7", f"repos/o/r/commits/X/{CHECKS}1"]


@pytest.mark.asyncio
async def test_a_head_checks_event_carries_current_head_even_when_unchanged(reads):
    reads({"/pulls/7": pull(head={"sha": "X"}), "/check-runs": page(run("ci"))})
    got = await facts.head_check_facts(None, "o/r", 7, {"repo": "o/r", "head_sha": "X", "trigger_head": "X"})
    assert got["current_head"] == "X" and got["check_state"] == "green"


@pytest.mark.asyncio
async def test_a_head_checks_event_without_its_pull_request_read_is_unreadable(reads):
    reader = reads({"/check-runs": page(run("ci"))})
    got = await facts.head_check_facts(None, "o/r", 7, {"repo": "o/r", "head_sha": "X", "trigger_head": "X"})
    assert got == {"repo": "o/r", "head_sha": "X", "trigger_head": "X", "mergeable": "UNKNOWN",
                   "check_state": "unreadable"}
    assert reader.paths == ["repos/o/r/pulls/7"]


@pytest.mark.asyncio
async def test_a_known_mergeable_state_still_reads_the_current_head(reads):
    reader = reads({"/pulls/7": pull(mergeable=False, head={"sha": "h2"}),
                    "/check-runs": page(run("ci", conclusion="failure"))})
    got = await facts.current_facts(None, "o/r", 7, facts.pull_facts(pull(mergeable=True)))
    assert reader.paths == ["repos/o/r/pulls/7", f"repos/o/r/commits/h2/{CHECKS}1"]
    assert got["head_sha"] == "h2" and got["trigger_head"] == "h1"
    assert got["mergeable"] == "CONFLICTING" and got["check_state"] == "failing"


@pytest.mark.asyncio
async def test_failed_reads_leave_unknown_and_unreadable_never_raise(monkeypatch):
    async def broken(*_args):
        raise OSError("no gh")

    monkeypatch.setattr(poller, "gated_read", broken)
    got = await facts.current_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert got["mergeable"] == "UNKNOWN" and got["check_state"] == "unreadable" and got["head_sha"] == "h1"


@pytest.mark.asyncio
async def test_a_held_back_read_is_unreadable(reads):
    got = await facts.current_facts(None, "o/r", 7, facts.pull_facts(pull(mergeable=True)))
    assert got["check_state"] == "unreadable"


@pytest.mark.asyncio
async def test_enrichment_only_completes_open_pr_events(reads):
    reader = reads({"/check-runs": page(run("ci"))})
    opened = await facts.enriched(normalize("github", webhook("opened", mergeable=True), "pr.opened"), object())
    assert opened.facts["check_state"] == "green"
    closed = await facts.enriched(
        normalize("github", webhook("closed", merged=True), "pr.merged"), object())
    assert "check_state" not in closed.facts
    assert await facts.enriched(None) is None
    assert len(reader.paths) == 2  # the opened event's PR and checks reads; the merged one reads nothing


@pytest.mark.asyncio
async def test_route_preparation_hands_the_enriched_envelope_to_the_rules(monkeypatch):
    from events.connectors import github_route_preparation as preparation
    from events.connectors import github_rules

    envelope = normalize("github", webhook("synchronize"), "pr.synchronize")
    seen = {}

    async def enriched(env):
        return type(env)(**{**env.__dict__, "facts": {**env.facts, "check_state": "green"}})

    def prepare(_unit, _event, _payload, _identity, _pr, scope, _delivery):
        seen["scope"] = scope

    monkeypatch.setattr(github_rules, "rule_scope", lambda *_a: github_rules.RuleScope(envelope=envelope))
    monkeypatch.setattr(facts, "enriched", enriched)
    monkeypatch.setattr(preparation, "_prepare_route", prepare)
    await preparation.prepare_route_ordered("webhook.github.pr_synchronize", webhook(), None, None)
    assert seen["scope"].envelope.facts["check_state"] == "green"
