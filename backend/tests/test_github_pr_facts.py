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


def suite(conclusion="success", runs=1, status="completed", id=1):
    return {"id": id, "status": status, "conclusion": conclusion, "latest_check_runs_count": runs}


HELD = suite("action_required", runs=0, id=9)


def suites(*items):
    return {"total_count": len(items), "check_suites": list(items)}


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
    assert HEAD_REPO_FACT not in facts.pull_facts(pull()) and HEAD_REPO_FACT not in facts.FACT_NAMES


@pytest.mark.parametrize("found, runs, state", [
    ([], [run("a"), run("b", conclusion="skipped"), run("c", conclusion="neutral")], "green"),
    ([], [run("a"), run("b", status="in_progress", conclusion=None)], "pending"),
    ([], [], "pending"),
    ([], [run("a", conclusion="failure"), run("b", status="queued", conclusion=None)], "failing"),
    ([], [run("a", conclusion="action_required")], "action_required"),
    ([HELD], [], "action_required"),  # a held suite has no runs; never pending
    ([HELD, suite()], [run("a")], "action_required"),
    ([HELD], [run("a", conclusion="failure")], "action_required"),
    # The reviewer's counterexample: one suite held, another timed out. The maintainer comes first.
    ([HELD, suite("timed_out", id=2)], [run("a", conclusion="timed_out")], "action_required"),
    ([suite("timed_out")], [run("a")], "failing"),
    # A zero-run suite that is not held never counts, whatever GitHub left it as.
    ([suite("stale", runs=0), suite(None, runs=0, status="queued", id=2)], [run("a")], "green"),
    ([], [run("a", conclusion="failure", started="2026-01-01T00:00:00Z", id=1),
          run("a", conclusion="success", started="2026-01-02T00:00:00Z", id=2)], "green"),
    # A re-run that started after the old run completed is the newest, even before it completes.
    ([], [run("a", conclusion="failure", completed="2026-01-01T00:05:00Z", id=1),
          run("a", status="in_progress", conclusion=None, started="2026-01-01T00:06:00Z", id=2)], "pending"),
    # Only the conclusions the reviewer gate counts green are green; a missing or unknown one fails.
    ([], [run("a"), run("b", conclusion=None)], "failing"),
    ([], [run("a"), run("b", conclusion="something_new")], "failing"),
])
def test_check_state_has_one_precedence_over_suites_and_newest_runs(found, runs, state):
    assert facts.check_state(found, runs) == state


def test_check_state_and_the_reviewer_gate_share_one_green_list():
    from events.connectors import github_reviewer_gate as gate
    assert gate.GREEN_CONCLUSIONS is gh_helpers.GREEN_CONCLUSIONS
    assert not hasattr(facts, "_FAILING") and not hasattr(gate, "_TERMINAL_GREEN_STATES")


def test_check_state_selects_the_run_the_reviewer_gate_selects():
    older = run("a", conclusion="failure", started="2026-01-01T00:00:00Z", completed="2026-01-01T00:09:00Z", id=1)
    newer = run("a", conclusion="success", started="2026-01-01T00:05:00Z", completed="2026-01-01T00:07:00Z", id=2)
    assert gh_helpers.newest_check_runs([older, newer]) == [older]  # completed_at decides, not started_at
    assert facts.check_state([], [older, newer]) == "failing"


def test_a_webhook_event_carries_the_pull_request_facts():
    envelope = normalize("github", webhook(), "pr.synchronize")
    for name in ("head_sha", "trigger_head", "base_ref", "base_sha", "draft", "mergeable", "labels"):
        assert name in envelope.facts
    assert envelope.facts["labels"] == ["a", "b"] and envelope.facts["sha"] == "h1"


def test_every_new_fact_is_declared_for_rules():
    for event in ("pr.opened", "pr.synchronize", "pr.ready_for_review", "pr.labeled", "pr.unlabeled",
                  "pr.merged", "pr.closed"):
        assert set(facts.FACT_NAMES) <= set(MANIFEST.decl(event).filterable_fields), event
    head = MANIFEST.decl("head.checks_completed").filterable_fields
    assert set(facts.FACT_NAMES) <= set(head) and "conclusion" not in head  # one outcome fact: check_state


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
SUITES = "check-suites"


@pytest.mark.asyncio
async def test_an_open_pr_event_reads_the_pull_request_and_its_own_heads_checks_once(reads):
    reader = reads({"/pulls/7": pull(mergeable=True, head={"sha": "h2", "repo": {"full_name": "fork/r"}}),
                    "/check-suites": suites(suite()), "/check-runs": page(run("ci"))})
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert reader.paths == ["repos/o/r/pulls/7", f"repos/o/r/commits/h1/{SUITES}", f"repos/o/r/commits/h1/{CHECKS}1"]
    # head_sha stays the payload's head and check_state describes it; the PR has moved on to h2.
    assert got["head_sha"] == "h1" and got["current_head"] == "h2" and got["check_state"] == "green"
    assert got["mergeable"] == "MERGEABLE"


def hundred(prefix):
    return [run(f"{prefix}{n}") for n in range(100)]


@pytest.mark.asyncio
async def test_check_runs_are_read_page_by_page_until_the_total(reads):
    pages = {f"{CHECKS}{n}": page(*hundred(f"p{n}-"), total=701) for n in range(1, 8)}
    pages[f"{CHECKS}8"] = page(run("late", conclusion="failure"), total=701)
    reader = reads({"/pulls/7": pull(), "/check-suites": suites(suite()), **pages})
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert reader.paths[2:] == [f"repos/o/r/commits/h1/{CHECKS}{n}" for n in range(1, 9)]  # no page cap
    assert got["check_state"] == "failing"


@pytest.mark.asyncio
@pytest.mark.parametrize("second", [None, page(total=101), {"check_runs": [run("x")]}])
async def test_a_failed_empty_or_uncounted_page_before_the_total_is_unreadable(reads, second):
    replies = {"/pulls/7": pull(), "/check-suites": suites(suite()), f"{CHECKS}1": page(*hundred("c"), total=101)}
    if second is not None:
        replies[f"{CHECKS}2"] = second
    reader = reads(replies)
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert got["check_state"] == "unreadable" and len(reader.paths) == 4


@pytest.mark.asyncio
async def test_a_failed_suites_read_is_unreadable(reads):
    reader = reads({"/pulls/7": pull(), "/check-runs": page(run("ci"))})  # the runs alone would read green
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert got["check_state"] == "unreadable" and reader.paths[-1] == f"repos/o/r/commits/h1/{SUITES}"


@pytest.mark.asyncio
async def test_a_spent_read_budget_ends_the_paging_as_unreadable(monkeypatch):
    left = [4]  # the PR read, the suites and two pages, then the budget is spent

    async def budgeted(_runner, _repo, path):
        if not left[0]:
            return None, "budget_exhausted"
        left[0] -= 1
        body = pull() if "/pulls/" in path else suites(suite()) if SUITES in path else page(*hundred("c"), total=10_000)
        return GhResponse(200, {}, body), None

    monkeypatch.setattr(poller, "gated_read", budgeted)
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert got["check_state"] == "unreadable" and left == [0]


@pytest.mark.asyncio
@pytest.mark.parametrize("own, origin", [(True, "inside"), (False, "outside")])
async def test_a_head_checks_event_learns_whose_pull_request_it_is(reads, monkeypatch, own, origin):
    monkeypatch.setattr(content, "is_own", lambda login: own and login == "someone")
    reads({"/pulls/7": pull(), "/check-runs": page()})
    got = await facts.head_facts(None, "o/r", 7, {"repo": "o/r", "head_sha": "h1"}, [suite()])
    assert got["author_origin"] == origin


@pytest.mark.asyncio
async def test_a_head_checks_event_keeps_its_settled_head_when_the_pull_request_moved(reads):
    reader = reads({"/pulls/7": pull(mergeable=True, head={"sha": "Y"}),
                    "/commits/X/check-runs": page(run("ci", conclusion="failure")),
                    "/commits/Y/check-runs": page(run("ci"))})
    settled = {"repo": "o/r", "head_sha": "X", "trigger_head": "X"}
    got = await facts.head_facts(None, "o/r", 7, settled, [suite("failure")])
    assert got["head_sha"] == "X" and got["trigger_head"] == "X" and got["current_head"] == "Y"
    assert got["check_state"] == "failing"  # about X
    assert got["mergeable"] == "MERGEABLE" and got["base_ref"] == "dev"
    # The aggregator passes the suites it read, so only the PR and X's runs are read here.
    assert reader.paths == ["repos/o/r/pulls/7", f"repos/o/r/commits/X/{CHECKS}1"]


@pytest.mark.asyncio
@pytest.mark.parametrize("found", [[HELD], [HELD, suite("timed_out", id=2)]])
@pytest.mark.parametrize("kind", ["open_pr", "head_checks"])
async def test_a_held_head_reads_action_required_on_every_event_kind(reads, found, kind):
    timed_out = [run("ci", conclusion="timed_out")] if len(found) > 1 else []
    reads({"/pulls/7": pull(), "/check-suites": suites(*found), "/check-runs": page(*timed_out)})
    if kind == "open_pr":
        got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    else:
        got = await facts.head_facts(None, "o/r", 7, {"repo": "o/r", "head_sha": "h1"}, found)
    assert got["check_state"] == "action_required"


@pytest.mark.asyncio
@pytest.mark.parametrize("given", [{"repo": "o/r", "head_sha": "X", "trigger_head": "X"},
                                   {"head_sha": "X", "sha": "X", "mergeable": "MERGEABLE"}])
async def test_a_failed_pull_request_read_keeps_head_sha_and_has_no_current_head(reads, given):
    reader = reads({"/check-suites": suites(suite()), "/check-runs": page(run("ci"))})  # would answer green
    got = await facts.head_facts(None, "o/r", 7, given)
    assert got == {"mergeable": "UNKNOWN", **given, "check_state": "unreadable"}
    assert reader.paths == ["repos/o/r/pulls/7"]


@pytest.mark.asyncio
async def test_a_known_mergeable_state_still_reads_the_pull_request(reads):
    reader = reads({"/pulls/7": pull(mergeable=False, head={"sha": "h2"}), "/check-suites": suites(suite()),
                    "/check-runs": page(run("ci", conclusion="failure"))})
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull(mergeable=True)))
    assert reader.paths[0] == "repos/o/r/pulls/7"
    assert got["head_sha"] == "h1" and got["current_head"] == "h2"
    assert got["mergeable"] == "CONFLICTING" and got["check_state"] == "failing"


@pytest.mark.asyncio
async def test_failed_reads_leave_unknown_and_unreadable_never_raise(monkeypatch):
    async def broken(*_args):
        raise OSError("no gh")

    monkeypatch.setattr(poller, "gated_read", broken)
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull()))
    assert got["mergeable"] == "UNKNOWN" and got["check_state"] == "unreadable" and got["head_sha"] == "h1"
    assert "current_head" not in got


@pytest.mark.asyncio
async def test_a_held_back_read_is_unreadable(reads):
    got = await facts.head_facts(None, "o/r", 7, facts.pull_facts(pull(mergeable=True)))
    assert got["check_state"] == "unreadable"


@pytest.mark.asyncio
async def test_enrichment_only_completes_open_pr_events(reads):
    reader = reads({"/pulls/7": pull(), "/check-suites": suites(suite()), "/check-runs": page(run("ci"))})
    opened = await facts.enriched(normalize("github", webhook("opened", mergeable=True), "pr.opened"), object())
    assert opened.facts["check_state"] == "green"
    closed = await facts.enriched(
        normalize("github", webhook("closed", merged=True), "pr.merged"), object())
    assert "check_state" not in closed.facts
    assert await facts.enriched(None) is None
    assert len(reader.paths) == 3  # the opened event's PR, suites and runs reads; the merged one reads nothing


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
