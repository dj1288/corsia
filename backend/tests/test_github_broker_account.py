"""Which GitHub account the pollers' read brokers run ``gh`` as."""
from __future__ import annotations

import asyncio
import subprocess

import pytest

from connections import github_agent
from connections.github_agent import AgentAccountView
from connections.github_agent_home import LOCKED_HOME
from connections.github_broker import _run_gh


@pytest.fixture
def ran(monkeypatch):
    seen = {}

    def fake(argv, **kwargs):
        seen.update(argv=argv, env=kwargs["env"])
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake)
    return seen


def never_verified():
    raise AssertionError("only a verifying account waits on verify_now")


def test_the_broker_runs_gh_as_the_agents_account(monkeypatch, ran):
    monkeypatch.setattr(github_agent, "view", lambda: AgentAccountView(state="ready"))
    monkeypatch.setattr(github_agent, "verify_now", never_verified)
    monkeypatch.setattr(github_agent, "backend_gh",
                        lambda _account: ("/agent/gh", {"GH_CONFIG_DIR": "/agent/home"}))
    _run_gh(["api", "user"], timeout=5)
    assert ran["argv"] == ["/agent/gh", "api", "user"]
    assert ran["env"]["GH_CONFIG_DIR"] == "/agent/home" and ran["env"]["GH_HOST"] == "github.com"


def test_an_agents_account_that_is_not_ready_reads_locked_never_as_the_person(monkeypatch, ran):
    monkeypatch.setenv("GH_TOKEN", "person-credential-value")
    monkeypatch.setattr(github_agent, "view", lambda: AgentAccountView(
        state="needs_auth", cause="signed_out", binary="/agent/gh", home="/agent/home"))
    monkeypatch.setattr(github_agent, "verify_now", never_verified)
    _run_gh(["api", "user"], timeout=5)
    assert ran["argv"][0] == "/agent/gh"
    assert ran["env"]["GH_CONFIG_DIR"] == LOCKED_HOME and "GH_TOKEN" not in ran["env"]


def test_the_notifications_feed_is_read_as_the_person_never_the_agents_account(monkeypatch, ran):
    from events.connectors import github_notifications as notifications

    def never(*_args):
        raise AssertionError("the person's feed never runs as the agents' account")

    async def one_cycle(runner, failures=0):
        runner(["api", "--method", "GET", "-i", "notifications"], timeout=5)
        return None, 0

    monkeypatch.setattr(github_agent, "view", never)
    monkeypatch.setattr(github_agent, "backend_gh", never)
    monkeypatch.setattr(notifications, "run_cycle", one_cycle)
    monkeypatch.delenv("GH_CONFIG_DIR", raising=False)
    asyncio.run(notifications.notifications_poll_loop())
    assert ran["argv"] == ["gh", "api", "--method", "GET", "-i", "notifications"]
    assert ran["env"]["GH_HOST"] == "github.com" and "GH_CONFIG_DIR" not in ran["env"]


@pytest.mark.parametrize("verified, home", [
    (AgentAccountView(state="ready", binary="/agent/gh", home="/agent/home"), "/agent/home"),
    (AgentAccountView(state="needs_auth", cause="signed_out", binary="/agent/gh", home="/agent/home"), LOCKED_HOME),
])
def test_a_verifying_account_waits_once_for_its_answer(monkeypatch, ran, verified, home):
    calls = []
    monkeypatch.setattr(github_agent, "view", lambda: AgentAccountView(
        state="unverified", cause="verifying", binary="/agent/gh", home="/agent/home"))
    monkeypatch.setattr(github_agent, "verify_now", lambda: calls.append(1) or verified)
    _run_gh(["api", "user"], timeout=5)
    assert calls == [1]
    assert ran["argv"][0] == "/agent/gh" and ran["env"]["GH_CONFIG_DIR"] == home
