"""A background resume must not reinsert its own stored messages as history."""

import pytest

from agno.agent import Agent
from agno.agent._messages import get_continue_run_messages
from agno.models.message import Message
from agno.run import RunContext
from agno.run.agent import RunOutput
from agno.run.base import RunStatus
from agno.session import AgentSession


@pytest.mark.parametrize("status", list(RunStatus))
def test_resume_preserves_previous_turn_and_current_tool_transcript_once(status):
    agent = Agent(add_history_to_context=True)
    agent.num_history_runs = None
    current_messages = [
        Message(role="system", content="system"),
        Message(role="user", content="prepare statements"),
        Message(role="assistant", content="read source"),
    ]
    previous = RunOutput(
        run_id="previous",
        status=RunStatus.completed,
        messages=[Message(role="user", content="keep the comparative figures")],
    )
    current = RunOutput(run_id="current", status=status, messages=current_messages)
    session = AgentSession(session_id="session", runs=[previous, current])
    context = RunContext(run_id="current", session_id="session")

    result = get_continue_run_messages(
        agent,
        input=current_messages,
        session=session,
        run_context=context,
    )

    assert [m.content for m in result.messages] == [
        "system",
        "keep the comparative figures",
        "prepare statements",
        "read source",
    ]
    assert result.messages[1].from_history is True
    assert not result.messages[2].from_history
    assert previous.messages[0].from_history is False
    assert session.runs == [previous, current]
    assert current.status == status
    assert context.messages is result.messages


def test_current_resume_does_not_consume_a_previous_run_history_slot():
    agent = Agent(add_history_to_context=True, num_history_runs=1)
    current_messages = [Message(role="user", content="current")]
    session = AgentSession(
        session_id="session",
        runs=[
            RunOutput(run_id="old", status=RunStatus.completed, messages=[Message(role="user", content="old")]),
            RunOutput(
                run_id="previous", status=RunStatus.completed, messages=[Message(role="user", content="previous")]
            ),
            RunOutput(run_id="current", status=RunStatus.running, messages=current_messages),
        ],
    )
    result = get_continue_run_messages(
        agent,
        input=current_messages,
        session=session,
        run_context=RunContext(run_id="current", session_id="session"),
    )
    assert [m.content for m in result.messages] == ["previous", "current"]


def test_explicit_current_identity_takes_precedence_over_original_context():
    agent = Agent(add_history_to_context=True)
    agent.num_history_runs = None
    original = RunOutput(
        run_id="original", status=RunStatus.completed, messages=[Message(role="user", content="original")]
    )
    current = RunOutput(run_id="current", status=RunStatus.running, messages=[Message(role="user", content="current")])
    result = get_continue_run_messages(
        agent,
        input=current.messages,
        session=AgentSession(session_id="session", runs=[original, current]),
        run_context=RunContext(run_id="original", session_id="session"),
        current_run_id="current",
    )
    assert [message.content for message in result.messages] == ["original", "current"]
