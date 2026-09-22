from __future__ import annotations

from typing import Any, AsyncIterator, Iterator

import pytest

from agno.agent import Agent
from agno.models.base import Model
from agno.models.message import Message
from agno.models.response import ModelResponse
from agno.run.agent import RunOutput
from agno.run.base import RunContext, RunStatus
from agno.tools.decorator import tool


class _ScriptedCaptureModel(Model):
    """Provider double that proposes one HITL call, then observes its rejection."""

    def __init__(self) -> None:
        super().__init__(id="dynamic-hitl", name="dynamic-hitl", provider="test")
        self.calls = 0
        self.contexts: list[list[Message]] = []
        self.provider_tool_names: list[list[str]] = []

    def _next_response(self, **kwargs: Any) -> ModelResponse:
        self.calls += 1
        self.contexts.append([message.model_copy(deep=True) for message in kwargs["messages"]])
        self.provider_tool_names.append(
            [item["function"]["name"] for item in kwargs.get("tools") or [] if item.get("type") == "function"]
        )
        if self.calls == 1:
            return ModelResponse(
                tool_calls=[
                    {
                        "id": "proposal-1",
                        "type": "function",
                        "function": {
                            "name": "present_for_review",
                            "arguments": '{"artifact": "draft-v1"}',
                        },
                    }
                ]
            )
        return ModelResponse(content="rejection received")

    def invoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        return self._next_response(**kwargs)

    async def ainvoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        return self._next_response(**kwargs)

    def invoke_stream(self, *args: Any, **kwargs: Any) -> Iterator[ModelResponse]:
        yield self._next_response(**kwargs)

    async def ainvoke_stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[ModelResponse]:
        yield self._next_response(**kwargs)

    def _parse_provider_response(self, response: Any, **kwargs: Any) -> ModelResponse:
        return response

    def _parse_provider_response_delta(self, response: Any) -> ModelResponse:
        return response


def _dynamic_agent(
    *, hook_contexts: list[list[Message] | None] | None = None
) -> tuple[Agent, _ScriptedCaptureModel, dict[str, str], list[str]]:
    phase = {"name": "present"}
    executions: list[str] = []

    def record_hook_context(run_context: RunContext) -> None:
        if hook_contexts is not None:
            hook_contexts.append(list(run_context.messages) if run_context.messages is not None else None)

    @tool(requires_confirmation=True, pre_hook=record_hook_context)
    def present_for_review(artifact: str) -> str:
        executions.append(f"present:{artifact}")
        return "presented"

    def inspect_current() -> str:
        executions.append("inspect")
        return "inspected"

    def phase_tools() -> list[Any]:
        if phase["name"] == "present":
            return [present_for_review]
        if phase["name"] == "inspect":
            return [inspect_current]
        return []

    model = _ScriptedCaptureModel()
    agent = Agent(
        model=model,
        tools=phase_tools,
        cache_callables=False,
        telemetry=False,
    )
    return agent, model, phase, executions


def _reject_paused_proposal(paused: RunOutput, note: str) -> RunOutput:
    persisted = RunOutput.from_dict(paused.to_dict())
    assert persisted.status == RunStatus.paused
    assert persisted.tools and len(persisted.tools) == 1
    persisted.tools[0].confirmed = False
    persisted.tools[0].confirmation_note = note
    return persisted


def _assert_rejection_trajectory(
    result: RunOutput,
    model: _ScriptedCaptureModel,
    executions: list[str],
    note: str,
    current_tool_names: list[str],
) -> None:
    assert result.status == RunStatus.completed
    assert result.content == "rejection received"
    assert model.calls == 2
    assert executions == []
    assert model.provider_tool_names == [["present_for_review"], current_tool_names]

    rejection = next(
        message for message in model.contexts[1] if message.role == "tool" and message.tool_call_id == "proposal-1"
    )
    assert rejection.content == note
    assert rejection.tool_name == "present_for_review"
    assert rejection.tool_args == {"artifact": "draft-v1"}
    assert rejection.tool_call_error is True


@pytest.mark.parametrize(
    ("next_phase", "current_tool_names"),
    [("inspect", ["inspect_current"]), ("empty", [])],
)
@pytest.mark.parametrize("stream", [False, True])
def test_sync_rejection_survives_changed_callable_tool_surface(
    stream: bool,
    next_phase: str,
    current_tool_names: list[str],
) -> None:
    agent, model, phase, executions = _dynamic_agent()
    paused = agent.run("Prepare the artifact for review.")
    note = "Keep the same owner and revise the comparative."
    persisted = _reject_paused_proposal(paused, note)

    phase["name"] = next_phase
    if stream:
        events = list(
            agent.continue_run(
                persisted,
                stream=True,
                stream_events=True,
                yield_run_output=True,
            )
        )
        result = next(event for event in events if isinstance(event, RunOutput))
    else:
        result = agent.continue_run(persisted)

    _assert_rejection_trajectory(result, model, executions, note, current_tool_names)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("next_phase", "current_tool_names"),
    [("inspect", ["inspect_current"]), ("empty", [])],
)
@pytest.mark.parametrize("stream", [False, True])
async def test_async_rejection_survives_changed_callable_tool_surface(
    stream: bool,
    next_phase: str,
    current_tool_names: list[str],
) -> None:
    agent, model, phase, executions = _dynamic_agent()
    paused = await agent.arun("Prepare the artifact for review.")
    note = "Keep the same owner and revise the comparative."
    persisted = _reject_paused_proposal(paused, note)

    phase["name"] = next_phase
    if stream:
        events = [
            event
            async for event in agent.acontinue_run(
                persisted,
                stream=True,
                stream_events=True,
                yield_run_output=True,
            )
        ]
        result = next(event for event in events if isinstance(event, RunOutput))
    else:
        result = await agent.acontinue_run(persisted)

    _assert_rejection_trajectory(result, model, executions, note, current_tool_names)


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_async_confirmed_continuation_hook_receives_current_transcript(stream: bool) -> None:
    hook_contexts: list[list[Message] | None] = []
    agent, _model, _phase, executions = _dynamic_agent(hook_contexts=hook_contexts)
    initial_input = "Prepare the artifact for review."
    paused = await agent.arun(initial_input)
    persisted = RunOutput.from_dict(paused.to_dict())
    assert persisted.status == RunStatus.paused
    assert persisted.tools and len(persisted.tools) == 1
    persisted.tools[0].confirmed = True

    if stream:
        events = [
            event
            async for event in agent.acontinue_run(
                persisted,
                stream=True,
                stream_events=True,
                yield_run_output=True,
            )
        ]
        result = next(event for event in events if isinstance(event, RunOutput))
    else:
        result = await agent.acontinue_run(persisted)

    assert result.status == RunStatus.completed
    assert executions == ["present:draft-v1"]
    assert len(hook_contexts) == 1
    hook_messages = hook_contexts[0]
    assert hook_messages is not None
    assert any(message.role == "user" and message.content == initial_input for message in hook_messages)
    assert any(
        message.role == "assistant" and message.tool_calls and message.tool_calls[0]["id"] == "proposal-1"
        for message in hook_messages
    )
