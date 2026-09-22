"""RunErrorEvent.error_type is populated on generic (non-guardrail) failures.

The guardrail handlers always passed error_type; the generic except-Exception
handlers emitted typeless events, which downstream same-error detection (the
rollout error-storm stop) could not use. error_type carries the AgnoError slug
when the exception has one, else the Python class name -- stability per failure
class, not a taxonomy.
"""

from copy import deepcopy
from typing import Any, AsyncIterator, Iterator

import pytest

from agno.agent import Agent
from agno.exceptions import InputCheckError, ModelProviderError, OutputCheckError
from agno.guardrails.base import BaseGuardrail
from agno.metrics import RunMetrics
from agno.models.base import Model
from agno.models.message import Message
from agno.models.response import ModelResponse, ToolExecution
from agno.run.agent import RunErrorEvent, RunInput, RunOutput
from agno.run.base import RunContext, RunStatus
from agno.run.requirement import RunRequirement


class ExplodingModel(Model):
    def __init__(self, exc: BaseException) -> None:
        super().__init__(id="exploding", name="exploding", provider="test")
        self.exc = exc

    def __deepcopy__(self, memo: dict) -> "ExplodingModel":
        return type(self)(exc=self.exc)

    def invoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        raise self.exc

    async def ainvoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        raise self.exc

    def invoke_stream(self, *args: Any, **kwargs: Any) -> Iterator[ModelResponse]:
        raise self.exc
        yield  # pragma: no cover

    async def ainvoke_stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[ModelResponse]:
        raise self.exc
        yield  # pragma: no cover

    def _parse_provider_response(self, response: Any, **kwargs: Any) -> ModelResponse:
        return response

    def _parse_provider_response_delta(self, response: Any) -> ModelResponse:
        return response


class ToolThenErrorModel(ExplodingModel):
    """Run one real tool turn before the provider fails on its follow-up."""

    def __init__(self) -> None:
        super().__init__(RuntimeError("provider failed after durable tool"))
        self.calls = 0

    def _next(self) -> ModelResponse:
        self.calls += 1
        if self.calls == 1:
            return ModelResponse(
                tool_calls=[
                    {
                        "id": "durable-tool",
                        "type": "function",
                        "function": {"name": "record_effect", "arguments": "{}"},
                    }
                ]
            )
        raise self.exc

    def invoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        return self._next()

    async def ainvoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        return self._next()

    def invoke_stream(self, *args: Any, **kwargs: Any) -> Iterator[ModelResponse]:
        yield self._next()

    async def ainvoke_stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[ModelResponse]:
        yield self._next()


class CompletedModel(Model):
    """Return ordinary content so a post-run guardrail owns the failure."""

    def __init__(self) -> None:
        super().__init__(id="completed", name="completed", provider="test")

    def __deepcopy__(self, memo: dict) -> "CompletedModel":
        return type(self)()

    def invoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        return ModelResponse(content="ordinary model response")

    async def ainvoke(self, *args: Any, **kwargs: Any) -> ModelResponse:
        return self.invoke(*args, **kwargs)

    def invoke_stream(self, *args: Any, **kwargs: Any) -> Iterator[ModelResponse]:
        yield self.invoke(*args, **kwargs)

    async def ainvoke_stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[ModelResponse]:
        yield self.invoke(*args, **kwargs)

    def _parse_provider_response(self, response: Any, **kwargs: Any) -> ModelResponse:
        return response

    def _parse_provider_response_delta(self, response: Any) -> ModelResponse:
        return response


class BlockingInputGuardrail(BaseGuardrail):
    def check(self, run_input: RunInput) -> None:
        raise InputCheckError("input guardrail rejected the run")

    async def async_check(self, run_input: RunInput) -> None:
        raise InputCheckError("input guardrail rejected the run")


def _raise_output_guardrail(**kwargs: Any) -> None:
    raise OutputCheckError("output guardrail rejected the run")


def _assert_error_then_output(events: list[Any]) -> RunOutput:
    errors = [event for event in events if isinstance(event, RunErrorEvent)]
    outputs = [event for event in events if isinstance(event, RunOutput)]

    assert len(errors) == 1
    assert len(outputs) == 1
    assert events.index(errors[0]) < events.index(outputs[0])
    assert outputs[0].status == RunStatus.error
    return outputs[0]


def _assert_error_only(events: list[Any]) -> None:
    errors = [event for event in events if isinstance(event, RunErrorEvent)]
    assert len(errors) == 1
    assert not any(isinstance(event, RunOutput) for event in events)


def _failed_continuation_response() -> RunOutput:
    prior_tool = ToolExecution(
        tool_call_id="prior-tool",
        tool_name="record_effect",
        tool_args={},
        result="prior durable effect",
    )
    return RunOutput(
        run_id="continued-error-run",
        session_id="continued-error-session",
        status=RunStatus.error,
        input=RunInput(input_content="continue the retained owner request"),
        messages=[
            Message(role="system", content="retain this system context"),
            Message(role="user", content="retain this prior request"),
            Message(
                role="assistant",
                tool_calls=[
                    {
                        "id": "prior-tool",
                        "type": "function",
                        "function": {"name": "record_effect", "arguments": "{}"},
                    }
                ],
            ),
            Message(role="tool", content="prior durable effect", tool_call_id="prior-tool"),
        ],
        tools=[prior_tool],
        requirements=[RunRequirement(tool_execution=prior_tool)],
        session_state={"accepted_effect": {"revision": 1}},
        metrics=RunMetrics(
            input_tokens=13,
            output_tokens=5,
            additional_metrics={"checkpoint": "retained"},
        ),
    )


def _continuation_snapshot(response: RunOutput) -> dict[str, Any]:
    return deepcopy(
        {
            "run_id": response.run_id,
            "session_id": response.session_id,
            "input": response.input.to_dict() if response.input is not None else None,
            "messages": [message.to_dict() for message in response.messages or []],
            "tools": [
                (tool.tool_call_id, tool.tool_name, tool.tool_args, tool.result) for tool in response.tools or []
            ],
            "requirements": [
                requirement.tool_execution.tool_call_id if requirement.tool_execution is not None else None
                for requirement in response.requirements or []
            ],
            "session_state": dict(response.session_state or {}),
            "metrics": {
                "input_tokens": response.metrics.input_tokens if response.metrics is not None else None,
                "output_tokens": response.metrics.output_tokens if response.metrics is not None else None,
                "additional_metrics": (
                    dict(response.metrics.additional_metrics or {}) if response.metrics is not None else None
                ),
            },
        }
    )


def _assert_continuation_snapshot(
    output: RunOutput,
    expected: dict[str, Any],
    *,
    allow_new_messages: bool = False,
) -> None:
    assert output.run_id == expected["run_id"]
    assert output.session_id == expected["session_id"]
    assert (output.input.to_dict() if output.input is not None else None) == expected["input"]
    actual_messages = [message.to_dict() for message in output.messages or []]
    if allow_new_messages:
        assert actual_messages[: len(expected["messages"])] == expected["messages"]
    else:
        assert actual_messages == expected["messages"]
    assert [
        (tool.tool_call_id, tool.tool_name, tool.tool_args, tool.result) for tool in output.tools or []
    ] == expected["tools"]
    assert [
        requirement.tool_execution.tool_call_id if requirement.tool_execution is not None else None
        for requirement in output.requirements or []
    ] == expected["requirements"]
    assert output.session_state is not None
    assert all(output.session_state.get(key) == value for key, value in expected["session_state"].items())
    assert output.metrics is not None
    assert output.metrics.input_tokens == expected["metrics"]["input_tokens"]
    assert output.metrics.output_tokens == expected["metrics"]["output_tokens"]
    assert output.metrics.additional_metrics is not None
    assert all(
        output.metrics.additional_metrics.get(key) == value
        for key, value in expected["metrics"]["additional_metrics"].items()
    )


def test_sync_stream_error_event_carries_class_name():
    agent = Agent(model=ExplodingModel(RuntimeError("boom")), telemetry=False)
    events = list(agent.run(input="hi", stream=True, stream_events=True))
    error_events = [event for event in events if isinstance(event, RunErrorEvent)]
    assert error_events
    assert all(event.error_type == "RuntimeError" for event in error_events)
    assert not any(isinstance(event, RunOutput) for event in events)


async def test_async_stream_error_event_carries_class_name():
    agent = Agent(model=ExplodingModel(RuntimeError("boom")), telemetry=False)
    events = [event async for event in agent.arun(input="hi", stream=True, stream_events=True)]
    error_events = [event for event in events if isinstance(event, RunErrorEvent)]
    assert error_events
    assert all(event.error_type == "RuntimeError" for event in error_events)
    assert not any(isinstance(event, RunOutput) for event in events)


def test_sync_stream_yield_run_output_returns_canonical_failed_tool_turn_without_storage():
    effects: list[str] = []

    def record_effect() -> str:
        effects.append("recorded")
        return "durable tool result"

    agent = Agent(model=ToolThenErrorModel(), tools=[record_effect], telemetry=False)
    assert agent.db is None
    assert agent.cache_session is False

    events = list(
        agent.run(
            input="record the effect, then continue",
            stream=True,
            stream_events=True,
            yield_run_output=True,
        )
    )

    output = _assert_error_then_output(events)
    assert effects == ["recorded"]
    assert [message.role for message in output.messages or []] == ["user", "assistant", "tool"]
    assert output.tools and output.tools[0].tool_call_id == "durable-tool"
    assert output.tools[0].result == "durable tool result"


async def test_async_stream_yield_run_output_returns_canonical_failed_tool_turn_without_storage():
    effects: list[str] = []

    def record_effect() -> str:
        effects.append("recorded")
        return "durable tool result"

    agent = Agent(model=ToolThenErrorModel(), tools=[record_effect], telemetry=False)
    assert agent.db is None
    assert agent.cache_session is False

    events = [
        event
        async for event in agent.arun(
            input="record the effect, then continue",
            stream=True,
            stream_events=True,
            yield_run_output=True,
        )
    ]

    output = _assert_error_then_output(events)
    assert effects == ["recorded"]
    assert [message.role for message in output.messages or []] == ["user", "assistant", "tool"]
    assert output.tools and output.tools[0].tool_call_id == "durable-tool"
    assert output.tools[0].result == "durable tool result"


@pytest.mark.parametrize("yield_run_output", [False, True], ids=["event_only", "canonical_output"])
def test_sync_continuation_stream_error_preserves_canonical_response_when_requested(
    yield_run_output: bool,
):
    response = _failed_continuation_response()
    expected = _continuation_snapshot(response)
    agent = Agent(model=ExplodingModel(RuntimeError("continuation provider failure")), telemetry=False)
    assert agent.db is None
    assert agent.cache_session is False
    run_context = RunContext(
        run_id=response.run_id or "",
        session_id=response.session_id or "",
        session_state={"accepted_effect": {"revision": 1}},
    )

    events = list(
        agent.continue_run(
            response,
            stream=True,
            stream_events=True,
            yield_run_output=yield_run_output,
            run_context=run_context,
        )
    )

    if not yield_run_output:
        _assert_error_only(events)
        return
    output = _assert_error_then_output(events)
    _assert_continuation_snapshot(output, expected)


@pytest.mark.parametrize("yield_run_output", [False, True], ids=["event_only", "canonical_output"])
async def test_async_continuation_stream_error_preserves_canonical_response_when_requested(
    yield_run_output: bool,
):
    response = _failed_continuation_response()
    expected = _continuation_snapshot(response)
    agent = Agent(model=ExplodingModel(RuntimeError("continuation provider failure")), telemetry=False)
    assert agent.db is None
    assert agent.cache_session is False
    run_context = RunContext(
        run_id=response.run_id or "",
        session_id=response.session_id or "",
        session_state={"accepted_effect": {"revision": 1}},
    )

    events = [
        event
        async for event in agent.acontinue_run(
            response,
            stream=True,
            stream_events=True,
            yield_run_output=yield_run_output,
            run_context=run_context,
        )
    ]

    if not yield_run_output:
        _assert_error_only(events)
        return
    output = _assert_error_then_output(events)
    _assert_continuation_snapshot(output, expected)


def test_sync_initial_input_guardrail_yields_canonical_error_output_when_requested():
    agent = Agent(
        model=CompletedModel(),
        pre_hooks=[BlockingInputGuardrail()],
        telemetry=False,
    )

    events = list(
        agent.run(
            input="guard the initial request",
            stream=True,
            stream_events=True,
            yield_run_output=True,
        )
    )

    output = _assert_error_then_output(events)
    assert output.input and output.input.input_content == "guard the initial request"
    assert output.messages is None


async def test_async_initial_input_guardrail_yields_canonical_error_output_when_requested():
    agent = Agent(
        model=CompletedModel(),
        pre_hooks=[BlockingInputGuardrail()],
        telemetry=False,
    )

    events = [
        event
        async for event in agent.arun(
            input="guard the initial request",
            stream=True,
            stream_events=True,
            yield_run_output=True,
        )
    ]

    output = _assert_error_then_output(events)
    assert output.input and output.input.input_content == "guard the initial request"
    assert output.messages is None


def test_sync_continuation_output_guardrail_yields_retained_canonical_response():
    response = _failed_continuation_response()
    expected = _continuation_snapshot(response)
    agent = Agent(
        model=CompletedModel(),
        post_hooks=[_raise_output_guardrail],
        telemetry=False,
    )
    events = list(
        agent.continue_run(
            response,
            stream=True,
            stream_events=True,
            yield_run_output=True,
            run_context=RunContext(
                run_id=response.run_id or "",
                session_id=response.session_id or "",
                session_state={"accepted_effect": {"revision": 1}},
            ),
        )
    )

    output = _assert_error_then_output(events)
    _assert_continuation_snapshot(output, expected, allow_new_messages=True)
    assert output.messages and output.messages[-1].content == "ordinary model response"


async def test_async_continuation_output_guardrail_yields_retained_canonical_response():
    response = _failed_continuation_response()
    expected = _continuation_snapshot(response)
    agent = Agent(
        model=CompletedModel(),
        post_hooks=[_raise_output_guardrail],
        telemetry=False,
    )
    events = [
        event
        async for event in agent.acontinue_run(
            response,
            stream=True,
            stream_events=True,
            yield_run_output=True,
            run_context=RunContext(
                run_id=response.run_id or "",
                session_id=response.session_id or "",
                session_state={"accepted_effect": {"revision": 1}},
            ),
        )
    ]

    output = _assert_error_then_output(events)
    _assert_continuation_snapshot(output, expected, allow_new_messages=True)
    assert output.messages and output.messages[-1].content == "ordinary model response"


def test_non_streaming_run_still_reports_error_status():
    # Non-streaming responses do not retain events; the sweep's observable contract
    # there is unchanged (status + content), pinned so the doors stay symmetric.
    agent = Agent(model=ExplodingModel(RuntimeError("boom")), telemetry=False)
    response = agent.run(input="hi")
    assert response.status == RunStatus.error
    assert "boom" in str(response.content)


@pytest.mark.parametrize("continuation", [False, True])
def test_closing_at_canonical_error_output_preserves_terminal_state(continuation):
    from agno.db.in_memory import InMemoryDb

    agent = Agent(model=ExplodingModel(RuntimeError("boom")), db=InMemoryDb(), telemetry=False)
    if continuation:
        initial = agent.run("hi")
        events = agent.continue_run(
            run_id=initial.run_id, session_id=initial.session_id, stream=True, yield_run_output=True
        )
    else:
        events = agent.run("hi", stream=True, yield_run_output=True)
    output = next(event for event in events if isinstance(event, RunOutput))
    events.close()
    stored = agent.get_run_output(run_id=output.run_id, session_id=output.session_id)
    assert output.status == stored.status == RunStatus.error
    assert [message.to_dict() for message in stored.messages] == [message.to_dict() for message in output.messages]


@pytest.mark.asyncio
@pytest.mark.parametrize("continuation", [False, True])
async def test_aclosing_at_canonical_error_output_preserves_terminal_state(continuation):
    from agno.db.in_memory import InMemoryDb

    agent = Agent(model=ExplodingModel(RuntimeError("boom")), db=InMemoryDb(), telemetry=False)
    if continuation:
        initial = await agent.arun("hi")
        events = agent.acontinue_run(
            run_id=initial.run_id, session_id=initial.session_id, stream=True, yield_run_output=True
        )
    else:
        events = agent.arun("hi", stream=True, yield_run_output=True)
    async for event in events:
        if isinstance(event, RunOutput):
            output = event
            break
    else:
        pytest.fail("missing canonical error output")
    await events.aclose()
    stored = await agent.aget_run_output(run_id=output.run_id, session_id=output.session_id)
    assert output.status == stored.status == RunStatus.error
    assert [message.to_dict() for message in stored.messages] == [message.to_dict() for message in output.messages]


def test_error_type_of_prefers_agno_slug():
    from agno.utils.events import error_type_of

    assert error_type_of(RuntimeError("x")) == "RuntimeError"
    assert error_type_of(ModelProviderError(message="x")) == "model_provider_error"


async def test_agno_error_keeps_its_slug():
    # Typed agno exceptions keep the same snake_case slug the guardrail handlers
    # emit, so the field's vocabulary stays consistent across handler kinds.
    agent = Agent(model=ExplodingModel(ModelProviderError(message="provider down")), telemetry=False)
    events = [event async for event in agent.arun(input="hi", stream=True, stream_events=True)]
    error_events = [event for event in events if isinstance(event, RunErrorEvent)]
    assert error_events
    assert all(event.error_type == "model_provider_error" for event in error_events)
