from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from config import get_llm, get_settings_manager
from utils.log_console import LogCategory
from utils.logger import get_logger

from .base_agent import BaseAgent
from ..contracts import AgentStep, ModuleError
from ..enums import RepairAction, StageName
from ..tools.registry import ToolRegistry


class AgentRunResult(BaseModel):
    ok: bool
    output: BaseModel | None = None
    error: str = ""
    raw_output: str = ""
    tool_trace: list[dict[str, Any]] = Field(default_factory=list)


def _extract_json_object(text: str) -> Any:
    raw = str(text or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        return json.loads(raw[start : end + 1])
    raise ValueError("invalid json output")


def _invoke_chat_llm(model: Any, system_prompt: str, user_prompt: str) -> str:
    from langchain_core.messages import HumanMessage, SystemMessage

    raw = model.invoke([SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)])
    return str(getattr(raw, "content", raw) or "")


def _finalize_agent_json_output(
    agent: BaseAgent,
    payload: BaseModel | dict,
    raw_output_text: str,
    tool_trace: list[dict[str, Any]] | None = None,
) -> BaseModel:
    obj = _extract_json_object(raw_output_text)
    if tool_trace and "tool_trace" not in obj:
        fields = getattr(agent.output_model, "model_fields", {})
        if "tool_trace" in fields:
            obj["tool_trace"] = tool_trace
    result = agent.output_model.model_validate(obj)
    agent.post_validate(payload, result)
    return result


class AgentRunner:
    def __init__(self, registry: ToolRegistry | None = None) -> None:
        self.logger = get_logger("query_workflow")
        self.registry = registry or ToolRegistry()

    def _retry_limits(self) -> tuple[int, int]:
        qw = get_settings_manager().config.stages.query_workflow
        retries = max(0, int(qw.max_json_retries))
        semantic_retries = max(0, int(qw.max_semantic_retries))
        return retries, semantic_retries

    def run(
        self,
        agent: BaseAgent,
        payload: BaseModel | dict,
        steps: list[AgentStep] | None = None,
    ) -> AgentRunResult:
        try:
            model = get_llm(agent.model_name or None)
        except Exception as exc:
            return AgentRunResult(ok=False, error=str(exc))
        system_prompt = agent.build_system_prompt()
        user_prompt = agent.build_user_prompt(payload, steps)
        if agent.supports_tool_calling():
            return self._run_with_tools(model, agent, payload, system_prompt, user_prompt)
        schema = agent.output_model.model_json_schema()
        retries, semantic_retries = self._retry_limits()
        current_user_prompt = user_prompt
        validation_error_text = ""
        raw_output_text = ""
        semantic_retry_count = 0

        for _ in range(retries + 1):
            try:
                self.logger.info(
                    "agent start",
                    agent=agent.name,
                    model_name=agent.model_name or "",
                    category=LogCategory.AGENT,
                )
                if hasattr(model, "with_structured_output"):
                    structured = model.with_structured_output(agent.output_model)
                    from langchain_core.messages import HumanMessage, SystemMessage

                    result = structured.invoke([SystemMessage(content=system_prompt), HumanMessage(content=current_user_prompt)])
                    if isinstance(result, agent.output_model):
                        agent.post_validate(payload, result)
                        self.logger.info("agent success", agent=agent.name, category=LogCategory.SUCCESS)
                        return AgentRunResult(ok=True, output=result, raw_output=result.model_dump_json())
                raw_output_text = _invoke_chat_llm(model, system_prompt, current_user_prompt)
                result = _finalize_agent_json_output(agent, payload, raw_output_text, tool_trace=None)
                self.logger.info("agent success", agent=agent.name, category=LogCategory.SUCCESS)
                return AgentRunResult(ok=True, output=result, raw_output=raw_output_text)
            except ValueError as exc:
                validation_error_text = str(exc)
                if semantic_retry_count < semantic_retries:
                    semantic_retry_count += 1
                    current_user_prompt = (
                        f"{user_prompt}\n\n"
                        "你上一次输出虽然是合法 JSON，但不满足业务约束。\n"
                        "请仅根据以下问题修复，不要改变未被指出的字段。\n\n"
                        f"问题：\n{validation_error_text}\n"
                    )
                    continue
            except ValidationError as exc:
                validation_error_text = str(exc)
            except Exception as exc:
                validation_error_text = str(exc)
            self.logger.warning(
                "agent retry",
                agent=agent.name,
                error=validation_error_text,
                category=LogCategory.AGENT,
            )
            current_user_prompt = (
                f"{user_prompt}\n\n"
                "你上一次输出不符合 JSON 协议。\n"
                "请严格根据以下校验错误修复输出。\n"
                "不要解释，不要补充自然语言，只返回修正后的 JSON。\n\n"
                f"校验错误：\n{validation_error_text}\n\n"
                f"目标输出 schema：\n{json.dumps(schema, ensure_ascii=False)}\n"
            )
        self.logger.error("agent failed", agent=agent.name, error=validation_error_text, category=LogCategory.AGENT)
        return AgentRunResult(ok=False, error=validation_error_text, raw_output=raw_output_text)

    def _run_with_tools(
        self,
        model: Any,
        agent: BaseAgent,
        payload: BaseModel | dict,
        system_prompt: str,
        user_prompt: str,
    ) -> AgentRunResult:
        if not hasattr(model, "bind_tools"):
            return AgentRunResult(ok=False, error="model does not support tool calling")
        from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

        bound = model.bind_tools(agent.resolve_tool_specs(self.registry))
        messages: list[Any] = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        repair_schema = json.dumps(agent.output_model.model_json_schema(), ensure_ascii=False)
        finalize = _finalize_agent_json_output
        raw_output_text = ""
        qw = get_settings_manager().config.stages.query_workflow
        max_rounds = max(1, int(qw.agent_runner_tool_round_cap))
        max_tool_rounds = getattr(agent, "max_tool_rounds", None)
        if max_tool_rounds is not None:
            max_rounds = max(max_rounds, int(max_tool_rounds) + 1)
        def _normalize_tool_call(call: Any) -> dict[str, Any]:
            if isinstance(call, dict):
                name = str(call.get("name") or call.get("tool") or "")
                tool_call_id = str(call.get("id") or call.get("tool_call_id") or "")
                args = call.get("args", call.get("arguments", {}))
            else:
                name = str(getattr(call, "name", "") or getattr(call, "tool", "") or "")
                tool_call_id = str(getattr(call, "id", "") or getattr(call, "tool_call_id", "") or "")
                args = getattr(call, "args", None)
                if args is None:
                    args = getattr(call, "arguments", {})
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {}
            if args is None:
                args = {}
            return {"name": name, "id": tool_call_id, "args": dict(args or {})}

        tool_trace: list[dict[str, Any]] = []
        for round_index in range(1, max_rounds + 1):
            self.logger.info(
                "agent start",
                agent=agent.name,
                model_name=agent.model_name or "",
                tool_round=round_index,
                category=LogCategory.AGENT,
            )
            try:
                response = bound.invoke(messages)
            except Exception as exc:
                self.logger.warning("agent retry", agent=agent.name, error=str(exc), category=LogCategory.AGENT)
                raw_output_text = str(exc)
                continue
            raw_tool_calls = list(getattr(response, "tool_calls", None) or [])
            tool_calls = [_normalize_tool_call(call) for call in raw_tool_calls]
            if tool_calls and any(not call.get("id") for call in tool_calls):
                self.logger.warning(
                    "agent tool calls missing id",
                    agent=agent.name,
                    tool_round=round_index,
                    category=LogCategory.TOOL,
                )
                messages.append(
                    HumanMessage(
                        content=(
                            "本轮 tool call 缺少 tool_call_id，无法执行。"
                            "请重新发起工具调用，并确保每个 tool call 都有 id。"
                        )
                    )
                )
                continue
            if tool_calls:
                if max_tool_rounds is not None and round_index >= int(max_tool_rounds):
                    self.logger.warning(
                        "agent tool rounds capped",
                        agent=agent.name,
                        tool_round=round_index,
                        cap=int(max_tool_rounds),
                        category=LogCategory.TOOL,
                    )
                    messages.append(response)
                    for call in tool_calls:
                        tool_name = str(call.get("name") or "")
                        tool_call_id = str(call.get("id") or "")
                        messages.append(
                            ToolMessage(
                                content=json.dumps(
                                    {
                                        "error": "tool_round_limit",
                                        "limit": int(max_tool_rounds),
                                        "tool_name": tool_name,
                                    },
                                    ensure_ascii=False,
                                ),
                                tool_call_id=tool_call_id,
                                name=tool_name,
                            )
                        )
                    messages.append(
                        HumanMessage(
                            content=(
                                "已接近工具轮次上限，请停止继续调用工具，直接总结已发现的信息，"
                                "并提示规划器缩小任务。"
                            )
                        )
                    )
                    continue
                limit = getattr(agent, "max_tool_calls_per_round", None)
                if limit is not None and len(tool_calls) > int(limit):
                    self.logger.warning(
                        "agent tool calls exceeded",
                        agent=agent.name,
                        tool_round=round_index,
                        tool_count=len(tool_calls),
                        limit=int(limit),
                        category=LogCategory.TOOL,
                    )
                    # Must respond to all tool_call_ids if we include this assistant message.
                    messages.append(response)
                    for call in tool_calls:
                        tool_name = str(call.get("name") or "")
                        tool_call_id = str(call.get("id") or "")
                        messages.append(
                            ToolMessage(
                                content=json.dumps(
                                    {
                                        "error": "tool_calls_exceeded_limit",
                                        "limit": int(limit),
                                        "tool_name": tool_name,
                                    },
                                    ensure_ascii=False,
                                ),
                                tool_call_id=tool_call_id,
                                name=tool_name,
                            )
                        )
                    messages.append(
                        HumanMessage(
                            content=(
                                f"本轮最多只能调用 {int(limit)} 个工具。"
                                "请减少工具调用数量，并仅保留最关键的一个（或允许的数量）。"
                            )
                        )
                    )
                    continue
                self.logger.info(
                    "agent tool calls",
                    agent=agent.name,
                    tool_round=round_index,
                    tool_count=len(tool_calls),
                    tool_names=[item.get("name", "") for item in tool_calls],
                    category=LogCategory.TOOL,
                )
                messages.append(response)
                for call in tool_calls:
                    tool_name = str(call.get("name") or "")
                    arguments = dict(call.get("args") or {})
                    tool_call_id = str(call.get("id") or "")
                    self.logger.info(
                        "agent tool call start",
                        agent=agent.name,
                        tool_name=tool_name,
                        arguments=arguments,
                        category=LogCategory.TOOL,
                    )
                    try:
                        tool_result = self.registry.invoke(tool_name, arguments)
                        self.logger.info(
                            "agent tool call success",
                            agent=agent.name,
                            tool_name=tool_name,
                            tool_result=tool_result,
                            category=LogCategory.TOOL,
                        )
                        tool_trace.append(
                            {
                                "tool": tool_name,
                                "arguments": arguments,
                                "result": tool_result,
                            }
                        )
                    except Exception as exc:
                        tool_result = {"error": str(exc), "tool_name": tool_name}
                        self.logger.warning(
                            "agent tool call failed",
                            agent=agent.name,
                            tool_name=tool_name,
                            error=str(exc),
                            category=LogCategory.TOOL,
                        )
                        tool_trace.append(
                            {
                                "tool": tool_name,
                                "arguments": arguments,
                                "error": str(exc),
                            }
                        )
                    messages.append(
                        ToolMessage(
                            content=json.dumps(tool_result, ensure_ascii=False),
                            tool_call_id=tool_call_id,
                            name=tool_name,
                        )
                    )
                continue
            content = getattr(response, "content", response)
            raw_output_text = str(content or "")
            try:
                result = finalize(agent, payload, raw_output_text, tool_trace=tool_trace)
                self.logger.info("agent success", agent=agent.name, category=LogCategory.SUCCESS)
                return AgentRunResult(ok=True, output=result, raw_output=raw_output_text, tool_trace=tool_trace)
            except Exception as exc:
                self.logger.warning("agent retry", agent=agent.name, error=str(exc), category=LogCategory.AGENT)
                messages.append(response)
                messages.append(
                    HumanMessage(
                        content=(
                            "你上一次没有返回符合协议的最终 JSON。\n"
                            f"错误：{exc}\n"
                            f"目标输出 schema：\n{repair_schema}\n"
                            "如果还缺证据，请继续调用工具；如果证据已足够，请只返回最终 JSON。"
                        )
                    )
                )
        self.logger.error(
            "agent failed",
            agent=agent.name,
            error=raw_output_text or "tool calling exceeded max rounds",
            category=LogCategory.AGENT,
        )
        return AgentRunResult(ok=False, error=raw_output_text or "tool calling exceeded max rounds", raw_output=raw_output_text, tool_trace=tool_trace)


def agent_failure_as_module_error(message: str, current_stage: StageName) -> ModuleError:
    return ModuleError(
        status="FATAL_ERROR",
        owner_stage=current_stage,
        current_stage=current_stage,
        error_code="AGENT_RUN_FAILED",
        message=message,
        hint=message,
        repair_action=RepairAction.STOP,
    )
