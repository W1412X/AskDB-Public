from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep


SYSTEM_RULES = [
    "你只汇总 intent_results，不重新推导 SQL，不重新解释 schema。",
    "不能重写或推翻单个 intent 的执行结果。",
    "不能编造未完成 intent 的答案。",
    "若只有部分成功，必须明确哪些部分已回答、哪些部分未完成。",
    "final_answer 应优先回答用户原始问题，再补充失败或缺失部分。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "final_answer 必须覆盖已完成 intent 的有效结论。",
    "对 FAILED 或未完成 intent，要明确说明未完成，不要省略。",
    "不要简单拼接 JSON，输出应是自然语言答案，但必须放在 JSON 字段中。",
]


class FinalSynthesisOutput(BaseModel):
    final_answer: str


class _Payload(BaseModel):
    original_query: str
    intent_results: list[dict]


class FinalSynthesizerAgent(BaseAgent[_Payload, FinalSynthesisOutput]):
    name = "final_synthesizer"
    description = "最终汇总"
    output_model = FinalSynthesisOutput

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的最终汇总代理。",
            mission_line="你的唯一职责是：把多个 intent 的结果组织成对用户可读的最终回答。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=f'{{"original_query": {p.original_query!r}, "intent_results": {p.intent_results}}}',
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )
