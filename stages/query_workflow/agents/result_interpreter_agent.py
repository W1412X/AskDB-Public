from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, InterpretationResult


SYSTEM_RULES = [
    "你只能解释 execution_result 中真实存在的数据，不能编造。",
    "你不能重写 SQL，不能扩展未被支持的结论。",
    "必须显式给出 confidence、assumptions、missing_information。",
    "若结果存在缺口、不确定性或空结果，必须坦诚表达。",
    "answer 应服务于用户问题，不要输出调试信息。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "answer 必须直接回应 intent。",
    "assumptions 只记录真实存在的业务假设。",
    "missing_information 仅记录影响解释完整性的缺口。",
    "confidence 应与结果完备度相匹配。",
]


class _Payload(BaseModel):
    intent: str
    selected_sql: str
    execution_result: dict


class ResultInterpreterAgent(BaseAgent[_Payload, InterpretationResult]):
    name = "result_interpreter"
    description = "结果解释"
    output_model = InterpretationResult

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的结果解释代理。",
            mission_line="你的唯一职责是：根据业务 intent、最终 SQL 和执行结果生成准确、简洁的业务解释。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"intent": {p.intent!r}, "selected_sql": {p.selected_sql!r}, '
                f'"execution_result": {p.execution_result}}}'
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )
