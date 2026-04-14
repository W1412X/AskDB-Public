from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, ErrorAttributionOutput


SYSTEM_RULES = [
    "你只做归因与修复建议，不直接修复 schema、sql、intent 或结果。",
    "你必须区分当前阶段自己的错误、上游工件导致的错误、以及环境类错误。",
    "owner_stage 必须是最应负责的阶段，而不是机械等于 current_stage。",
    "repair_action 必须与 owner_stage 和错误类型一致。",
    "error_type 只能是 ENVIRONMENT、UPSTREAM 或 LOCAL。",
    "message 必须简洁、可执行，不要输出大段闲聊。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "owner_stage、current_stage、error_code、message、repair_action、error_type、confidence 都必须给出。",
    "若能明确归因到上游阶段，不要错误归到当前阶段。",
    "repair_action 必须是可执行修复枚举：REBUILD_SCHEMA、ENRICH_SCHEMA、REPLAN_RA、RERENDER_SQL、"
    "REVALIDATE_SQL、REEXECUTE_SQL、REINTERPRET_RESULT、RETRY_CURRENT；需要终止用 STOP。",
    "不要输出 ASK_USER（意图修复循环无法挂起提问）；应选具体修复动作或 STOP。",
]


class _Payload(BaseModel):
    current_stage: str
    current_input: dict = {}
    error_message: str
    upstream_artifacts: dict = {}


class ErrorAttributionAgent(BaseAgent[_Payload, ErrorAttributionOutput]):
    name = "error_attribution"
    description = "错误归因"
    output_model = ErrorAttributionOutput

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的错误责任归因代理。",
            mission_line="你的唯一职责是：根据当前阶段输入、错误信息和上游工件，判断责任阶段并给出修复动作建议。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"current_stage": {p.current_stage!r}, "current_input": {p.current_input}, '
                f'"error_message": {p.error_message!r}, "upstream_artifacts": {p.upstream_artifacts}}}'
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )
