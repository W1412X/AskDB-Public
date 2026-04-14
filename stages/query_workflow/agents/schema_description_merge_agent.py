from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep


class SchemaDescriptionMergeResult(BaseModel):
    merged_description: str


SYSTEM_RULES = [
    "你的职责是把两段 schema 描述合并为一段无重复、无遗漏的描述。",
    "必须保留关键信息，不得编造不存在的字段或事实。",
    "输出必须简洁，避免同义重复。",
    "只返回严格 JSON，不要输出 markdown，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "返回字段 merged_description。",
    "若两段描述语义一致，应保留信息更全的一段。",
    "若两段描述互补，应合并为单段完整描述。",
]


class _Payload(BaseModel):
    existing_description: str = ""
    incoming_description: str = ""


class SchemaDescriptionMergeAgent(BaseAgent[_Payload, SchemaDescriptionMergeResult]):
    name = "schema_description_merger"
    description = "schema 描述合并"
    output_model = SchemaDescriptionMergeResult

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 schema 描述合并代理。",
            mission_line="你的唯一职责是：合并两段 schema 描述文本，去重并保留完整信息。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"existing_description": {p.existing_description!r}, "incoming_description": {p.incoming_description!r}}}'
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

