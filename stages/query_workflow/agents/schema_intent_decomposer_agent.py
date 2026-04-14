from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, IntentDecomposeResult


SYSTEM_RULES = [
    "你的职责是把 schema 构建意图拆成多个简单子任务，并形成 DAG 依赖。",
    "只关注 schema 构建，不输出 RA、SQL、执行策略。",
    "每个子任务只做一个最小 schema 目标（如发现主表、确认列、验证单跳 join、补充描述）。",
    "子任务必须是结构能力目标，禁止写具体过滤值/阈值（例如 maintenance、0.88）。",
    "dependent_intent_ids 仅表达 schema 构建先后依赖。",
    "最终 DAG 必须收敛：应存在一个最终汇总节点依赖前序关键节点。",
    "不要输出数据库名、表名、列名猜测；保持语义化任务描述。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "输出格式：{\"intents\":[{\"intent_id\":\"s_001\",\"intent\":\"...\",\"dependent_intent_ids\":[]}]}。",
    "intent_id 必须唯一，建议 s_001、s_002。",
    "若输出多个节点，DAG 末端应收敛到一个最终节点；若很简单可只输出单节点。",
    "若 schema 意图本身很简单，可只输出 1~2 个节点。",
]


class _Payload(BaseModel):
    schema_intent: str
    database_scope: list[str] = []
    current_schema: dict = {}


class SchemaIntentDecomposerAgent(BaseAgent[_Payload, IntentDecomposeResult]):
    name = "schema_intent_decomposer"
    description = "schema 构建意图分解"
    output_model = IntentDecomposeResult

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 schema 构建意图分解代理。",
            mission_line="你的唯一职责是：把一个 schema 构建意图拆成可执行的简单子任务 DAG。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"schema_intent": {p.schema_intent!r}, "database_scope": {p.database_scope}, '
                f'"current_schema": {p.current_schema}}}'
            ),
            requirement_prefix=["约束："],
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, _payload: _Payload | dict, output: IntentDecomposeResult) -> None:
        items = output.intents or []
        if not items:
            raise ValueError("schema sub-intent dag is empty")
        seen = set()
        for item in items:
            intent_id = str(item.intent_id or "").strip()
            intent_text = str(item.intent or "").strip()
            if not intent_id or not intent_text:
                raise ValueError("schema sub-intent id/text must be non-empty")
            if intent_id in seen:
                raise ValueError("duplicate schema sub-intent id")
            seen.add(intent_id)
        for item in items:
            for dep in item.dependent_intent_ids:
                if dep not in seen:
                    raise ValueError(f"unknown schema sub-intent dependency: {dep}")
