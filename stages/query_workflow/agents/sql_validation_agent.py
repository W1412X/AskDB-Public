from __future__ import annotations

import json

from pydantic import BaseModel, Field

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, SQLValidationDecision


SYSTEM_RULES = [
    "你的职责是判断候选 SQL 是否真正满足用户原始查询需求。",
    "你不是 SQL 语法校验器，不负责解释数据库报错，也不负责重写 SQL。",
    "你必须同时参考 intent、schema、结构化 RA 规划、候选 SQL 与上下文反馈，判断是否存在语义偏差。",
    "RA 规划是结构化的：包含 ctes/body、from_derived、joins(join_kind/on_expr/lateral)、filters(clause/predicate_kind)、window_definitions、window_expressions、group_by_variant/grouping_sets、order_by 的结构化排序项、set_branches、set_combine_operator、distinct、limit/offset、for_update、optimizer_hints。",
    "如果 SQL 额外添加了题目没有要求的限制，或者遗漏了题目明确要求的条件，必须判定 fail。",
    "如果 SQL 把结构化 RA 中表达的格式、长度、精度、枚举、范围、join 关系、聚合口径、集合运算、窗口、CTE、派生表或排序规则写窄或写宽，必须判定 fail。",
    "如果 SQL 与意图一致、且未引入多余约束，可判定 ok。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "status 只能是 ok 或 fail。",
    "若 fail，reason 必须明确指出不满足用户需求的原因，且要尽量具体。",
    "reason 需说明是多加约束、少了约束、对象选错、口径错、方法不对，还是对结构化 RA 字段的误读。",
    "若 intent 或 schema 不足以支撑 SQL，则应判定 fail 并说明缺失点。",
]


class _Payload(BaseModel):
    intent: str
    known_information_text: str = ""
    resolved_schema: dict = Field(default_factory=dict)
    ra_plan: dict = Field(default_factory=dict)
    sql_render_result: dict = Field(default_factory=dict)
    selected_sql: str = ""
    sql_dialect: str = "mysql"
    sql_validation_feedback: str = ""


class SQLValidationAgent(BaseAgent[_Payload, SQLValidationDecision]):
    name = "sql_validation_agent"
    description = "SQL 语义校验"
    output_model = SQLValidationDecision

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 SQL 语义校验代理。",
            mission_line="你的唯一职责是：判断候选 SQL 是否满足用户原始查询需求，并给出 ok/fail 结论。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=json.dumps(
                {
                    "intent": p.intent,
                    "known_information_text": p.known_information_text,
                    "resolved_schema": p.resolved_schema,
                    "ra_plan": p.ra_plan,
                    "sql_render_result": p.sql_render_result,
                    "selected_sql": p.selected_sql,
                    "sql_dialect": p.sql_dialect,
                    "sql_validation_feedback": p.sql_validation_feedback,
                },
                ensure_ascii=False,
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, payload: _Payload | dict, output: SQLValidationDecision) -> None:
        if output.status == "fail" and not str(output.reason or "").strip():
            raise ValueError("failed sql validation output must contain reason")
