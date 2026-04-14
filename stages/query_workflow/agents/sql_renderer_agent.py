from __future__ import annotations

import json

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, SQLRenderResult


SYSTEM_RULES = [
    "输入是 RA，不是自然语言，不要重新规划业务逻辑。",
    "不能改写业务语义，不能引入 resolved_schema 中不存在的对象。",
    "输出只能是 SQL 候选 JSON，不能输出解释性自然语言。",
    "候选数量应少而精，不要无意义发散。",
    "expected_columns 应与 RA output_contract 对齐。",
    "如果 render_feedback 非空，必须优先修复其中指出的 SQL 合法性或语义问题，避免重复同类错误。",
    "必须逐项翻译结构化 RA：ctes/body、from_derived、set_branches、window_definitions、window_expressions、group_by_variant/grouping_sets、distinct、limit/offset、for_update、optimizer_hints、order_by 的结构化排序项。",
    "filters 里的 clause 必须分别渲染为 WHERE / HAVING / QUALIFY；predicate_kind 为 exists/not_exists 时必须渲染为相关子查询谓词。",
    "joins 必须使用 join_kind/on_expr/lateral 渲染，不要回退到旧式 type 或两列等值假设。",
    "若使用 GROUP BY，SELECT 中所有非聚合表达式必须出现在 GROUP BY 中，确保兼容 ONLY_FULL_GROUP_BY；ROLLUP/CUBE/GROUPING SETS 必须按 group_by_variant/grouping_sets 结构渲染。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块。",
]

USER_REQUIREMENTS = [
    "status 填 SUCCESS 或 FAILED。",
    "若 SUCCESS，candidates 至少给出 1 条高质量 SQL。",
    "rationale 说明该候选为何成立；assumptions 只记录必要假设。",
    "不要输出 schema 外对象，不要省略关键 join 或聚合。",
    "若 render_feedback 非空，必须优先修复其中指出的 SQL 合法性问题，避免重复同类错误。",
    "不得把结构化 order_by 退化成字符串列表；不得忽略 ctes、derived、window、set 运算、for_update 或 optimizer_hints。",
]


class _Payload(BaseModel):
    ra: dict
    resolved_schema: dict
    sql_dialect: str = "mysql"
    render_feedback: str = ""


class SQLRendererAgent(BaseAgent[_Payload, SQLRenderResult]):
    name = "sql_renderer"
    description = "SQL 渲染"
    output_model = SQLRenderResult

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 SQL 渲染代理。",
            mission_line="你的唯一职责是：把关系代数 JSON 渲染为少量高质量 SQL 候选。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=json.dumps(
                {
                    "ra": p.ra,
                    "resolved_schema": p.resolved_schema,
                    "sql_dialect": p.sql_dialect,
                    "render_feedback": p.render_feedback,
                },
                ensure_ascii=False,
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, payload: _Payload | dict, output: SQLRenderResult) -> None:
        p = _Payload.model_validate(payload)
        if output.status == "SUCCESS" and not output.candidates:
            raise ValueError("successful sql renderer output must contain candidates")
        schema_refs = set()
        for db_name, db_obj in (p.resolved_schema or {}).get("databases", {}).items():
            for table_name in (db_obj or {}).get("tables", {}).keys():
                schema_refs.add(f"{db_name}.{table_name}".lower())
        for candidate in output.candidates:
            sql = str(candidate.sql or "").lower()
            if schema_refs and not any(ref in sql for ref in schema_refs):
                raise ValueError("rendered sql does not reference resolved schema objects")
