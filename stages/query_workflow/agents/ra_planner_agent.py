from __future__ import annotations

import json

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, RAPlanOutput


SYSTEM_RULES = [
    "你只能输出关系代数 JSON，不能直接输出 SQL。",
    "不能使用 schema 中不存在的数据库、表、列。",
    "不能引入无证据的 join、过滤条件或聚合口径。",
    "输出必须与当前 intent 严格一致，不要偷换业务语义。",
    "如果上游给出了 sql_validation_feedback，必须优先修正其中指出的语义偏差，不得重复同类错误。",
    "必须使用结构化 RA 字段表达 WHERE / HAVING / QUALIFY、集合运算、派生表、LATERAL、窗口、ROLLUP/CUBE/GROUPING SETS、结构化 ORDER BY、CTE、DISTINCT、LIMIT/OFFSET、FOR UPDATE 与 hints。",
    "joins 必须使用 join_kind，不再使用旧字段 type；filters 必须使用 clause 与 predicate_kind；order_by 必须是 RASortItem 列表，不再是字符串列表。",
    "CTE 必须是结构化的 ctes/body，不要用自然语言摘要；如果使用 set 运算，必须显式填写 plan_kind、set_branches、set_combine_operator。",
    "assumptions 只记录必要假设，不要填无意义空话。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "status 填 SUCCESS 或 FAILED。",
    "若 SUCCESS，ra 必须完整并与 schema 对齐。",
    "若证据不足以规划可靠 RA，可返回 FAILED，并给出最小必要 mark。",
    "output_contract.required_columns 必须与最终结果口径匹配。",
    "不要输出旧字段 type、order_by_items、as_summary 或其它已废弃结构。",
    "对于 set 运算、窗口、派生表、CTE、rollup/cube/grouping_sets、for_update 与 optimizer_hints，必须按结构化字段表达，不要折叠成文本。",
]


class _Payload(BaseModel):
    intent: str
    known_information_text: str = ""
    resolved_schema: dict = {}
    sql_validation_feedback: str = ""


class RAPlannerAgent(BaseAgent[_Payload, RAPlanOutput]):
    name = "ra_planner"
    description = "关系代数规划"
    output_model = RAPlanOutput

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的关系代数规划代理。",
            mission_line="你的唯一职责是：根据业务 intent 和 resolved schema 生成严格可转 SQL 的关系代数 JSON。",
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
                    "sql_validation_feedback": p.sql_validation_feedback,
                },
                ensure_ascii=False,
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, payload: _Payload | dict, output: RAPlanOutput) -> None:
        p = _Payload.model_validate(payload)
        allowed = set()
        table_columns: dict[tuple[str, str], set[str]] = {}
        for db_name, db_obj in (p.resolved_schema or {}).get("databases", {}).items():
            for table_name, table_obj in (db_obj or {}).get("tables", {}).items():
                allowed.add((db_name, table_name))
                cols = set((table_obj or {}).get("columns", {}).keys())
                table_columns[(db_name, table_name)] = cols
                for column_name in (table_obj or {}).get("columns", {}).keys():
                    allowed.add((f"{db_name}.{table_name}", column_name))
        alias_to_table: dict[str, tuple[str, str]] = {}
        for entity in output.ra.entities:
            if (entity.database, entity.table) not in allowed:
                raise ValueError(f"ra entity not in schema: {entity.database}.{entity.table}")
            alias_to_table[entity.alias] = (entity.database, entity.table)
            for column in entity.columns:
                if (f"{entity.database}.{entity.table}", column) not in allowed:
                    raise ValueError(f"ra column not in schema: {entity.database}.{entity.table}.{column}")
        for join in output.ra.joins:
            if join.left_alias not in alias_to_table:
                raise ValueError(f"ra join alias not found in entities: {join.left_alias}")
            if join.right_alias not in alias_to_table:
                raise ValueError(f"ra join alias not found in entities: {join.right_alias}")
            left_db_table = alias_to_table[join.left_alias]
            right_db_table = alias_to_table[join.right_alias]
            if join.left_column not in table_columns.get(left_db_table, set()):
                raise ValueError(
                    f"ra join column not in schema: {left_db_table[0]}.{left_db_table[1]}.{join.left_column}"
                )
            if join.right_column not in table_columns.get(right_db_table, set()):
                raise ValueError(
                    f"ra join column not in schema: {right_db_table[0]}.{right_db_table[1]}.{join.right_column}"
                )
        if output.ra.plan_kind == "set" and len(output.ra.set_branches) < 2:
            raise ValueError("set plan must contain at least two set_branches")
