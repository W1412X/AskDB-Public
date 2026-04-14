from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
import re

from ..contracts import AgentStep, SchemaToolOutput


SYSTEM_RULES = [
    "你可以使用 function calling 调用可装配工具，但不能写 schema。",
    "你不能输出最终 SQL、最终答案，也不能宣布 schema 已完成。",
    "你可以探索数据并陈述事实，但不能把值缺失当成 schema 缺口。",
    "你的探测目标是结构事实（表/列/关系/描述），不是验证业务查询中某个具体值是否存在。",
    "默认优先单轮调用一个最关键的工具；只有多个工具调用彼此独立且必须并行时，才允许同轮多个 tool calls。",
    "工具参数必须由你根据 tool_task、current_schema、known_information_text、database_scope 自己决定。",
    "探索优先按表：先表级发现，再对单一候选表做列级确认，最后才做单跳 join 验证。",
    "你的主要职责是发现表/列候选、扩展列描述、验证 join 关系；多跳路径发现可以交给专门工具，不要在没有工具证据时自行编造路径。",
    "当目标是发现可连接路径时，优先考虑 semantic_join_path_search 做路径召回；如果当前任务更适合表/列发现、单跳验证或其他工具，也可以选择其他工具，不必强制使用路径工具。",
    "当提及物理对象时（库/表/列/Join），必须使用完整路径 db.table.column，不得缩写或改写格式。",
    "对 tool_task.goal 中出现的物理表/列引用保持批判性：上游很可能在猜列名。你必须通过工具确认存在性后，才能写入 confirm。",
    "relation_validator 的 joinable 只代表数据上可连接，不代表语义正确；避免验证/确认明显语义不一致的 join（例如 factory_id 与 equipment_id）。优先验证同名列或典型外键命名（*_id）。",
    "证据不足时可以返回空候选，但不能编造工具发现。",
    "若接近轮次上限，应停止调用工具并总结已发现信息，提示规划器缩小任务。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "需要证据时先调用工具；只有证据足够后再输出最终 JSON。",
    "你的最终输出必须严格为：{confirm:{tables:[],columns:[],more_info:[],join_paths:[]}, suggestion:[]}",
    "confirm 只包含“已确认可写入 schema 的事实”。不要把“待验证/可能/建议”放进 confirm。",
    "suggestion 仅用于提示下一轮需要获取的信息或需要验证的 join/列，不可写入 schema。",
    "confirm.tables 仅允许 db.table；confirm.columns 与 more_info.column 仅允许 db.table.column；confirm.join_paths.left/right 仅允许 db.table.column。",
    "如果你无法验证某条 join（没调用或调用失败 relation_validator），就不要放入 confirm.join_paths，只能放入 suggestion。",
    "如果上游 goal 指向某个具体列名（例如 equipment.id），你必须先用 list_table_columns/schema_catalog 确认该列真实存在；不存在则不要写入 confirm，而应在 suggestion 中说明“列名不存在，需要确认真实列名”。",
    "diff 输出：confirm 中只输出 current_schema 里尚不存在的新对象（新表/新列/新 join/新的列补充信息）。已经存在于 current_schema 的对象不要重复输出到 confirm。",
    "如果通过 sql_explorer/样本值确认某个状态值在列中不存在，不要继续发散探索其它表来“推导”该状态；在 suggestion 中仅陈述事实，不要要求映射值。",
    "当已确认相关列存在时，禁止把“值不存在”包装成下一轮结构探索目标；可在 suggestion 中提示“结构已覆盖，值问题留给下游查询结果阶段处理”。",
]


class _Payload(BaseModel):
    tool_task: dict
    current_schema: dict = {}
    known_information_text: str = ""
    database_scope: list[str] = []


class SchemaToolAgent(BaseAgent[_Payload, SchemaToolOutput]):
    name = "schema_tool_agent"
    description = "schema 工具代理"
    output_model = SchemaToolOutput
    available_tools = [
        "semantic_table_search",
        "list_tables",
        "list_table_columns",
        "semantic_column_search",
        "semantic_join_path_search",
        "schema_catalog",
        "sql_explorer",
        "relation_validator",
    ]
    tool_choice_mode = "mixed"
    max_tool_calls_per_round = 1
    max_tool_rounds = 8

    _re_table = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
    _re_column = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 schema 工具代理。",
            mission_line="你的唯一职责是：理解调度需求，选择合适工具获取信息，并整理成服务下一轮 schemalink 的摘要。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"tool_task": {p.tool_task}, "current_schema": {p.current_schema}, '
                f'"known_information_text": {p.known_information_text!r}, "database_scope": {p.database_scope}}}'
            ),
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, _payload: _Payload, output: SchemaToolOutput) -> None:
        # Deterministic diff: even if the model repeats existing objects, we only keep
        # new confirmations not already present in current_schema.
        existing_tables: set[str] = set()
        existing_columns: set[str] = set()
        existing_joins: set[frozenset[str]] = set()
        existing_desc: dict[str, str] = {}
        try:
            dbs = (_payload.current_schema or {}).get("databases") or {}
            for db_name, db_obj in dbs.items():
                tables = (db_obj or {}).get("tables") or {}
                for table_name, table_obj in tables.items():
                    existing_tables.add(f"{db_name}.{table_name}")
                    cols = (table_obj or {}).get("columns") or {}
                    for col_name, col_spec in cols.items():
                        key = f"{db_name}.{table_name}.{col_name}"
                        existing_columns.add(key)
                        if isinstance(col_spec, dict):
                            desc = str(col_spec.get("description") or "").strip()
                            if desc:
                                existing_desc[key] = desc
            for jp in (_payload.current_schema or {}).get("join_paths") or []:
                if not isinstance(jp, dict):
                    continue
                left = str(jp.get("left") or "").strip()
                right = str(jp.get("right") or "").strip()
                if left and right:
                    existing_joins.add(frozenset([left, right]))
        except Exception:
            # Never fail post_validate due to diff parsing.
            existing_tables = set()
            existing_columns = set()
            existing_joins = set()
            existing_desc = {}

        confirm = output.confirm
        confirm.tables = [t for t in confirm.tables if str(t).strip() and str(t).strip() not in existing_tables]
        confirm.columns = [c for c in confirm.columns if str(c).strip() and str(c).strip() not in existing_columns]
        filtered_more = []
        for item in confirm.more_info:
            col = str(item.column or "").strip()
            desc = str(item.description or "").strip()
            if not col or not desc:
                continue
            # Only keep if column is new (to be created) or already exists but description adds new text.
            if col not in confirm.columns and col not in existing_columns:
                continue
            cur = existing_desc.get(col, "")
            if cur and desc in cur:
                continue
            filtered_more.append(item)
        confirm.more_info = filtered_more
        filtered_joins = []
        for j in confirm.join_paths:
            left = str(j.left or "").strip()
            right = str(j.right or "").strip()
            if not left or not right:
                continue
            if left == right:
                continue
            if frozenset([left, right]) in existing_joins:
                continue
            filtered_joins.append(j)
        confirm.join_paths = filtered_joins

        confirm = output.confirm

        for t in confirm.tables:
            if not self._re_table.match(t):
                raise ValueError(f"invalid table ref: {t!r} (expected db.table)")

        for c in confirm.columns:
            if not self._re_column.match(c):
                raise ValueError(f"invalid column ref: {c!r} (expected db.table.column)")

        for item in confirm.more_info:
            if not self._re_column.match(item.column):
                raise ValueError(f"invalid more_info.column: {item.column!r} (expected db.table.column)")
            if not str(item.description or "").strip():
                raise ValueError(f"empty more_info.description for {item.column!r}")

        for j in confirm.join_paths:
            if not self._re_column.match(j.left):
                raise ValueError(f"invalid join_paths.left: {j.left!r} (expected db.table.column)")
            if not self._re_column.match(j.right):
                raise ValueError(f"invalid join_paths.right: {j.right!r} (expected db.table.column)")
