from __future__ import annotations

import re

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt
from ..contracts import AgentStep, IntentPairDecomposeResult


SYSTEM_RULES = [
    "只做语义 intent 分解，不做 schema 构建执行、RA、SQL、执行计划。",
    "输出必须停留在业务语义层，不允许输出数据库名、表名、列名。",
    "不允许输出 SQL、join 关系、物理执行步骤、技术实现说明。",
    "你可以使用语义列检索、语义表检索工具理解领域词与实体落在哪些表，但最终输出必须保持业务语义层。",
    "你必须把每个意图拆成 query 与 schema 两部分，且一一对应。",
    "query 用于后续 RA/SQL，schema 用于后续 schema 构建。",
    "schema 的本质是“为实现查询需要构建哪些能力/覆盖哪些信息”，不是“直接复述查询条件”。",
    "schema 要写成方法导向表述（如：构建/识别/关联/聚合/汇总/统计），不要写成结果导向或取值导向表述。",
    "schema 字段必须是纯语义中文描述，禁止输出任何物理路径、点号路径、表名、列名。",
    "如果工具返回了物理对象信息（例如 production_lines.status），你只能在内部理解，最终输出必须改写为语义表述。",
    "schema 禁止出现具体过滤值、具体枚举值、具体阈值（例如 维护、0.88、近30天、Top10 等）；应改写为“支持按状态过滤/按阈值比较/按时间范围筛选”等能力描述。",
    "如果原问题无法合理拆分，就只输出一个元素，不要为了拆分而拆分。",
    "当用户一个请求里包含多个独立要求时，必须拆成多个元素，且每个元素是完整独立查询。",
    "若问题存在歧义但仍可保守表达，就产出最小可执行语义 intent，而不是编造细节。",
    "只返回严格 JSON，不要输出 markdown，不要输出代码块，不要输出解释性自然语言。",
]

USER_REQUIREMENTS = [
    "如果需要理解领域词含义或粗粒度表候选，可调用 semantic_column_search 或 semantic_table_search，但不要把物理表名、列名写进最终输出。",
    "输出必须是 {\"intents\":[{\"query\":\"...\",\"schema\":\"...\"}]}。",
    "query 必须是业务查询意图，不得出现 db.table.column。",
    "schema 必须是 schema 构建意图，不得出现 SQL 关键词。",
    "schema 必须是中文自然语言短句，不得包含 '.'、'_'、反引号、数据库/表/列字面量。",
    "schema 必须是“需要做什么来支持查询”的描述，不得包含具体值或阈值本身。",
    "错误示例：获取生产线的当前运行状态为维护的记录；正确示例：构建可查询生产线运行状态并支持按状态过滤的 schema。",
    "错误示例：筛选抽检合格率低于0.88的产线并统计维护次数；正确示例：构建可计算产线抽检合格率、关联设备维护记录并按产线汇总统计的 schema。",
    "输出前自检：若 schema 中包含类似 x.y 或 x.y.z 的片段，必须重写为语义描述后再输出。",
    "输出前自检：若 schema 中出现具体数值、比较符号或具体枚举词，必须改写成能力描述后再输出。",
    "query 与 schema 必须语义一致、一一对应。",
    "若问题属于同一查询链，宁可合并为单元素，也不要拆分成伪多意图。",
]


class _Payload(BaseModel):
    query: str
    database_scope: list[str]


class IntentDecomposerAgent(BaseAgent[_Payload, IntentPairDecomposeResult]):
    name = "intent_decomposer"
    description = "业务语义意图分解"
    output_model = IntentPairDecomposeResult
    available_tools = ["semantic_column_search", "semantic_table_search"]
    tool_choice_mode = "mixed"

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的语义意图分解代理。",
            mission_line="你的唯一职责是：把用户问题拆分为一个或多个 query/schema 一一对应的业务语义意图。",
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=f'{{"query": {p.query!r}, "database_scope": {p.database_scope}}}',
            requirement_prefix=[
                '输出格式：{"intents":[{"query":"...","schema":"..."}]}',
                "约束：",
            ],
            requirements=USER_REQUIREMENTS,
            requirement_start=0,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, _payload: _Payload | dict, output: IntentPairDecomposeResult) -> None:
        intents = output.intents or []
        for item in output.intents:
            query = str(item.query or "").strip()
            schema = str(item.schema_intent or "").strip()
            if not query or not schema:
                raise ValueError("query/schema must be non-empty")
            if re.search(r"[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*", query):
                raise ValueError("query contains physical refs")
            if re.search(r"[A-Za-z_][\w$]*\.[A-Za-z_][\w$]*", schema):
                raise ValueError("schema contains physical refs")
        if len(intents) == 0:
            raise ValueError("no intents produced")
