from __future__ import annotations

from pydantic import BaseModel

from .base_agent import BaseAgent
from .prompt_builder import build_json_system_prompt, build_json_user_prompt, numbered_list
from ..contracts import AgentStep, SchemaOrchestratorOutput


SYSTEM_ACTIONS = [
    "WRITE_SCHEMA",
    "CALL_TOOL",
    "ASK_USER",
    "SUCCESS",
]

SYSTEM_RULES = [
    "你只决定下一步动作，不写 schema、不调用数据库、不输出 SQL。",
    "你只对 schema 构建负责；RA/SQL 的生成与执行不在你职责内。",
    "禁止无证据编造字段/表/join/数据库；物理对象必须用完整路径 db.table.column。",
    "仅基于 current_schema / steps / last_tool_output / last_write_result 做最小必要决策。",
    "你评估的是 schema 结构完备性，不是数据值是否存在。",
    "决策顺序：证据不足->CALL_TOOL；需要写入->WRITE_SCHEMA；schema 足够->SUCCESS；两轮无新增且工具无解->ASK_USER。",
    "初始未知阶段只能做表级发现（列出表或语义表检索）。",
    "你需要关注steps以及last_tool_output/last_write_result，根据它们决定下一步动作。因为可能存在上一轮你给出错误的指令决策，导致执行失败.",
    "一旦获得可写入信息，必须立即 WRITE_SCHEMA，不要继续 CALL_TOOL。",
    "当 last_tool_output 含 toolagent 输出时：只允许依据 last_tool_output.confirm 做写入判断；suggestion 只能用于决定下一轮 CALL_TOOL。",
    "WRITE_SCHEMA 只能在 last_tool_output.confirm 非空时使用。",
    "若 last_write_result.write_no_effect=true，禁止重复 WRITE_SCHEMA；必须改为 CALL_TOOL 获取缺失证据，或在 schema 已足够时 SUCCESS。",
    "如果你需要某个列但列名不确定，禁止输出任何 db.table.column 猜测；必须 CALL_TOOL 并用语义描述要找的列。",
    "不要把“数据里不存在某个取值/样本值未出现”当作 schema 缺口；只要相关列已确认存在且类型合理，schema 应视为足够。",
    "若 last_write_result.sufficiency.reason 提到具体取值不存在，你不能据此继续做值探测；应回到结构目标判断 SUCCESS 或补充结构证据。",
    "只有在用户意图本身不清晰时才允许 ASK_USER；不能因为值缺失而 ASK_USER。",
    "description 只用一句话说明“下一步要做什么”。",
    "只返回严格 JSON。",
]

USER_REQUIREMENT_PREFIX = [
    "输出字段必须包含 action、description，并按 action 补齐对应字段。",
    "规则：",
]

USER_REQUIREMENTS = [
    "CALL_TOOL：tool_task.goal 只写“需要获取的信息”，不得写动机/用途；每轮聚焦一个子问题。",
    "WRITE_SCHEMA：description 只写“需要写入哪些对象”，不输出 plan，不夹带查询条件。",
    "SUCCESS：description 只写“schema 已足够”。",
    "ASK_USER：必须输出具体问题（question/why_needed/acceptance_criteria），description 只概括要补充的信息。",
    "如果 last_tool_output 已包含可写入对象，必须 WRITE_SCHEMA，不要继续 CALL_TOOL。",
    "WRITE_SCHEMA 不得包含任何查询条件/过滤/排序/分组内容（例如最近30天、<=8年等），这些不属于 schema。",
    "初始阶段如果 current_schema 没有任何表，CALL_TOOL 的 goal 只能是“获取数据库表列表”或“语义检索表”，不能同时要求列/关系。",
    "当列名不确定时，CALL_TOOL.goal 必须用语义描述（例如“设备表中表示设备唯一标识的列名”），不得写 db.table.column 猜测。",
    "CALL_TOOL.goal 禁止包含具体值探测（例如“确认是否存在 maintenance/0.88”）；只允许结构性探测目标。",
]


class _Payload(BaseModel):
    intent: str
    known_information_text: str = ""
    current_schema: dict = {}
    database_scope: list[str] = []
    last_tool_output: dict = {}
    last_write_result: dict = {}


class SchemaLinkOrchestratorAgent(BaseAgent[_Payload, SchemaOrchestratorOutput]):
    name = "schemalink_orchestrator"
    description = "schemalink 调度"
    output_model = SchemaOrchestratorOutput

    def build_system_prompt(self) -> str:
        return build_json_system_prompt(
            role_line="你是 AskDB 系统中的 schemalink 调度代理。",
            mission_line="你的唯一职责是：在尽量少的轮次内，为当前 schema intent 决定 schemalink 的下一步动作，直到构建出结构上最小完备的 schema。",
            extra_sections=[
                "你只能做四类动作：",
                numbered_list(SYSTEM_ACTIONS),
                "",
            ],
            rules=SYSTEM_RULES,
        )

    def build_user_prompt(self, payload: _Payload | dict, steps: list[AgentStep] | None) -> str:
        p = _Payload.model_validate(payload)
        return build_json_user_prompt(
            steps_block=self.format_steps_block(steps),
            task_input_json=(
                f'{{"intent": {p.intent!r}, "known_information_text": {p.known_information_text!r}, '
                f'"current_schema": {p.current_schema}, "database_scope": {p.database_scope}, '
                f'"last_tool_output": {p.last_tool_output}, "last_write_result": {p.last_write_result}}}'
            ),
            requirement_prefix=USER_REQUIREMENT_PREFIX,
            requirements=USER_REQUIREMENTS,
            output_schema=self.format_output_schema(),
        )

    def post_validate(self, _payload: _Payload, output: SchemaOrchestratorOutput) -> None:
        # AgentRunner passes the original payload object through; it may be a raw dict.
        p = _Payload.model_validate(_payload)

        if output.action == "CALL_TOOL":
            goal = (output.tool_task.goal or "").strip()
            if not goal:
                raise ValueError("CALL_TOOL requires non-empty tool_task.goal")
            # If toolagent already provided confirmed writable objects, orchestrator must write immediately.
            confirm = (p.last_tool_output or {}).get("confirm") or {}
            if isinstance(confirm, dict):
                for k in ["tables", "columns", "more_info", "join_paths"]:
                    v = confirm.get(k) or []
                    if isinstance(v, list) and len(v) > 0:
                        raise ValueError("last_tool_output.confirm is non-empty; must choose WRITE_SCHEMA (do not CALL_TOOL)")
                        break

        if output.action == "WRITE_SCHEMA":
            confirm = (p.last_tool_output or {}).get("confirm") or {}
            has_confirm = False
            if isinstance(confirm, dict):
                for k in ["tables", "columns", "more_info", "join_paths"]:
                    v = confirm.get(k) or []
                    if isinstance(v, list) and len(v) > 0:
                        has_confirm = True
                        break
            if not has_confirm:
                raise ValueError("WRITE_SCHEMA requires non-empty last_tool_output.confirm; otherwise choose CALL_TOOL")

        if output.action == "ASK_USER":
            q = (output.ask_request.question or "").strip()
            if not q:
                raise ValueError("ASK_USER requires ask_request.question")
