from __future__ import annotations

from typing import Any

from .ask_user import AskUserTool
from .list_table_columns import ListTableColumnsTool
from .list_tables import ListTablesTool
from .relation_validator import RelationValidationTool
from .schema_catalog import SchemaCatalogTool
from .semantic_join_path_search import SemanticJoinPathSearchTool, SemanticJoinSearchTool
from .semantic_column_search import SemanticColumnSearchTool
from .semantic_table_search import SemanticTableSearchTool
from .sql_explorer import SqlExploreTool


class ToolRegistry:
    def __init__(self) -> None:
        self._tools = {
            "semantic_table_search": SemanticTableSearchTool(),
            "list_tables": ListTablesTool(),
            "list_table_columns": ListTableColumnsTool(),
            "semantic_column_search": SemanticColumnSearchTool(),
            "semantic_join_path_search": SemanticJoinPathSearchTool(),
            "semantic_join_search": SemanticJoinSearchTool(),
            "schema_catalog": SchemaCatalogTool(),
            "sql_explorer": SqlExploreTool(),
            "relation_validator": RelationValidationTool(),
            "ask_user": AskUserTool(),
        }

    def get_tool(self, name: str):
        return self._tools[name]

    def invoke(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.get_tool(name).invoke(arguments or {})

    def tool_spec(self, name: str) -> dict[str, Any]:
        if name == "semantic_column_search":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "通过列名、注释、语义摘要检索与当前业务问题相关的候选列。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "text": {"type": "string", "description": "业务问题、目标字段、口径、维度等检索文本。"},
                            "database_scope": {"type": "array", "items": {"type": "string"}, "description": "允许检索的数据库范围。"},
                            "top_k": {"type": "integer", "description": "返回候选列数量。"},
                        },
                        "required": ["text", "database_scope"],
                    },
                },
            }
        if name in {"semantic_join_path_search", "semantic_join_search"}:
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "从种子列或种子表出发，按语义、结构与类型启发式搜索可连接的表路径，可跨库并支持多跳。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "text": {"type": "string", "description": "用于描述要发现的连接目标或业务意图的文本。"},
                            "seed_columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "已知的种子列，格式 db.table.column。",
                            },
                            "seed_tables": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "已知的种子表，格式 db.table。",
                            },
                            "target_tables": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "希望优先到达的目标表，格式 db.table。",
                            },
                            "database_scope": {"type": "array", "items": {"type": "string"}, "description": "允许搜索的数据库范围。"},
                            "top_k": {"type": "integer", "description": "返回候选 join 对数量。"},
                            "max_columns_per_database": {"type": "integer", "description": "每个数据库参与搜索的最大候选列数。"},
                            "min_score": {"type": "number", "description": "候选最小分数阈值。"},
                            "allow_cross_database": {"type": "boolean", "description": "是否允许跨库搜索。"},
                        },
                        "required": ["text", "database_scope"],
                    },
                },
            }
        if name == "semantic_table_search":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "通过表名与表描述检索与当前业务问题相关的候选表。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "text": {"type": "string", "description": "业务问题、目标实体或口径等检索文本。"},
                            "database_scope": {"type": "array", "items": {"type": "string"}, "description": "允许检索的数据库范围。"},
                            "top_k": {"type": "integer", "description": "返回候选表数量。"},
                        },
                        "required": ["text", "database_scope"],
                    },
                },
            }
        if name == "list_tables":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "列出指定数据库下的表及其描述。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "database": {"type": "string", "description": "数据库名称。"},
                        },
                        "required": ["database"],
                    },
                },
            }
        if name == "list_table_columns":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "列出指定表下的列及其 description。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "database": {"type": "string", "description": "数据库名称。"},
                            "table": {"type": "string", "description": "表名称。"},
                        },
                        "required": ["database", "table"],
                    },
                },
            }
        if name == "schema_catalog":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "读取数据库、表或列的元数据描述。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "database": {"type": "string"},
                            "table": {"type": "string"},
                            "column": {"type": "string"},
                            "fields": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["database"],
                    },
                },
            }
        if name == "sql_explorer":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "执行只读 SELECT/WITH 探索 SQL，用于验证样本或数据分布。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "database": {"type": "string"},
                            "sql": {"type": "string"},
                            "limit": {"type": "integer"},
                            "timeout_ms": {"type": "integer"},
                        },
                        "required": ["sql"],
                    },
                },
            }
        if name == "relation_validator":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "验证两个列之间是否适合作为 join 键，并返回 join 质量指标。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "left_column": {"type": "string", "description": "左侧列，格式 db.table.column"},
                            "right_column": {"type": "string", "description": "右侧列，格式 db.table.column"},
                        },
                        "required": ["left_column", "right_column"],
                    },
                },
            }
        if name == "ask_user":
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": "构造一个 ask-user 请求载荷，由工作流统一转为 ask ticket。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "scope": {"type": "string"},
                            "owner_id": {"type": "string"},
                            "question_id": {"type": "string"},
                            "question": {"type": "string"},
                            "why_needed": {"type": "string"},
                            "acceptance_criteria": {"type": "array", "items": {"type": "string"}},
                            "resume_point": {"type": "object"},
                        },
                        "required": ["scope", "owner_id", "question_id", "question"],
                    },
                },
            }
        raise KeyError(f"unknown tool spec: {name}")

    def tool_specs(self, names: list[str]) -> list[dict[str, Any]]:
        return [self.tool_spec(name) for name in names]
