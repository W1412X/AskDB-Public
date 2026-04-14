from __future__ import annotations

from ..contracts import DependencyContext, DependencyItem, Schema
from ..execution.schema_merge import merge_schema
from ..state import IntentState, WorkflowState


class IntentContextBuilder:
    def build(self, intent_state: IntentState, workflow_state: WorkflowState) -> tuple[DependencyContext, str, Schema]:
        items: list[DependencyItem] = []
        initial_schema = Schema()
        for dep_id in intent_state.dependent_intent_ids:
            dep_state = workflow_state.intents.get(dep_id)
            if dep_state is None or dep_state.status.name != "COMPLETED":
                continue
            dep_schema = dep_state.resolved_schema or Schema()
            initial_schema = merge_schema(initial_schema, dep_schema)
            items.append(
                DependencyItem(
                    intent_id=dep_state.intent_id,
                    intent=dep_state.intent_text,
                    resolved_schema=dep_schema,
                    sql=dep_state.selected_sql or "",
                    result_summary=(dep_state.interpretation_result.answer if dep_state.interpretation_result else ""),
                )
            )
        current_intent = intent_state.query_intent_text or intent_state.intent_text
        context = DependencyContext(known_information=items, current_intent=current_intent, initial_schema=initial_schema)
        schema_intent = intent_state.schema_intent_text or intent_state.intent_text
        if not items:
            known_information_text = f"【已知信息】无\n\n【Schema构建意图】\n{schema_intent}"
        else:
            lines = ["【已知信息】"]
            for item in items:
                lines.append(f"依赖意图 {item.intent_id}：")
                lines.append(f"  可复用 schema：{self._schema_brief(item.resolved_schema)}")
            lines.append("")
            lines.append("【Schema构建意图】")
            lines.append(schema_intent)
            known_information_text = "\n".join(lines)
        return context, known_information_text, initial_schema

    def _schema_brief(self, schema: Schema) -> str:
        table_count = 0
        column_count = 0
        for _db_name, db_obj in (schema.databases or {}).items():
            for _table_name, table_obj in (db_obj.tables or {}).items():
                table_count += 1
                column_count += len(table_obj.columns or {})
        return f"{table_count}表/{column_count}列"
