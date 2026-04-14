<template>
  <div class="idetail">
    <p class="intent-desc">{{ intent.description }}</p>
    <p v-if="intent.schema_intent" class="schema-intent">
      <strong>Schema 意图</strong>：{{ intent.schema_intent }}
    </p>

    <el-divider content-position="left">七阶段进度</el-divider>
    <el-steps direction="vertical" :space="56">
      <el-step
        v-for="t in intent.task_flow"
        :key="t.task_id"
        :title="phaseTitle(t)"
        :description="t.status"
        :status="taskStepStatus(t.status)"
      />
    </el-steps>

    <template v-if="intent.schemalink && (intent.schemalink.round_index != null || intent.schemalink.mode)">
      <el-divider content-position="left">SchemaLink 当前轮次</el-divider>
      <el-descriptions :column="2" size="small" border>
        <el-descriptions-item label="模式">{{ intent.schemalink.mode }}</el-descriptions-item>
        <el-descriptions-item label="轮次索引">{{ intent.schemalink.round_index }}</el-descriptions-item>
        <el-descriptions-item label="子意图">{{ intent.schemalink.intent_id }}</el-descriptions-item>
        <el-descriptions-item label="工具输出键">
          {{ (intent.schemalink.last_tool_output_keys || []).join(', ') || '-' }}
        </el-descriptions-item>
      </el-descriptions>
      <pre v-if="intent.schemalink.last_write_result && Object.keys(intent.schemalink.last_write_result).length" class="json-pre">{{
        JSON.stringify(intent.schemalink.last_write_result, null, 2)
      }}</pre>
    </template>

    <template v-if="intent.schema_sub_dag && intent.schema_sub_dag.nodes?.length">
      <el-divider content-position="left">Schema 子意图 DAG</el-divider>
      <el-table :data="intent.schema_sub_dag.nodes" border size="small" max-height="200">
        <el-table-column prop="id" label="节点" width="120" />
        <el-table-column prop="intent" label="子意图" show-overflow-tooltip />
        <el-table-column label="依赖" show-overflow-tooltip>
          <template #default="{ row }">{{ (row.dependent_intent_ids || []).join(', ') }}</template>
        </el-table-column>
      </el-table>
    </template>

    <template v-if="intent.ra_plan">
      <el-divider content-position="left">RA 规划</el-divider>
      <p v-if="intent.ra_plan.summary" class="ra-summary">{{ intent.ra_plan.summary }}</p>
      <el-collapse>
        <el-collapse-item title="实体 (Entities)" name="ent">
          <el-table :data="intent.ra_plan.entities || []" border size="small">
            <el-table-column prop="database" label="库" width="100" />
            <el-table-column prop="table" label="表" width="120" />
            <el-table-column prop="alias" label="别名" width="80" />
            <el-table-column label="列">
              <template #default="{ row }">{{ (row.columns || []).join(', ') }}</template>
            </el-table-column>
          </el-table>
        </el-collapse-item>
        <el-collapse-item title="连接 (Joins)" name="jn">
          <el-table :data="intent.ra_plan.joins || []" border size="small">
            <el-table-column prop="left_alias" label="左" width="70" />
            <el-table-column prop="right_alias" label="右" width="70" />
            <el-table-column prop="left_column" label="左列" />
            <el-table-column prop="right_column" label="右列" />
            <el-table-column prop="type" label="类型" width="70" />
          </el-table>
        </el-collapse-item>
        <el-collapse-item title="过滤 / 聚合" name="fa">
          <div class="subhdr">Filters</div>
          <el-table :data="intent.ra_plan.filters || []" border size="small" class="mb8">
            <el-table-column prop="expr" label="表达式" />
            <el-table-column prop="reason" label="原因" />
          </el-table>
          <div class="subhdr">Aggregations</div>
          <el-table :data="intent.ra_plan.aggregations || []" border size="small">
            <el-table-column prop="expr" label="表达式" />
            <el-table-column prop="alias" label="别名" width="100" />
          </el-table>
        </el-collapse-item>
      </el-collapse>
    </template>

    <template v-if="intent.sql_render && (intent.sql_render.candidates || []).length">
      <el-divider content-position="left">SQL 渲染候选</el-divider>
      <el-collapse>
        <el-collapse-item v-for="(c, i) in intent.sql_render.candidates" :key="i" :title="`候选 ${i + 1}`" :name="`c${i}`">
          <pre class="sql-block">{{ c.sql }}</pre>
          <p v-if="c.rationale" class="small-muted">{{ c.rationale }}</p>
        </el-collapse-item>
      </el-collapse>
    </template>

    <template v-if="intent.sql_validation && (intent.sql_validation.reports || []).length">
      <el-divider content-position="left">SQL 校验报告</el-divider>
      <el-alert
        :type="intent.sql_validation.status === 'SUCCESS' ? 'success' : 'warning'"
        :title="`状态: ${intent.sql_validation.status}，最佳候选索引: ${intent.sql_validation.best_candidate_index}`"
        :closable="false"
        show-icon
        class="mb8"
      />
      <el-table :data="intent.sql_validation.reports" border size="small">
        <el-table-column prop="candidate_index" label="#" width="60" />
        <el-table-column prop="passed" label="通过" width="70">
          <template #default="{ row }">
            <el-tag :type="row.passed ? 'success' : 'danger'" size="small">{{ row.passed ? 'Y' : 'N' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="错误">
          <template #default="{ row }">{{ (row.errors || []).join('; ') }}</template>
        </el-table-column>
      </el-table>
    </template>

    <template v-if="intent.dependencies && intent.dependencies.length">
      <el-divider content-position="left">上游依赖</el-divider>
      <div class="dependency-list">
        <el-card v-for="dep in intent.dependencies" :key="`${intent.intent_id}-${dep.intent_id}`" class="dependency-card" shadow="never">
          <div class="dependency-header">
            <el-tag size="small" type="info">{{ dep.intent_id }}</el-tag>
            <span class="dependency-title">{{ dep.intent_request || dep.intent_id }}</span>
          </div>
          <div v-if="dep.result_summary" class="dependency-summary">{{ dep.result_summary }}</div>
          <div class="dependency-meta">
            <span>Schema：{{ dep.resolved_schema_summary?.table_count }} 表 / {{ dep.resolved_schema_summary?.column_count }} 列</span>
          </div>
          <pre v-if="dep.sql_preview" class="sql-block dependency-sql">{{ dep.sql_preview }}</pre>
        </el-card>
      </div>
    </template>

    <template v-if="intent.schema && Object.keys(intent.schema.databases || {}).length">
      <el-divider content-position="left">使用的 Schema</el-divider>
      <el-table :data="schemaTableRows(intent.schema)" border size="small" max-height="200">
        <el-table-column prop="db" label="库" width="120" />
        <el-table-column prop="table" label="表" width="120" />
        <el-table-column prop="column" label="列" />
      </el-table>
    </template>

    <template v-if="intent.sql">
      <el-divider content-position="left">执行的 SQL</el-divider>
      <pre class="sql-block">{{ intent.sql }}</pre>
    </template>

    <template v-if="intent.exec_result && intent.exec_result.rows && intent.exec_result.rows.length">
      <el-divider content-position="left">执行结果 (共 {{ intent.exec_result.row_count }} 行)</el-divider>
      <el-alert
        v-if="intent.exec_result.truncated"
        type="info"
        :closable="false"
        show-icon
        class="mb8"
        :title="`当前仅展示前 ${intent.exec_result.displayed_row_count} 行`"
      />
      <el-table :data="intent.exec_result.rows" border size="small" max-height="280">
        <el-table-column v-for="col in execColumns(intent.exec_result.rows)" :key="col" :prop="col" :label="col" show-overflow-tooltip />
      </el-table>
    </template>

    <template v-if="intent.interpretation && intent.interpretation.answer">
      <el-divider content-position="left">结果解释</el-divider>
      <div class="intent-answer">{{ intent.interpretation.answer }}</div>
    </template>

    <template v-if="intent.error">
      <el-divider content-position="left">错误</el-divider>
      <el-alert type="error" :title="intent.error.message || 'error'" show-icon :closable="false" />
    </template>
  </div>
</template>

<script setup>
defineProps({
  intent: { type: Object, required: true },
})

function phaseTitle(t) {
  return `${t.task_id} (${t.phase})`
}

function taskStepStatus(s) {
  const v = String(s || '').toLowerCase()
  if (v === 'completed') return 'success'
  if (v === 'failed' || v === 'blocked') return 'error'
  if (v === 'running') return 'process'
  return 'wait'
}

function schemaTableRows(schema) {
  const dbs = schema.databases || {}
  const rows = []
  for (const [db, dbObj] of Object.entries(dbs)) {
    if (!dbObj || typeof dbObj !== 'object') continue
    const tables = dbObj.tables || dbObj
    for (const [table, meta] of Object.entries(tables)) {
      if (table === 'tables' && typeof meta === 'object') continue
      const cols = meta?.columns ? Object.keys(meta.columns) : []
      if (cols.length) cols.forEach((c) => rows.push({ db, table, column: c }))
      else rows.push({ db, table, column: '-' })
    }
  }
  return rows
}

function execColumns(rows) {
  if (!rows || !rows.length) return []
  const set = new Set()
  rows.forEach((r) => Object.keys(r || {}).forEach((k) => set.add(k)))
  return Array.from(set)
}
</script>

<style scoped>
.idetail {
  padding: 4px 0;
}
.intent-desc {
  color: var(--el-text-color-secondary);
  font-size: 13px;
  margin-bottom: 8px;
}
.schema-intent {
  font-size: 13px;
  margin-bottom: 12px;
}
.ra-summary {
  margin-bottom: 12px;
  line-height: 1.5;
}
.subhdr {
  font-size: 12px;
  font-weight: 600;
  margin: 8px 0 4px;
}
.mb8 {
  margin-bottom: 8px;
}
.json-pre {
  font-size: 11px;
  background: var(--el-fill-color-light);
  padding: 8px;
  border-radius: 4px;
  overflow: auto;
  max-height: 160px;
  margin-top: 8px;
}
.small-muted {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.dependency-list {
  display: grid;
  gap: 12px;
  margin-bottom: 12px;
}
.dependency-card {
  background: var(--el-fill-color-lighter);
}
.dependency-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.dependency-title {
  font-weight: 600;
}
.dependency-summary {
  white-space: pre-wrap;
  margin-bottom: 8px;
  font-size: 13px;
}
.dependency-meta {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-bottom: 8px;
}
.dependency-sql {
  max-height: 200px;
}
.sql-block {
  background: var(--el-fill-color-light);
  padding: 12px;
  border-radius: 4px;
  overflow-x: auto;
  font-size: 12px;
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
}
.intent-answer {
  white-space: pre-wrap;
  line-height: 1.6;
}
</style>
