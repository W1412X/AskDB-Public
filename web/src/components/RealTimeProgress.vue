<template>
  <div class="rtp">
    <div class="rtp-head">
      <el-tag v-if="workflowStatus" size="small" type="info">工作流 {{ workflowStatus }}</el-tag>
      <el-tag v-if="streamConnected" size="small" type="success" style="margin-left: 8px">SSE</el-tag>
      <el-tag v-else-if="workflowId" size="small" type="warning" style="margin-left: 8px">SSE 未连接</el-tag>
      <span v-if="stepStats" class="rtp-meta">{{ stepStats }}</span>
      <span v-if="updatedAt" class="rtp-meta">快照 {{ formatHms(updatedAt) }}</span>
    </div>

    <el-row :gutter="14" class="rtp-row">
      <el-col :xs="24" :lg="11">
        <div class="rtp-card-title">意图依赖 DAG</div>
        <div class="flow-wrap">
          <VueFlow
            v-if="flowNodes.length"
            v-model:nodes="flowNodes"
            v-model:edges="flowEdges"
            :default-viewport="{ zoom: 0.85 }"
            fit-view-on-init
            class="vue-flow-wrap"
            @node-click="(e) => $emit('select-intent', e.node?.id)"
          >
            <Background />
            <Controls />
          </VueFlow>
          <el-empty v-else description="分解完成后显示" :image-size="56" />
        </div>
      </el-col>
      <el-col :xs="24" :lg="13">
        <div class="rtp-card-title">执行时间线（步骤 + 检查点）</div>
        <div class="tl-toolbar">
          <el-select v-model="scopeFilter" placeholder="范围" size="small" style="width: 118px" clearable>
            <el-option label="全部" value="" />
            <el-option label="workflow" value="workflow" />
            <el-option label="intent" value="intent" />
            <el-option label="schemalink" value="schemalink" />
          </el-select>
          <el-input
            v-model="searchQ"
            size="small"
            clearable
            placeholder="筛选摘要 / agent / phase / owner…"
            class="tl-search"
          />
          <el-switch v-model="autoFollow" size="small" active-text="跟随最新" />
          <el-button size="small" text type="primary" @click="scrollToLatest">到底部</el-button>
        </div>
        <div ref="scrollRef" class="timeline-scroll" @scroll="onScroll">
          <div class="tl-table">
            <div class="tl-head">
              <span class="c-idx">#</span>
              <span class="c-time">时间</span>
              <span class="c-delta">+Δ</span>
              <span class="c-kind">类型</span>
              <span class="c-scope">scope</span>
              <span class="c-owner">owner</span>
              <span class="c-agent">agent</span>
              <span class="c-phase">phase / 标签</span>
              <span class="c-sum">摘要</span>
            </div>
            <div
              v-for="row in displayRows"
              :key="row.key"
              class="tl-row"
              :class="{ 'tl-cp': row.kind === 'checkpoint', 'tl-open': expanded[row.key] }"
              @click="toggleExpand(row.key)"
            >
              <span class="c-idx mono">{{ row.seq }}</span>
              <span class="c-time mono">{{ row.timeStr }}</span>
              <span class="c-delta mono">{{ row.deltaLabel }}</span>
              <span class="c-kind">
                <el-tag :type="row.kind === 'checkpoint' ? 'warning' : 'primary'" size="small" effect="plain">
                  {{ row.kind === 'checkpoint' ? 'ckpt' : 'step' }}
                </el-tag>
              </span>
              <span class="c-scope">{{ row.scope }}</span>
              <span class="c-owner mono" :title="row.owner_id">{{ shortId(row.owner_id) }}</span>
              <span class="c-agent mono" :title="row.agent">{{ shortText(row.agent, 14) }}</span>
              <span class="c-phase">{{ row.phase || '—' }}{{ row.round_index != null ? ` · r${row.round_index}` : '' }}</span>
              <span class="c-sum">
                <span class="sum-preview">{{ expanded[row.key] ? row.summary : preview(row.summary) }}</span>
              </span>
            </div>
            <el-empty v-if="!displayRows.length" description="尚无步骤或检查点" :image-size="48" />
          </div>
        </div>
        <p v-if="truncatedHint" class="tl-trunc">{{ truncatedHint }}</p>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, watch, computed, nextTick } from 'vue'
import { VueFlow } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import '@vue-flow/core/dist/style.css'
import '@vue-flow/core/dist/theme-default.css'
import '@vue-flow/controls/dist/style.css'

const props = defineProps({
  view: { type: Object, default: null },
  workflowStatus: { type: String, default: '' },
  workflowId: { type: String, default: '' },
  streamConnected: { type: Boolean, default: false },
  updatedAt: { type: Number, default: null },
})

defineEmits(['select-intent'])

const flowNodes = ref([])
const flowEdges = ref([])
const scrollRef = ref(null)
const scopeFilter = ref('')
const searchQ = ref('')
const autoFollow = ref(true)
const expanded = ref({})
const userScrolledUp = ref(false)

const stepStats = computed(() => {
  const v = props.view
  if (!v) return ''
  const total = v.steps_total
  const shown = v.steps_shown
  const cp = (v.checkpoints || []).length
  if (total == null && !shown) return ''
  const parts = []
  if (total != null) parts.push(`步骤 ${total} 条`)
  if (shown != null && total != null && shown < total) parts.push(`本页末 ${shown} 条`)
  else if (shown != null) parts.push(`步骤 ${shown} 条`)
  if (cp) parts.push(`检查点 ${cp}`)
  return parts.join(' · ')
})

const truncatedHint = computed(() => {
  const v = props.view
  if (!v?.steps_truncated) return ''
  return '仅展示最近 500 条步骤；更早记录已在服务端截断。'
})

const mergedChronological = computed(() => {
  const v = props.view
  if (!v) return []
  const steps = v.steps_recent || []
  const cps = v.checkpoints || []
  const rows = []
  let n = 0
  steps.forEach((s) => {
    rows.push({
      key: `s-${s.step_id}-${n++}`,
      kind: 'step',
      created_at: Number(s.created_at) || 0,
      scope: s.scope || '',
      owner_id: s.owner_id || '',
      agent: s.agent || '',
      phase: s.phase || '',
      round_index: s.round_index,
      summary: String(s.summary || ''),
      step_id: s.step_id || '',
    })
  })
  cps.forEach((c) => {
    rows.push({
      key: `c-${c.checkpoint_id}-${n++}`,
      kind: 'checkpoint',
      created_at: Number(c.created_at) || 0,
      scope: c.scope || '',
      owner_id: c.owner_id || '',
      agent: 'checkpoint',
      phase: c.label || '',
      round_index: null,
      summary: [c.label, c.checkpoint_id].filter(Boolean).join(' · '),
      step_id: c.checkpoint_id || '',
    })
  })
  rows.sort((a, b) => a.created_at - b.created_at)
  return rows
})

const displayRows = computed(() => {
  let rows = mergedChronological.value
  if (scopeFilter.value) {
    rows = rows.filter((r) => r.scope === scopeFilter.value)
  }
  const q = (searchQ.value || '').trim().toLowerCase()
  if (q) {
    rows = rows.filter((r) => {
      const hay = [r.summary, r.agent, r.phase, r.owner_id, r.step_id, r.scope].join(' ').toLowerCase()
      return hay.includes(q)
    })
  }
  let prevT = 0
  return rows.map((r, i) => {
    const deltaMs =
      prevT > 0 && r.created_at > 0 ? Math.round((r.created_at - prevT) * 1000) : null
    prevT = r.created_at || prevT
    return {
      ...r,
      seq: i + 1,
      timeStr: formatHmsMs(r.created_at),
      deltaLabel: deltaMs == null ? '—' : `${deltaMs}ms`,
    }
  })
})

function preview(s) {
  if (!s) return '—'
  const max = 160
  return s.length <= max ? s : `${s.slice(0, max)}…`
}

function shortId(s) {
  if (!s) return '—'
  return s.length > 18 ? `${s.slice(0, 16)}…` : s
}

function shortText(s, max) {
  if (!s || s === '—') return '—'
  return s.length > max ? `${s.slice(0, max - 1)}…` : s
}

function toggleExpand(key) {
  expanded.value = { ...expanded.value, [key]: !expanded.value[key] }
}

function formatHms(ts) {
  if (ts == null || !Number(ts)) return ''
  try {
    return new Date(ts * 1000).toLocaleTimeString()
  } catch {
    return ''
  }
}

function formatHmsMs(ts) {
  if (ts == null || !Number(ts)) return '—'
  try {
    const d = new Date(ts * 1000)
    const p = (n) => String(n).padStart(2, '0')
    const ms = String(d.getMilliseconds()).padStart(3, '0')
    return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${ms}`
  } catch {
    return '—'
  }
}

function scrollToLatest() {
  const el = scrollRef.value
  if (!el) return
  el.scrollTop = el.scrollHeight
  userScrolledUp.value = false
}

function onScroll() {
  const el = scrollRef.value
  if (!el) return
  const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80
  userScrolledUp.value = !nearBottom
}

watch(
  () => displayRows.value.length,
  async () => {
    if (!autoFollow.value || userScrolledUp.value) return
    await nextTick()
    scrollToLatest()
  }
)

watch(
  () => props.view?.updated_at,
  async () => {
    if (!autoFollow.value || userScrolledUp.value) return
    await nextTick()
    scrollToLatest()
  }
)

function layoutFlow() {
  const v = props.view
  const nodes = v?.topology?.nodes || []
  const edges = v?.topology?.edges || []
  if (!nodes.length) {
    flowNodes.value = []
    flowEdges.value = []
    return
  }
  const incoming = {}
  nodes.forEach((n) => {
    incoming[n.id] = []
  })
  edges.forEach((e) => {
    if (incoming[e.target] !== undefined) incoming[e.target].push(e.source)
  })
  const levels = {}
  nodes.forEach((n) => {
    levels[n.id] = 0
  })
  for (let round = 0; round < 25; round++) {
    let changed = false
    for (const n of nodes) {
      const id = n.id
      const preds = incoming[id] || []
      const predLevels = preds.map((p) => levels[p]).filter((x) => x !== undefined)
      const nextLevel = predLevels.length ? Math.max(...predLevels) + 1 : 0
      if (levels[id] !== nextLevel) {
        levels[id] = nextLevel
        changed = true
      }
    }
    if (!changed) break
  }
  const byLevel = {}
  nodes.forEach((n) => {
    const l = levels[n.id] ?? 0
    if (!byLevel[l]) byLevel[l] = []
    byLevel[l].push(n)
  })
  const nodeWidth = 200
  const nodeHeight = 56
  const gapX = 72
  const gapY = 64
  flowNodes.value = nodes.map((n) => {
    const l = levels[n.id] ?? 0
    const row = byLevel[l].indexOf(n)
    return {
      id: n.id,
      type: 'default',
      position: { x: row * (nodeWidth + gapX), y: l * (nodeHeight + gapY) },
      data: { label: n.label || n.id },
      class: `flow-node flow-node-${(n.status || '').toLowerCase()}`,
    }
  })
  flowEdges.value = edges.map((e, i) => ({
    id: `e-${e.source}-${e.target}-${i}`,
    source: e.source,
    target: e.target,
  }))
}

watch(
  () => props.view,
  () => layoutFlow(),
  { immediate: true, deep: true }
)
</script>

<style scoped>
.rtp {
  margin-bottom: 16px;
}
.rtp-head {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 10px;
}
.rtp-meta {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.rtp-meta + .rtp-meta {
  margin-left: 8px;
}
.rtp-row {
  width: 100%;
}
.rtp-card-title {
  font-size: 13px;
  font-weight: 600;
  margin-bottom: 8px;
  color: var(--el-text-color-primary);
}
.flow-wrap {
  width: 100%;
  height: 300px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  overflow: hidden;
}
.vue-flow-wrap {
  width: 100%;
  height: 100%;
}
:deep(.flow-node) {
  font-size: 11px;
}
:deep(.flow-node-completed) {
  --vf-node-bg: var(--el-color-success-light-9);
}
:deep(.flow-node-failed),
:deep(.flow-node-blocked_by_upstream) {
  --vf-node-bg: var(--el-color-danger-light-9);
}
:deep(.flow-node-wait_user) {
  --vf-node-bg: var(--el-color-warning-light-9);
}
:deep(.flow-node-running),
:deep(.flow-node-ready) {
  --vf-node-bg: var(--el-color-primary-light-9);
}

.tl-toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.tl-search {
  flex: 1;
  min-width: 160px;
  max-width: 360px;
}

.timeline-scroll {
  max-height: min(52vh, 560px);
  overflow: auto;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  background: var(--el-fill-color-blank);
}

.tl-table {
  font-size: 12px;
  min-width: 720px;
}

.tl-head {
  display: grid;
  grid-template-columns: 36px 92px 52px 56px 88px 100px 100px 120px 1fr;
  gap: 6px;
  padding: 8px 10px;
  position: sticky;
  top: 0;
  z-index: 2;
  background: var(--el-fill-color-light);
  border-bottom: 1px solid var(--el-border-color-lighter);
  font-weight: 600;
  color: var(--el-text-color-secondary);
}

.tl-row {
  display: grid;
  grid-template-columns: 36px 92px 52px 56px 88px 100px 100px 120px 1fr;
  gap: 6px;
  padding: 6px 10px;
  border-bottom: 1px solid var(--el-border-color-extra-light);
  align-items: start;
  cursor: pointer;
  transition: background 0.12s ease;
}
.tl-row:hover {
  background: var(--el-fill-color-lighter);
}
.tl-row:nth-child(even):not(:hover) {
  background: var(--el-fill-color-extra-light);
}
.tl-cp {
  border-left: 3px solid var(--el-color-warning);
}
.tl-row:not(.tl-cp) {
  border-left: 3px solid transparent;
}
.tl-open .sum-preview {
  white-space: pre-wrap;
  word-break: break-word;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 11px;
}

.c-sum {
  color: var(--el-text-color-regular);
  line-height: 1.45;
}
.sum-preview {
  display: block;
  white-space: pre-wrap;
  word-break: break-word;
}

.tl-trunc {
  margin: 8px 0 0;
  font-size: 11px;
  color: var(--el-text-color-secondary);
}
.c-phase {
  color: var(--el-text-color-regular);
  line-height: 1.45;
  min-width: 0;
  white-space: normal;
  overflow-wrap: break-word;
  word-break: break-word;
}
</style>
