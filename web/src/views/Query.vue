<template>
  <div class="query-page">
    <el-card class="input-card">
      <template #header><span>自然语言查询（实时进度 · SSE）</span></template>
      <el-alert v-if="!initOk" type="info" :closable="false" style="margin-bottom: 16px" show-icon>
        未执行初始化也可直接提问；完成「初始化」后可获得更准确的列语义与检索效果。
      </el-alert>
      <el-alert v-if="queryStore.streamError" type="error" :title="queryStore.streamError" :closable="false" show-icon class="mb12" />
      <el-form :disabled="queryStore.loading" label-width="80px" class="query-form">
        <el-form-item label="问题">
          <el-input v-model="query" type="textarea" :rows="3" placeholder="输入自然语言问题" class="query-textarea" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="queryStore.loading" @click="run">执行（异步 + 实时推送）</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <RealTimeProgress
      v-if="queryStore.workflowId && queryStore.view"
      :view="queryStore.view"
      :workflow-status="queryStore.snapshot?.status || ''"
      :workflow-id="queryStore.workflowId"
      :stream-connected="queryStore.streamConnected"
      :updated-at="queryStore.view?.updated_at"
      @select-intent="onSelectIntent"
    />

    <el-card v-if="queryStore.isWaitUser" class="section-card blocker-card">
      <template #header>
        <span>需要您补充信息</span>
        <el-tag v-if="blockerPhase" size="small" type="info" style="margin-left: 8px">{{ blockerPhase }}</el-tag>
      </template>
      <div v-if="!result?.ask_ticket" class="blocker-message">
        <el-alert type="warning" :closable="false" show-icon>
          当前处于等待用户补充状态，但未获取到澄清内容。请刷新后重试或重新发起查询。
        </el-alert>
      </div>
      <template v-else>
        <div class="blocker-message">{{ blockerMessage }}</div>
        <el-input v-model="userReply" type="textarea" :rows="3" placeholder="请输入您的补充信息后点击「提交并继续」" />
        <el-button type="primary" :loading="queryStore.resuming" :disabled="!canResume" style="margin-top: 8px" @click="resume">
          提交并继续（异步 + 继续推送）
        </el-button>
      </template>
    </el-card>

    <el-card v-if="result && result.final_answer" class="section-card">
      <template #header><span>综合答复</span></template>
      <div class="synthesized-answer">{{ result.final_answer }}</div>
    </el-card>

    <el-card v-if="view && view.intents && view.intents.length" class="section-card intents-card">
      <template #header><span>各意图详情</span></template>
      <el-tabs v-model="activeIntentTab" type="card" class="intent-tabs">
        <el-tab-pane v-for="intent in view.intents" :key="intent.intent_id" :name="intent.intent_id">
          <template #label>
            <span>
              <el-tag size="small" :type="statusTagType(intent.status)" style="margin-right: 6px">{{ intent.intent_id }}</el-tag>
              {{ (intent.description || intent.intent_id).slice(0, 24) }}{{ (intent.description || '').length > 24 ? '…' : '' }}
            </span>
          </template>
          <IntentDetail :intent="intent" />
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <el-card v-if="result && result.status === 'FAILED'" class="section-card">
      <el-alert type="error" :title="result.error?.current_stage || '失败'" :description="result.error?.message || 'workflow failed'" show-icon />
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import { initApi } from '../api'
import { useQueryStore } from '../stores/queryStore'
import RealTimeProgress from '../components/RealTimeProgress.vue'
import IntentDetail from '../components/IntentDetail.vue'

const queryStore = useQueryStore()
const query = ref('')
const initOk = ref(false)
const userReply = ref('')
const activeIntentTab = ref('')

const result = computed(() => queryStore.result)
const view = computed(() => queryStore.view)

const isWaitUser = computed(() => queryStore.isWaitUser)

const blockerPhase = computed(() => {
  const b = result.value?.ask_ticket
  if (!b) return ''
  return (b.resume_point?.phase || '').replace(/_/g, ' ') || ''
})

const canResume = computed(() => {
  if (!result.value?.ask_ticket?.ticket_id) return false
  return String(userReply.value || '').trim().length > 0
})

function formatBlockerItem(item) {
  if (item == null) return ''
  if (typeof item === 'string') return item.trim()
  if (typeof item === 'object') {
    const s = item.text ?? item.label ?? item.value ?? item.question ?? item.message
    if (s != null && typeof s === 'string') return s.trim()
    try {
      return JSON.stringify(item)
    } catch (_) {
      return String(item)
    }
  }
  return String(item).trim()
}

const blockerMessage = computed(() => {
  if (!result.value?.ask_ticket) return ''
  const b = result.value.ask_ticket
  const parts = []
  const title = b.question_id || '澄清'
  parts.push(`[${title}]`)
  const question = formatBlockerItem(b.question)
  if (question) parts.push(question)
  const whyNeeded = formatBlockerItem(b.why_needed)
  if (whyNeeded) parts.push(whyNeeded)
  const acceptance = b.acceptance_criteria
  if (Array.isArray(acceptance) && acceptance.length) {
    parts.push('请至少补充：')
    acceptance.forEach((item) => parts.push(`- ${formatBlockerItem(item)}`))
  }
  return parts.length > 1 ? parts.join('\n\n') : parts[0] || question || '请补充信息'
})

watch(
  () => view.value?.intents,
  (intents) => {
    if (intents?.length && !activeIntentTab.value) activeIntentTab.value = intents[0].intent_id
  },
  { immediate: true }
)

function onSelectIntent(id) {
  if (id && view.value?.intents?.some((i) => i.intent_id === id)) activeIntentTab.value = id
}

function statusTagType(s) {
  const v = String(s || '').toUpperCase()
  if (v === 'COMPLETED') return 'success'
  if (v === 'FAILED' || v === 'BLOCKED_BY_UPSTREAM') return 'danger'
  if (v === 'WAIT_USER') return 'warning'
  if (v === 'RUNNING' || v === 'READY') return 'primary'
  return 'info'
}

async function run() {
  const q = (query.value || '').trim()
  if (!q) {
    ElMessage.warning('请输入问题')
    return
  }
  queryStore.reset()
  activeIntentTab.value = ''
  try {
    await queryStore.runQueryAsync(q)
    if (isWaitUser.value) userReply.value = ''
  } catch (e) {
    ElMessage.error(e.message || '执行失败')
  }
}

async function resume() {
  const msg = (userReply.value || '').trim()
  if (!msg) {
    ElMessage.warning('请输入回复内容')
    return
  }
  if (!queryStore.workflowId || !result.value?.ask_ticket?.ticket_id) {
    ElMessage.error('缺少 workflow_id 或 ticket_id')
    return
  }
  try {
    await queryStore.resumeAsync(queryStore.workflowId, result.value.ask_ticket.ticket_id, msg)
    userReply.value = ''
  } catch (e) {
    ElMessage.error(e.message || '恢复失败')
  }
}

async function fetchInitStatus() {
  try {
    const s = await initApi.status()
    initOk.value = !!s.is_initialized
  } catch (_) {
    initOk.value = false
  }
}

function onVisible() {
  if (document.visibilityState === 'visible') fetchInitStatus()
}

onMounted(() => {
  fetchInitStatus()
  document.addEventListener('visibilitychange', onVisible)
})

onUnmounted(() => {
  document.removeEventListener('visibilitychange', onVisible)
  queryStore.disconnectStream()
})
</script>

<style scoped>
.query-page {
  width: 100%;
  max-width: 1600px;
  margin: 0 auto;
  padding: 0 4px;
}
.mb12 {
  margin-bottom: 12px;
}
.input-card {
  margin-bottom: 16px;
}
.query-form {
  width: 100%;
}
.query-textarea {
  width: 100%;
}
.section-card {
  margin-bottom: 16px;
}
.blocker-message {
  margin-bottom: 12px;
  color: var(--el-text-color-regular);
  white-space: pre-line;
}
.synthesized-answer {
  white-space: pre-wrap;
  line-height: 1.6;
}
.intents-card {
  width: 100%;
}
.intent-tabs {
  width: 100%;
}
</style>
