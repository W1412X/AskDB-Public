<template>
  <div class="init-page">
    <el-card>
      <template #header>
        <span>初始化</span>
      </template>
      <p class="hint">执行初始化后，配置与数据目录将生效。</p>
      <el-button type="primary" :loading="loading" :disabled="status?.status === 'running'" @click="runInit">
        {{ status?.status === 'running' ? '初始化中…' : '执行初始化' }}
      </el-button>
      <div v-if="status?.status === 'running' || (status?.logs && status.logs.length)" class="log-area">
        <div v-for="(line, i) in (status?.logs || [])" :key="i" class="log-line">{{ line.message || line }}</div>
      </div>
      <p v-if="status?.status === 'success'" class="success-hint">初始化已完成，可前往「问答」使用。</p>
      <p v-if="status?.error" class="error-hint">{{ status.error }}</p>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import { initApi } from '../api'

const loading = ref(false)
const status = ref(null)
let pollTimer = null

async function fetchStatus() {
  try {
    status.value = await initApi.status()
  } catch (_) {
    status.value = null
  }
}

function startPolling() {
  if (pollTimer) return
  pollTimer = setInterval(fetchStatus, 1500)
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

async function runInit() {
  loading.value = true
  try {
    await initApi.start()
    ElMessage.success('已开始初始化，请稍候')
    startPolling()
  } catch (e) {
    ElMessage.error(e?.message || '启动初始化失败')
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  await fetchStatus()
  if (status.value?.status === 'running') startPolling()
})
onUnmounted(stopPolling)
</script>

<style scoped>
.init-page .hint { margin-bottom: 1rem; color: var(--el-text-color-secondary); }
.init-page .log-area { margin-top: 1rem; max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 12px; background: var(--el-fill-color-light); padding: 8px; border-radius: 4px; }
.init-page .log-line { margin: 2px 0; }
.init-page .success-hint { margin-top: 1rem; color: var(--el-color-success); }
.init-page .error-hint { margin-top: 1rem; color: var(--el-color-danger); }
</style>
