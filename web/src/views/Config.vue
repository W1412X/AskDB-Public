<template>
  <div class="config-page">
    <el-card>
      <template #header>
        <span>配置管理</span>
      </template>
      <p class="hint">编辑 config 目录下的配置文件，保存后需在「初始化」中重新执行初始化后生效。</p>
      <el-form label-width="100px">
        <el-form-item label="配置文件">
          <el-select v-model="currentFile" :loading="loadingFiles" placeholder="选择文件" @change="loadFile" style="width: 220px">
            <el-option v-for="f in files" :key="f" :label="f" :value="f" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="currentFile" label="内容">
          <el-input
            v-model="content"
            type="textarea"
            :rows="18"
            placeholder="JSON 内容"
            font-monospace
          />
        </el-form-item>
        <el-form-item v-if="currentFile">
          <el-button type="primary" :loading="saving" @click="save">保存</el-button>
          <el-button @click="loadFile">重新加载</el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { configApi } from '../api'

const files = ref([])
const currentFile = ref('')
const content = ref('')
const saving = ref(false)
const loadingFiles = ref(false)

async function loadFiles() {
  loadingFiles.value = true
  try {
    const nextFiles = await configApi.listFiles()
    files.value = Array.isArray(nextFiles) ? nextFiles : []
    if (files.value.length && (!currentFile.value || !files.value.includes(currentFile.value))) {
      currentFile.value = files.value[0]
    }
    if (!files.value.length) {
      currentFile.value = ''
      content.value = ''
      ElMessage.warning('未获取到可编辑的配置文件')
    }
  } catch (e) {
    files.value = []
    currentFile.value = ''
    content.value = ''
    ElMessage.error(e.message || '加载配置文件列表失败')
  } finally {
    loadingFiles.value = false
  }
}

async function loadFile() {
  if (!currentFile.value) return
  try {
    const data = await configApi.get(currentFile.value)
    content.value = JSON.stringify(data, null, 2)
  } catch (e) {
    ElMessage.error(e.message || '加载失败')
  }
}

async function save() {
  if (!currentFile.value) return
  let obj
  try {
    obj = JSON.parse(content.value)
  } catch (_) {
    ElMessage.error('JSON 格式错误')
    return
  }
  saving.value = true
  try {
    await configApi.put(currentFile.value, obj)
    ElMessage.success('保存成功')
  } catch (e) {
    ElMessage.error(e.message || '保存失败')
  } finally {
    saving.value = false
  }
}

onMounted(() => {
  loadFiles().then(() => {
    if (currentFile.value) loadFile()
  })
})
</script>

<style scoped>
.config-page { width: 100%; max-width: 1100px; }
.hint { color: var(--el-text-color-secondary); font-size: 13px; margin-bottom: 16px; }
</style>
