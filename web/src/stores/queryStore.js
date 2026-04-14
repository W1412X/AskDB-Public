import { defineStore } from 'pinia'
import { ref, shallowRef, computed } from 'vue'
import { queryApi, queryStreamUrl } from '../api'

export const useQueryStore = defineStore('query', () => {
  const workflowId = ref('')
  const snapshot = shallowRef(null)
  const loading = ref(false)
  const resuming = ref(false)
  const streamConnected = ref(false)
  const streamError = ref('')
  let eventSource = null

  const result = computed(() => {
    const s = snapshot.value
    if (!s) return null
    return {
      status: s.status,
      workflow_id: s.workflow_id,
      final_answer: s.final_answer || '',
      ask_ticket: s.ask_ticket,
      error: s.error,
      intent_results: s.intent_results || [],
      view: s.view,
    }
  })

  const view = computed(() => snapshot.value?.view || null)
  const isTerminal = computed(() => !!snapshot.value?.terminal)
  const isWaitUser = computed(() => String(snapshot.value?.status || '').toUpperCase() === 'WAIT_USER')

  function disconnectStream() {
    if (eventSource) {
      try {
        eventSource.close()
      } catch (_) {}
      eventSource = null
    }
    streamConnected.value = false
  }

  function applyPayload(data) {
    if (!data || data.event === 'not_found') {
      streamError.value = 'workflow not found'
      return
    }
    if (data.event !== 'snapshot') return
    const { event: _e, ...rest } = data
    snapshot.value = rest
    streamError.value = ''
  }

  function connectStream(wf) {
    disconnectStream()
    workflowId.value = wf
    streamConnected.value = true
    streamError.value = ''
    const url = queryStreamUrl(wf)
    eventSource = new EventSource(url)
    eventSource.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data)
        applyPayload(data)
        if (data.terminal) disconnectStream()
      } catch (e) {
        streamError.value = String(e.message || e)
      }
    }
    eventSource.onerror = () => {
      streamConnected.value = false
      if (eventSource?.readyState === EventSource.CLOSED) return
    }
  }

  async function runQueryAsync(queryText, extra = {}) {
    loading.value = true
    streamError.value = ''
    snapshot.value = null
    try {
      const { workflow_id: wf } = await queryApi.runAsync({ query: queryText, ...extra })
      connectStream(wf)
    } catch (e) {
      streamError.value = e.message || String(e)
      throw e
    } finally {
      loading.value = false
    }
  }

  async function resumeAsync(wf, ticketId, reply) {
    resuming.value = true
    streamError.value = ''
    try {
      await queryApi.resumeAsync({ workflow_id: wf, ticket_id: ticketId, reply })
      connectStream(wf)
    } catch (e) {
      streamError.value = e.message || String(e)
      throw e
    } finally {
      resuming.value = false
    }
  }

  /** Fallback polling if SSE blocked */
  async function pollUntilTerminal(wf, intervalMs = 400) {
    workflowId.value = wf
    while (true) {
      const s = await queryApi.status(wf)
      snapshot.value = s
      if (s.terminal) break
      await new Promise((r) => setTimeout(r, intervalMs))
    }
  }

  function reset() {
    disconnectStream()
    workflowId.value = ''
    snapshot.value = null
    streamError.value = ''
  }

  return {
    workflowId,
    snapshot,
    loading,
    resuming,
    streamConnected,
    streamError,
    result,
    view,
    isTerminal,
    isWaitUser,
    runQueryAsync,
    resumeAsync,
    pollUntilTerminal,
    disconnectStream,
    reset,
  }
})
