import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  { path: '/', redirect: '/config' },
  { path: '/config', name: 'Config', component: () => import('../views/Config.vue'), meta: { title: '配置' } },
  { path: '/init', name: 'Init', component: () => import('../views/Init.vue'), meta: { title: '初始化' } },
  { path: '/query', name: 'Query', component: () => import('../views/Query.vue'), meta: { title: '问答' } },
]

const router = createRouter({ history: createWebHistory(), routes })
router.beforeEach((to, _from, next) => {
  document.title = to.meta?.title ? `${to.meta.title} - AskDB` : 'AskDB'
  next()
})
export default router
