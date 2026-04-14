<template>
  <el-config-provider :locale="locale">
    <el-container class="layout">
      <el-header class="header">
        <span class="title">AskDB</span>
        <el-menu
          :default-active="activeMenu"
          mode="horizontal"
          router
          background-color="#409eff"
          text-color="#fff"
          active-text-color="#fff"
        >
          <el-menu-item index="/config">配置</el-menu-item>
          <el-menu-item index="/init">初始化</el-menu-item>
          <el-menu-item index="/query">问答</el-menu-item>
        </el-menu>
      </el-header>
      <el-main class="main">
        <router-view v-slot="{ Component }">
          <transition name="fade" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </el-main>
    </el-container>
  </el-config-provider>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import zhCn from 'element-plus/es/locale/lang/zh-cn'

const locale = zhCn
const route = useRoute()
const activeMenu = computed(() => route.path || '/config')
</script>

<style>
* { box-sizing: border-box; }
html, body, #app { height: 100%; margin: 0; }
.layout { height: 100%; flex-direction: column; }
.header {
  display: flex;
  align-items: center;
  padding: 0 16px;
  background: #409eff;
  color: #fff;
}
.title { font-size: 1.25rem; font-weight: 600; margin-right: 24px; }
.header .el-menu { flex: 1; border: none; }
.main { flex: 1; overflow: auto; padding: 16px 24px; width: 100%; }
.fade-enter-active, .fade-leave-active { transition: opacity 0.15s ease; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
</style>
