# AskDB Web UI

浏览器端配置、初始化与问答；后端为 **FastAPI**，与 CLI 共用同一套 `config`、`stages/query_workflow` 流水线。

## 依赖

在项目根目录已执行 `pip install -r requirements.txt` 后，应已包含 `fastapi` 与 `uvicorn`。若单独最小安装，至少需要：

```bash
pip install fastapi "uvicorn[standard]"
```

## 后端 (FastAPI)

在项目根目录执行（确保 `PYTHONPATH` 包含项目根，在仓库根目录运行即可）：

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- OpenAPI 文档：http://127.0.0.1:8000/docs  
- 路由前缀：`/api/config`（读写 JSON 配置）、`/api/init`（初始化状态与启动）、`/api/query`（同步/异步查询、SSE、resume）

若本机 `8000` 端口已被占用，可改用其他端口，例如：

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8010
```

## 前端 (Vue 3 + Element Plus)

```bash
cd web
npm install
# 若后端不是 8000，可先指定代理目标
# export VITE_API_PROXY_TARGET=http://127.0.0.1:8010
# 或直接指定前端请求基址
# export VITE_API_BASE_URL=http://127.0.0.1:8010/api
npm run dev
```

浏览器打开 http://localhost:5173

## 使用流程

1. **配置**：在「配置」页选择并编辑 `database.json` / `models.json` / `stages.json`，保存后会触发服务端 `reload_app_config()`（及数据库工具重载，如适用）。  
   - `models.json` 为 **供应商（providers）+ 模型 code** 结构：密钥与 `base_url` 写在供应商上；`stages.json` 与各处的 `model_name` 应填写 **`providers.*.models` 的键（模型 code）**，详见 [config/README.md](config/README.md)。
2. **初始化**：在「初始化」页启动后台初始化任务，轮询状态与日志；完成后可进行问答（与 CLI 相同，依赖 `data/initialize/` 产物）。
3. **问答**：输入自然语言并执行；可查看综合答复、意图依赖与各意图详情。若状态为「需要补充信息」，填写后「提交并继续」会调用 resume 接口。异步跑数时可配合 `/api/query/stream/{workflow_id}` 的 SSE 订阅进度。

## 与 CLI 的关系

- 数据目录、工作流存储、日志与 `main.py` 一致。  
- 查询逻辑均通过 `stages.query_workflow.facade`（`run_query_workflow` / `resume_query_workflow`）进入 `QueryWorkflowPipeline`。
