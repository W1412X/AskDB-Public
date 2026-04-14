from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

# 从项目根目录加载 .env，使 api_key_env / password_env 等生效
_project_root = Path(__file__).resolve().parents[1]
_dotenv_path = _project_root / ".env"
if _dotenv_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_dotenv_path)


def _config_dir() -> Path:
    env_dir = str(os.getenv("APP_CONFIG_DIR") or "").strip()
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    return (Path(__file__).resolve().parent / "json").resolve()


def get_config_dir() -> Path:
    """Return the directory containing database.json, models.json, stages.json (for Web UI read/write)."""
    return _config_dir()


def _load_json_config(filename: str) -> Dict[str, Any]:
    path = _config_dir() / filename
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"config file must contain a JSON object: {path}")
    return data


def _env_override(value: str, env_name: str) -> str:
    env_value = str(os.getenv(env_name) or "").strip()
    return env_value or value


class DatabaseConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str
    port: int = 3306
    user: str
    password: str = ""
    password_env: str = ""
    database: Optional[str] = None
    charset: str = "utf8mb4"
    mincached: int = 1
    maxcached: int = 5
    maxshared: int = 3
    maxconnections: int = 10
    blocking: bool = True
    maxusage: int = 1000
    setsession: List[str] = Field(default_factory=list)
    reset: bool = True
    ping: int = 1


class DatabaseSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_connection: str
    default_scope: List[str] = Field(default_factory=list)
    initialize_databases: List[str] = Field(default_factory=list)
    query_databases: List[str] = Field(default_factory=list)
    connections: Dict[str, DatabaseConnectionConfig] = Field(default_factory=dict)


class LLMCallPolicySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timeout_seconds: int = 90
    max_transport_retries: int = 2
    max_format_retries: int = 3
    retryable_error_classes: List[str] = Field(default_factory=lambda: ["timeout", "transport", "rate_limit"])


# models.json 中允许的供应商 id（与 get_llm 路由一致）
KNOWN_LLM_PROVIDERS: frozenset[str] = frozenset({"aliyun", "deepseek", "openai", "qwen"})
_DEFAULT_PROVIDER_BASE_URL: Dict[str, str] = {
    "aliyun": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "qwen": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "deepseek": "https://api.deepseek.com/v1",
    "openai": "https://api.openai.com/v1",
}


class ModelSpec(BaseModel):
    """合并供应商后的运行时模型规格（供 LangChain 等使用）。"""

    model_config = ConfigDict(extra="forbid")

    provider: str
    model_name: str  # 上游 API 模型 id，来自模型条目的 code
    api_key: str = ""
    api_key_env: str = ""
    base_url: str = ""
    base_url_env: str = ""
    supports_structured_output: bool = False


class LLMModelEntryConfig(BaseModel):
    """某供应商下的单个模型：code 为调用厂商 API 时使用的模型 id。"""

    model_config = ConfigDict(extra="forbid")

    code: str = ""
    supports_structured_output: bool = False


class LLMProviderConfig(BaseModel):
    """单个供应商：api_key / base_url 仅在此处配置，其下挂多个模型 code。"""

    model_config = ConfigDict(extra="forbid")

    api_key: str = ""
    api_key_env: str = ""
    base_url: str = ""
    base_url_env: str = ""
    models: Dict[str, LLMModelEntryConfig] = Field(default_factory=dict)


class ModelsSettings(BaseModel):
    """
    models.json：default_model / fallback_order / call_policy / providers。
    业务侧只使用「模型 code」——即 providers.*.models 的键名——调用 get_model(code) / get_llm(code)。
    """

    model_config = ConfigDict(extra="forbid")

    default_model: str
    fallback_order: List[str] = Field(default_factory=list)
    call_policy: LLMCallPolicySettings = Field(default_factory=LLMCallPolicySettings)
    providers: Dict[str, LLMProviderConfig]

    _resolved_specs: Dict[str, ModelSpec] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def _build_resolved_specs(self) -> ModelsSettings:
        if not self.providers:
            raise ValueError("models.json: 'providers' must be a non-empty object")
        if not str(self.default_model or "").strip():
            raise ValueError("models.json: 'default_model' is required")

        resolved: Dict[str, ModelSpec] = {}
        for provider_id, prov in self.providers.items():
            pid = str(provider_id).strip()
            if pid not in KNOWN_LLM_PROVIDERS:
                raise ValueError(
                    f"models.json: unknown provider {provider_id!r}; "
                    f"allowed: {', '.join(sorted(KNOWN_LLM_PROVIDERS))}"
                )
            if not prov.models:
                raise ValueError(f"models.json: providers.{pid}.models must be non-empty")
            base_url = str(prov.base_url or "").strip() or _DEFAULT_PROVIDER_BASE_URL.get(pid, "")
            for model_code, entry in prov.models.items():
                mcode = str(model_code).strip()
                if not mcode:
                    continue
                if mcode in resolved:
                    raise ValueError(f"models.json: duplicate model code {mcode!r}")
                api_model_id = str(entry.code or "").strip() or mcode
                resolved[mcode] = ModelSpec(
                    provider=pid,
                    model_name=api_model_id,
                    api_key=str(prov.api_key or ""),
                    api_key_env=str(prov.api_key_env or ""),
                    base_url=base_url,
                    base_url_env=str(prov.base_url_env or ""),
                    supports_structured_output=bool(entry.supports_structured_output),
                )

        dm = str(self.default_model).strip()
        if dm not in resolved:
            raise ValueError(f"models.json: default_model {dm!r} is not defined under any provider.models")
        for cand in self.fallback_order:
            c = str(cand or "").strip()
            if c and c not in resolved:
                raise ValueError(f"models.json: fallback_order entry {c!r} is not a defined model code")

        self._resolved_specs = resolved
        return self

    def iter_model_codes(self) -> List[str]:
        return sorted(self._resolved_specs.keys())

    def has_model_code(self, model_code: str) -> bool:
        return str(model_code or "").strip() in self._resolved_specs

    def raw_model_spec(self, model_code: str) -> ModelSpec:
        key = str(model_code or "").strip()
        spec = self._resolved_specs.get(key)
        if spec is None:
            raise KeyError(f"unknown model code: {key}")
        return spec

    def supports_structured_output_for_code(self, model_code: str) -> bool:
        spec = self._resolved_specs.get(str(model_code or "").strip())
        return bool(spec.supports_structured_output) if spec is not None else False


class QueryWorkflowSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_parallel_intents: int = 4
    max_steps: int = 50
    max_json_retries: int = 2
    max_semantic_retries: int = 1
    max_schemalink_rounds: int = 32
    max_repair_attempts_per_intent: int = 16
    max_ask_turns_per_ticket: int = 3
    max_rows: int = 100
    sql_timeout_ms: int = 30000
    max_decompose_self_repair: int = 3
    agent_runner_tool_round_cap: int = 8


class InitializeAgentSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str


class InitializeEmbeddingSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    model_path_name: str
    normalize_embeddings: bool = True
    batch_size: int = 32
    device: str = ""
    overwrite: bool = False
    hf_endpoint: str = ""  # HuggingFace 镜像或 API 地址，如 https://hf-mirror.com，空则用默认
    max_length: int = 512
    trust_remote_code: bool = False


class InitializeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent: InitializeAgentSettings
    embedding: InitializeEmbeddingSettings


class GeneralSummarySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str
    max_input_length: int = 10000


class GeneralSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: GeneralSummarySettings


class ColumnAgentSamplingSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    random_sample_max: int = 10
    distinct_sample_max: int = 20
    truncate_length_max: int = 1024
    pattern_analysis_sample_size: int = 100
    table_context_sample_size: int = 5


class ColumnAgentWorkflowSamplingSettings(BaseModel):
    """initialize/agent/workflow 列采样 SQL 与截断（与 sampling.* 用途不同）。"""

    model_config = ConfigDict(extra="forbid")

    random_sample_n: int = 8
    distinct_sample_n: int = 12
    table_context_n: int = 5
    table_context_extra_cols: int = 5
    truncate_length: int = 200
    max_rows_for_distinct_count: int = 200_000


class ColumnAgentMaxTokensPerField(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sample_value: int = 200
    comment: int = 500
    semantic_summary: int = 1000


class ColumnAgentTokenSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_tokens_per_field: ColumnAgentMaxTokensPerField = Field(default_factory=ColumnAgentMaxTokensPerField)
    max_total_tokens: int = 8000


class ColumnAgentRetrySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_retries: int = 3
    retry_backoff_factor: int = 2
    retry_timeout: int = 30


class ColumnAgentParallelSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_parallel_columns: int = 5


class ColumnAgentSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sampling: ColumnAgentSamplingSettings = Field(default_factory=ColumnAgentSamplingSettings)
    workflow_sampling: ColumnAgentWorkflowSamplingSettings = Field(default_factory=ColumnAgentWorkflowSamplingSettings)
    token: ColumnAgentTokenSettings = Field(default_factory=ColumnAgentTokenSettings)
    retry: ColumnAgentRetrySettings = Field(default_factory=ColumnAgentRetrySettings)
    parallel: ColumnAgentParallelSettings = Field(default_factory=ColumnAgentParallelSettings)


class SchemaToolAgentStageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_tool_calls_per_round: int = 1
    max_tool_rounds: int = 8


class SchemaGateStageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    llm_cache_max_entries: int = 16


class JoinPathSearchSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_depth_hard_cap: int = 5
    min_edge_score: float = 0.18
    min_path_score: float = 0.18
    max_frontier_size: int = 200
    max_anchors_per_table: int = 6
    max_columns_per_database: int = 80
    min_improvement: float = 0.03
    no_improve_layers: int = 2


class WorkflowStoreStageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_items: int = 200


class SqlExplorerStageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_limit: int = 100
    default_timeout_ms: int = 30000


class StageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query_workflow: QueryWorkflowSettings
    initialize: InitializeSettings
    general: GeneralSettings
    column_agent: ColumnAgentSettings
    schema_tool_agent: SchemaToolAgentStageSettings = Field(default_factory=SchemaToolAgentStageSettings)
    schema_gate: SchemaGateStageSettings = Field(default_factory=SchemaGateStageSettings)
    join_path_search: JoinPathSearchSettings = Field(default_factory=JoinPathSearchSettings)
    workflow_store: WorkflowStoreStageSettings = Field(default_factory=WorkflowStoreStageSettings)
    sql_explorer: SqlExplorerStageSettings = Field(default_factory=SqlExplorerStageSettings)


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: DatabaseSettings
    models: ModelsSettings
    stages: StageSettings

    def get_database_connection(self, name: Optional[str] = None) -> DatabaseConnectionConfig:
        connection_name = str(name or self.database.default_connection)
        connection = self.database.connections.get(connection_name)
        if connection is None:
            raise KeyError(f"unknown database connection: {connection_name}")
        password = connection.password
        if connection.password_env:
            password = _env_override(password, connection.password_env)
        return connection.model_copy(update={"password": password})

    def get_default_database_name(self) -> str:
        return str(self.get_database_connection().database or "")

    def get_default_database_scope(self) -> List[str]:
        if self.database.query_databases:
            return list(self.database.query_databases)
        if self.database.default_scope:
            return list(self.database.default_scope)
        default_db = self.get_default_database_name()
        return [default_db] if default_db else []

    def get_initialize_databases(self) -> List[str]:
        if self.database.initialize_databases:
            return list(self.database.initialize_databases)
        return self.get_default_database_scope()

    def get_model(self, model_code: Optional[str] = None) -> ModelSpec:
        """按模型 code（providers.*.models 的键）解析完整 ModelSpec（含 env 覆盖后的密钥与 base_url）。"""
        code = str(model_code or self.models.default_model).strip()
        spec = self.models.raw_model_spec(code)
        api_key = spec.api_key
        if spec.api_key_env:
            api_key = _env_override(api_key, spec.api_key_env)
        base_url = spec.base_url
        if spec.base_url_env:
            base_url = _env_override(base_url, spec.base_url_env)
        return spec.model_copy(update={"api_key": api_key, "base_url": base_url})

    def get_fallback_model_name(self, current_model_name: str = "") -> str:
        current = str(current_model_name or "").strip()
        for candidate in self.models.fallback_order:
            c = str(candidate or "").strip()
            if c and c != current and self.models.has_model_code(c):
                return c
        return ""

    def get_stage_model_name(self, stage_path: str) -> str:
        current: Any = self.stages
        for part in [token for token in stage_path.split(".") if token]:
            if not hasattr(current, part):
                break
            current = getattr(current, part)
        model_name = getattr(current, "model_name", "")
        return str(model_name or self.models.default_model)

    def langchain_models_compat(self) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for code in self.models.iter_model_codes():
            spec = self.get_model(code)
            result[code] = spec.model_dump(mode="json")
        return result

    def database_config_compat(self) -> Dict[str, Any]:
        return self.get_database_connection().model_dump(mode="json")


@lru_cache(maxsize=1)
def get_app_config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "database": _load_json_config("database.json"),
            "models": _load_json_config("models.json"),
            "stages": _load_json_config("stages.json"),
        }
    )


def reload_app_config() -> AppConfig:
    get_app_config.cache_clear()
    return get_app_config()
