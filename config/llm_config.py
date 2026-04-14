from __future__ import annotations

from config.app_config import ModelSpec
from config.settings_manager import get_settings_manager


def get_llm(model_code: str | None = None):
    """
    获取 LLM 实例。

    Args:
        model_code: 模型 code，即 models.json 里 providers.*.models 的键名；
            省略时使用 default_model。
    Returns:
        LangChain ChatModel 实例
    """
    cfg = get_settings_manager().config
    code = str(model_code or cfg.models.default_model)
    spec = cfg.get_model(code)
    policy = cfg.models.call_policy

    # OpenAI 兼容协议：qwen / aliyun 为百炼 compatible-mode；deepseek / openai 为官方网关
    if spec.provider in {"qwen", "aliyun", "deepseek", "openai"}:
        from langchain_openai import ChatOpenAI

        model = ChatOpenAI(
            model_name=spec.model_name,
            api_key=spec.api_key,
            base_url=spec.base_url or None,
            timeout=policy.timeout_seconds,
            max_retries=policy.max_transport_retries,
        )
    else:
        raise ValueError(f"Unsupported model provider: {spec.provider}")
    setattr(model, "_codex_model_name", code)
    setattr(model, "_codex_model_factory", get_llm)
    return model


def get_llm_model_spec(model_code: str | None = None) -> ModelSpec:
    """仅传入模型 code，返回已做环境变量覆盖后的 ModelSpec（不构造 LangChain 客户端）。"""
    return get_settings_manager().config.get_model(model_code)
