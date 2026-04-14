from __future__ import annotations

from collections.abc import Sequence


def numbered_list(items: Sequence[str], *, start: int = 1) -> str:
    return "\n".join(f"{index}. {item}" for index, item in enumerate(items, start))


def compose_sections(*sections: str) -> str:
    return "\n".join(sections)


def build_json_system_prompt(
    *,
    role_line: str,
    mission_line: str,
    rules: Sequence[str],
    extra_sections: Sequence[str] = (),
) -> str:
    sections = [role_line, mission_line, ""]
    sections.extend(extra_sections)
    sections.extend(("必须遵守：", numbered_list(rules)))
    return compose_sections(*sections)


def build_json_user_prompt(
    *,
    steps_block: str,
    task_input_json: str,
    output_schema: str,
    requirements: Sequence[str],
    requirement_prefix: Sequence[str] = (),
    requirement_start: int = 1,
) -> str:
    sections = [
        steps_block,
        "",
        "【任务输入】",
        task_input_json,
        "",
        "【输出要求】",
    ]
    sections.extend(requirement_prefix)
    sections.append(numbered_list(requirements, start=requirement_start))
    sections.extend(("", "目标输出 schema：", output_schema, "", "只返回 JSON。"))
    return compose_sections(*sections)
