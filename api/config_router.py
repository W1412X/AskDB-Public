"""
Config file read/write API.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException

from config.app_config import get_config_dir, reload_app_config
from utils.database_tool import reload_db_tool

router = APIRouter(prefix="/api/config", tags=["config"])

ALLOWED_FILES = ("database.json", "models.json", "stages.json")


def _config_path(filename: str) -> Path:
    if filename not in ALLOWED_FILES:
        raise HTTPException(status_code=400, detail=f"Invalid config file: {filename}")
    return get_config_dir() / filename


@router.get("/files", response_model=List[str])
def list_config_files() -> List[str]:
    """Return list of editable config filenames."""
    return list(ALLOWED_FILES)


@router.get("/{filename}", response_model=Dict[str, Any])
def get_config(filename: str) -> Dict[str, Any]:
    """Return JSON content of config file."""
    path = _config_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Config file not found: {filename}")
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON: {e}")


@router.put("/{filename}", response_model=Dict[str, Any])
def put_config(filename: str, body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Overwrite config file with JSON body. Reloads app config after write."""
    path = _config_path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(body, f, ensure_ascii=False, indent=2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    try:
        reload_app_config()
        reload_db_tool()
    except Exception:
        pass
    return {"ok": True, "filename": filename}


@router.post("/reload")
def reload_config() -> Dict[str, str]:
    """Reload app config from disk and refresh DB connection (e.g. after external edit or to apply new config)."""
    try:
        reload_app_config()
        reload_db_tool()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
