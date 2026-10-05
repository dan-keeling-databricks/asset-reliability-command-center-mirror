from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_adapter: Literal["local", "lakebase"] = "local"
    telemetry_interval_seconds: float = 1.0
    alert_evaluator_interval_seconds: float = 1.0
    local_event_step_seconds: float = 0.3
    lakebase_dsn: str | None = None
    lakebase_schema: str = "asset_reliability"
    lakebase_job_principal: str | None = None
    lakebase_migrate_on_start: bool = True
    sse_coalesce_seconds: float = 0.05
    telemetry_retention_seconds: int = 86_400
    telemetry_retention_interval_seconds: float = 300.0
    telemetry_retention_batch_size: int = 1_000
    databricks_host: str | None = None
    local_databricks_auth: bool = False
    databricks_config_profile: str | None = None
    primary_model_endpoint: str = "databricks-claude-sonnet-4-5"
    fast_model_endpoint: str = "databricks-gpt-5-1"
    governed_mcp_endpoint: str | None = None
    governed_skill_endpoint: str | None = None
    governed_skill_tool: str | None = None
    approval_poll_seconds: float = 2.0
    model_input_cost_per_million: float = 3.0
    model_output_cost_per_million: float = 15.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
