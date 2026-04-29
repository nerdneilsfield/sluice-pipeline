import re
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter

from sluice.core.errors import ConfigError

ALLOWED_VARS = {
    "pipeline_id",
    "run_date",
    "run_hour",
    "run_minute",
    "run_iso",
    "run_epoch",
}


def _vars(pipeline_id: str, dt: datetime) -> dict[str, str]:
    return {
        "pipeline_id": pipeline_id,
        "run_date": dt.strftime("%Y-%m-%d"),
        "run_hour": dt.strftime("%H"),
        "run_minute": dt.strftime("%M"),
        "run_iso": dt.isoformat(timespec="seconds"),
        "run_epoch": str(int(dt.timestamp())),
    }


def render_run_key(template: str, pipeline_id: str, dt: datetime) -> str:
    return template.format(**_vars(pipeline_id, dt))


def validate_template(
    template: str,
    cron: str,
    timezone_name: str,
    *,
    base: datetime | None = None,
) -> None:
    if "{pipeline_id}" not in template:
        raise ConfigError("run_key_template must contain {pipeline_id}")
    for name in re.findall(r"\{([^}]+)\}", template):
        if name not in ALLOWED_VARS:
            raise ConfigError(
                f"run_key_template uses unknown variable {{{name}}}; "
                f"allowed: {sorted(ALLOWED_VARS)}"
            )
    tz = ZoneInfo(timezone_name)
    base = base.astimezone(tz) if base is not None else datetime.now(tz=tz)
    it = croniter(cron, base)
    seen: set[str] = set()
    for _ in range(48):
        nxt = it.get_next(datetime)
        key = render_run_key(template, "p", nxt)
        if key in seen:
            raise ConfigError(
                f"run_key_template {template!r} does not vary with schedule "
                f"cadence {cron!r}; runs would collide in sink_emissions"
            )
        seen.add(key)
