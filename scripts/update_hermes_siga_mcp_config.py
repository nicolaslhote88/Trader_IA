from __future__ import annotations

import datetime
import shutil
from pathlib import Path

import yaml


CONFIG = Path("/home/hermeswebui/.hermes/config.yaml")
WRAPPER = (
    "if [ -d /home/hermeswebui/.hermes ]; then "
    "H=/home/hermeswebui/.hermes; PY=/app/venv/bin/python; "
    "else H=/home/hermes/.hermes; PY=$H/mcp-venv/bin/python; fi; "
    "export GDRIVE_CREDENTIALS_PATH=$H/secrets/siga-gdrive-credentials.json "
    "GDRIVE_OAUTH_PATH=$H/secrets/gcp-oauth.keys.json; "
    'exec "$PY" "$H/mcp/siga_drive_mcp.py"'
)


def main() -> None:
    backup = CONFIG.with_name(
        "config.yaml.bak." + datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    )
    shutil.copy2(CONFIG, backup)
    data = yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}
    servers = data.setdefault("mcp_servers", {})
    servers["siga-drive"] = {
        "command": "/bin/sh",
        "args": ["-lc", WRAPPER],
        "env": {
            "SIGA_FOLDER_ID": "1zbAqyzAw2UlesMBLKpbLJB6zF_sqeTB2",
            "SIGA_TEMP_FOLDER_ID": "1v70CQpVSwDszlRkNei2SYF2SoEpxWHHO",
        },
        "enabled": True,
    }
    CONFIG.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
    print(f"backup={backup}")
    print("updated=siga-drive")


if __name__ == "__main__":
    main()
