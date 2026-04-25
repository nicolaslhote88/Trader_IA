return [{"json": {**(it.get("json", {}) or {}), "vector_status": "SKIPPED_V1"}} for it in (_items or [])]
