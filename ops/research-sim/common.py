import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import os
from tempfile import NamedTemporaryFile


def canonical(data):
    return json.dumps(
        data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def digest(data):
    return hashlib.sha256(canonical(data)).hexdigest()


def moment(value):
    if not value:
        return None
    if isinstance(value, (float, int)):
        return datetime.fromtimestamp(value, timezone.utc)
    d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


def age(value, at):
    when = moment(value)
    return (moment(at) - when).total_seconds() if when else None


def fresh(value, at, seconds):
    delta = age(value, at)
    return delta is not None and 0 <= delta <= seconds


def atomic(path, data, mode=0o640):
    path = Path(path)
    with NamedTemporaryFile(dir=path.parent, delete=False) as f:
        payload = canonical(data)
        f.write(gzip.compress(payload, compresslevel=5, mtime=0) if path.suffix == ".gz" else payload)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(f.name, mode)
    os.replace(f.name, path)


def load(path):
    path = Path(path)
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == ".gz" else raw)
