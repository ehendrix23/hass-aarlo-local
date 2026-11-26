"""Atomic, owner-only storage for locally verified credential generations."""

import fcntl
import json
import logging
import os
import ssl
import stat
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import NotRequired, TypedDict

from . import arlo_cert_poc as cert
from .inventory import CameraInfo, Hub, safe_id
from .rtsp_transport import tls_context

_LOGGER = logging.getLogger(__name__)


class DeviceRecord(TypedDict):
    device_id: str
    unique_id: str
    name: str
    model: str


class InventoryRecord(TypedDict):
    hub: DeviceRecord
    cameras: list[DeviceRecord]


class CredentialRecord(InventoryRecord):
    directory: str
    validated_utc: NotRequired[str]


def _device_record(value: object) -> DeviceRecord:
    if not isinstance(value, dict):
        raise TypeError("Invalid cached device")
    name, model = value.get("name"), value.get("model")
    if not isinstance(name, str) or not isinstance(model, str):
        raise TypeError("Invalid cached device metadata")
    return {
        "device_id": safe_id(value.get("device_id")),
        "unique_id": safe_id(value.get("unique_id")),
        "name": name,
        "model": model,
    }


def parse_record(value: object) -> CredentialRecord:
    if not isinstance(value, dict):
        raise TypeError("Invalid credential record")
    directory = value.get("directory")
    cameras = value.get("cameras")
    if not isinstance(directory, str) or not isinstance(cameras, list):
        raise TypeError("Invalid credential inventory")
    record: CredentialRecord = {
        "directory": directory,
        "hub": _device_record(value.get("hub")),
        "cameras": [_device_record(cam) for cam in cameras],
    }
    if "validated_utc" in value:
        timestamp = value["validated_utc"]
        if not isinstance(timestamp, str):
            raise ValueError("Invalid validation timestamp")
        if datetime.fromisoformat(timestamp).tzinfo is None:
            raise ValueError("Validation timestamp must be timezone-aware")
        record["validated_utc"] = timestamp
    return record


def private_dir(path: Path | str, create: bool = False) -> Path:
    path = Path(path)
    if create:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = path.stat()
    if (
        path.is_symlink()
        or not path.is_dir()
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("Unsafe Arlo Local storage directory")
    return path


def atomic_json(path: Path, data: Mapping[str, object]) -> None:
    fd, name = tempfile.mkstemp(prefix=".record-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def serialize_inventory(hub: Hub, cameras: Mapping[str, CameraInfo]) -> InventoryRecord:
    def device(value: Hub | CameraInfo) -> DeviceRecord:
        return {
            "device_id": value.device_id,
            "unique_id": value.unique_id,
            "name": value.name,
            "model": value.model,
        }

    return {"hub": device(hub), "cameras": [device(c) for c in cameras.values()]}


def deserialize_inventory(record: InventoryRecord) -> tuple[Hub, dict[str, CameraInfo]]:
    hub = Hub(**record["hub"])
    safe_id(hub.device_id)
    safe_id(hub.unique_id)
    cameras: dict[str, CameraInfo] = {}
    uids: set[str] = set()
    for value in record["cameras"]:
        cam = CameraInfo(**value)
        safe_id(cam.device_id)
        safe_id(cam.unique_id)
        if cam.device_id in cameras or cam.unique_id in uids:
            raise ValueError("Duplicate cached identity")
        cameras[cam.device_id] = cam
        uids.add(cam.unique_id)
    return hub, cameras


class CredentialStore:
    def __init__(self, root: Path | str) -> None:
        self.root = private_dir(root, True)

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        """Serialize credential transactions across runtimes and config flows."""
        fd = os.open(
            self.root / ".transaction.lock",
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
            0o600,
        )
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise ValueError("Unsafe credential transaction lock")
            # Never queue another issuance behind a possibly unresolved one.
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def current(self) -> CredentialRecord | None:
        p = self.root / "active.json"
        if p.is_symlink():
            raise ValueError("Invalid credential pointer")
        if not p.is_file():
            return None
        return parse_record(json.loads(p.read_text()))

    def candidate(self, record: CredentialRecord) -> tuple[Path, Hub]:
        name = record["directory"]
        if (
            not isinstance(name, str)
            or not name.startswith("arlo-cert-poc-")
            or Path(name).name != name
        ):
            raise ValueError("Invalid credential generation")
        path = private_dir(self.root / name)
        hub, _ = deserialize_inventory(record)
        return path, hub

    def inspect(self, record: CredentialRecord) -> cert.CacheReport:
        path, hub = self.candidate(record)
        return cert.inspect_cache(path, hub.unique_id)

    def context(self, record: CredentialRecord) -> tuple[ssl.SSLContext, bytes]:
        path, hub = self.candidate(record)
        if self.inspect(record)["status"] != "locally_usable_candidate":
            raise ValueError("Invalid or expired credential")
        base = path / "certs"
        d = base / hub.unique_id
        return tls_context(
            peer_chain=d / "combined.crt",
            private_key=base / "private.pem",
            device_cert=d / "device.crt",
            ica_cert=base / "ica.crt",
        )

    def activate(self, record: CredentialRecord) -> CredentialRecord:
        with self.exclusive():
            return self._activate_locked(record)

    def _activate_locked(self, record: CredentialRecord) -> CredentialRecord:
        # Only the caller that has completed the TLS probe may activate.
        self.context(record)
        record = record.copy()
        record["validated_utc"] = datetime.now(UTC).isoformat()
        atomic_json(self.root / "active.json", record)
        # active.json is the commit point. Failure of the secondary ledger
        # update must not leave the caller using a different generation than
        # the authoritative pointer. Provisioning reconciles a matching
        # candidate record against this validated active pointer.
        try:
            atomic_json(
                self.root / "issuance.json",
                {
                    "status": "committed",
                    "directory": record["directory"],
                    "utc": record["validated_utc"],
                },
            )
        except OSError:
            _LOGGER.warning(
                "Arlo Local credential committed; issuance ledger update requires reconciliation"
            )
        return record

    def update_inventory(
        self, hub: Hub, cameras: Mapping[str, CameraInfo]
    ) -> CredentialRecord:
        with self.exclusive():
            return self._update_inventory_locked(hub, cameras)

    def _update_inventory_locked(
        self, hub: Hub, cameras: Mapping[str, CameraInfo]
    ) -> CredentialRecord:
        current = self.current()
        if (
            current is None
            or current["hub"]["unique_id"] != hub.unique_id
            or current["hub"]["device_id"] != hub.device_id
        ):
            raise ValueError("Hub identity changed")
        updated = serialize_inventory(hub, cameras)
        current["hub"] = updated["hub"]
        current["cameras"] = updated["cameras"]
        atomic_json(self.root / "active.json", current)
        return current
