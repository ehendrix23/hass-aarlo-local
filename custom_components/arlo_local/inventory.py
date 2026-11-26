"""Owner-verified inventory; deliberately independent of camera capability flags."""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field


def safe_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value)
        or value in (".", "..")
    ):
        raise ValueError("Invalid Arlo identifier")
    return value


@dataclass(frozen=True, slots=True)
class Hub:
    device_id: str
    unique_id: str
    name: str
    model: str


@dataclass(frozen=True, slots=True)
class CameraInfo:
    device_id: str
    unique_id: str
    name: str
    model: str
    # Observation from the owner inventory only; not part of device identity
    # or the persisted credential/inventory schema.
    interface_version: str | None = field(default=None, compare=False)


@dataclass(frozen=True, slots=True)
class Delta:
    added: set[str]
    removed: set[str]
    changed: set[str]


def parse_inventory(
    devices: object, owner_id: str, hub_id: str | None = None
) -> tuple[Hub, dict[str, CameraInfo]]:
    if not isinstance(devices, list) or not isinstance(owner_id, str) or not owner_id:
        raise ValueError("Unverified device inventory")

    def owned(d: Mapping[str, object]) -> bool:
        owner = d.get("owner")
        return isinstance(owner, dict) and owner.get("ownerId") == owner_id

    hubs = [
        d
        for d in devices
        if isinstance(d, dict)
        and d.get("modelId") == "VMB5000"
        and (hub_id is None or d.get("deviceId") == hub_id)
    ]
    if len(hubs) != 1 or not owned(hubs[0]):
        raise ValueError("Expected exactly one owned VMB5000")
    h = hubs[0]
    hub = Hub(
        safe_id(h.get("deviceId")),
        safe_id(h.get("uniqueId")),
        str(h.get("deviceName") or "SmartHub"),
        "VMB5000",
    )
    cameras: dict[str, CameraInfo] = {}
    uids: set[str] = set()
    for d in devices:
        if (
            not isinstance(d, dict)
            or d.get("parentId") != hub.device_id
            or d.get("deviceType") != "camera"
        ):
            continue
        if not owned(d):
            raise ValueError("Camera ownership mismatch")
        dev = safe_id(d.get("deviceId"))
        uid = safe_id(d.get("uniqueId"))
        if dev in cameras or uid in uids:
            raise ValueError("Duplicate camera identity")
        version = d.get("interfaceVersion")
        cameras[dev] = CameraInfo(
            dev,
            uid,
            str(d.get("deviceName") or dev),
            str(d.get("modelId") or "Arlo"),
            version
            if isinstance(version, str) and re.fullmatch(r"i[0-9]{3,8}", version)
            else None,
        )
        uids.add(uid)
    return hub, cameras


def reconcile(old: Mapping[str, CameraInfo], new: Mapping[str, CameraInfo]) -> Delta:
    return Delta(
        set(new) - set(old),
        set(old) - set(new),
        {k for k in old.keys() & new.keys() if old[k] != new[k]},
    )
