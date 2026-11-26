"""Isolated Arlo certificate-provisioning POC; never import into production automatically.

No login, logout, storage-port management, camera commands or automatic renewal.
Only prepare/provision in a newly created private scratch directory.
The caller must resolve the UUID semantics and authorize a real issuance separately.
"""

import base64
import inspect
import re
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import NotRequired, Protocol, TypedDict

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.types import PublicKeyTypes
from pyaarlo.backend import ArloBackEnd
from pyaarlo.constant import CREATE_DEVICE_CERTS_PATH
from pyaarlo.security_utils import SecurityUtils


class HubIdentity(Protocol):
    @property
    def unique_id(self) -> str: ...


class RoutedHub(HubIdentity, Protocol):
    @property
    def xcloud_id(self) -> str: ...


class Validity(TypedDict):
    not_before: str
    not_after: str
    current: bool


class CacheReport(TypedDict):
    present: dict[str, bool]
    status: str
    key_matches: NotRequired[bool]
    validity: NotRequired[dict[str, Validity]]


def _hub_component(value: object) -> str:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]+", value)
        or value in (".", "..")
    ):
        raise ValueError("Invalid hub unique ID")
    return value


def _public_der(key: PublicKeyTypes) -> bytes:
    return key.public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )


def _dates(cert: x509.Certificate) -> tuple[datetime, datetime]:
    return cert.not_valid_before_utc, cert.not_valid_after_utc


def _current(cert: x509.Certificate) -> bool:
    start, end = _dates(cert)
    return start <= datetime.now(UTC) < end


def inspect_cache(storage_dir: Path | str, unique_id: str) -> CacheReport:
    """Read existing cache only; return metadata, never key/certificate contents."""
    root = Path(storage_dir) / "certs"
    hub = root / _hub_component(unique_id)
    paths = {
        "private": root / "private.pem",
        "public": root / "public.pem",
        "peer": hub / "peer.crt",
        "device": hub / "device.crt",
        "ica": root / "ica.crt",
    }
    report: CacheReport = {
        "present": {name: p.is_file() for name, p in paths.items()},
        "status": "missing",
    }
    if not all(report["present"].values()):
        return report
    try:
        key = serialization.load_pem_private_key(
            paths["private"].read_bytes(), password=None
        )
        public = serialization.load_pem_public_key(paths["public"].read_bytes())
        certs = {
            name: x509.load_pem_x509_certificate(paths[name].read_bytes())
            for name in ("peer", "device", "ica")
        }
        report["key_matches"] = (
            _public_der(key.public_key())
            == _public_der(public)
            == _public_der(certs["peer"].public_key())
        )
        report["validity"] = {
            name: {
                "not_before": _dates(c)[0].isoformat(),
                "not_after": _dates(c)[1].isoformat(),
                "current": _current(c),
            }
            for name, c in certs.items()
        }
        report["status"] = (
            "locally_usable_candidate"
            if (report["key_matches"] and all(_current(c) for c in certs.values()))
            else "invalid"
        )
        return report
    except (ValueError, OSError, TypeError):
        report["status"] = "invalid"
        return report


def prepare(
    scratch_parent: Path, hub: RoutedHub, *, request_uuid: str
) -> tuple[Path, SecurityUtils, dict[str, object], dict[str, str]]:
    """Generate keys using actual pyaarlo. Never use production's storage_dir."""
    _hub_component(hub.unique_id)
    if not request_uuid or not hub.xcloud_id:
        raise ValueError("Explicit request identity and hub routing are required")
    root = Path(tempfile.mkdtemp(prefix="arlo-cert-poc-", dir=scratch_parent))
    # mkdtemp is mode 0700; no global umask change in Home Assistant.
    root.chmod(0o700)
    security = SecurityUtils(str(root))
    Path(security.private_key_path).chmod(0o600)
    return (
        root,
        security,
        request_params(security, hub, request_uuid),
        {"xcloudId": hub.xcloud_id},
    )


def request_params(
    security: SecurityUtils, hub: HubIdentity, request_uuid: str
) -> dict[str, object]:
    """Reuse the exact pyaarlo-generated key and validate key consistency."""
    uid = _hub_component(hub.unique_id)
    if not request_uuid:
        raise ValueError("Missing request identity")
    public = serialization.load_pem_public_key(security.public_key.encode())
    key = serialization.load_pem_private_key(
        security.private_key.encode(), password=None
    )
    if _public_der(public) != _public_der(key.public_key()):
        raise ValueError("Key pair mismatch")
    return {
        "uuid": request_uuid,
        "uniqueIds": [uid],
        "publicKey": base64.b64encode(_public_der(public)).decode("ascii"),
    }


class CertificateRequestError(RuntimeError):
    """Request failure with allowlisted, non-secret diagnostic metadata."""

    def __init__(self, diagnostic: dict[str, str | int | bool | None]) -> None:
        super().__init__(
            "Certificate request failed or response not recognized; no retry"
        )
        self.diagnostic = diagnostic


def _numeric_code(value: object) -> int | None:
    # Arbitrary message strings, headers, credentials and IDs are not diagnostics.
    if type(value) is int and 0 <= value <= 999999:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,6}", value):
        return int(value)
    return None


def request_diagnostic(status: int, body: object) -> dict[str, str | int | bool | None]:
    meta = body.get("meta") if isinstance(body, dict) else None
    data = body.get("data") if isinstance(body, dict) else None
    success = body.get("success") if isinstance(body, dict) else None
    return {
        "backend_status": _numeric_code(status),
        "body_kind": "object"
        if isinstance(body, dict)
        else "absent"
        if body is None
        else "other",
        "api_meta_code": _numeric_code(meta.get("code"))
        if isinstance(meta, dict)
        else None,
        "api_meta_error": _numeric_code(meta.get("error"))
        if isinstance(meta, dict)
        else None,
        "api_success": success if type(success) is bool else None,
        "data_object": isinstance(data, dict),
    }


def request_once(
    backend: ArloBackEnd,
    params: Mapping[str, object],
    headers: Mapping[str, str],
    *,
    issuance_authorized: bool = False,
) -> Mapping[str, object]:
    """Use pyaarlo request lock/session without reauth or automatic retry."""
    if not issuance_authorized:
        raise PermissionError("Explicit issuance safety gate is closed")
    call = backend._request_tuple
    if "retry_401" not in inspect.signature(call).parameters:
        raise RuntimeError("Unsupported pyaarlo backend; fail closed")
    status, body = call(
        CREATE_DEVICE_CERTS_PATH,
        method="POST",
        params=dict(params),
        headers=dict(headers),
        raw=True,
        timeout=10,
        retry_401=False,
    )
    if status != 200 or not isinstance(body, dict):
        raise CertificateRequestError(request_diagnostic(status, body))
    success = body.get("success") is True
    if isinstance(body.get("meta"), dict):
        success = body["meta"].get("code") in (200, "200")
    data = body.get("data")
    if not success or not isinstance(data, dict):
        raise CertificateRequestError(request_diagnostic(status, body))
    return data


def validate_and_save(
    security: SecurityUtils, hub_unique_id: str, data: Mapping[str, object]
) -> CacheReport:
    """Use pyaarlo persistence only after checking response/key/date consistency."""
    uid = _hub_component(hub_unique_id)
    entries = data.get("certsData")
    if not isinstance(entries, list) or len(entries) != 1:
        raise ValueError("Expected exactly one hub certificate set")

    def cert(value: object) -> x509.Certificate:
        if not isinstance(value, str):
            raise TypeError("Invalid certificate encoding")
        return x509.load_der_x509_certificate(base64.b64decode(value, validate=True))

    if not isinstance(entries[0], dict):
        raise TypeError("Invalid hub certificate set")
    peer, device, ica = (
        cert(entries[0]["peerCert"]),
        cert(entries[0]["deviceCert"]),
        cert(data["icaCert"]),
    )
    key = serialization.load_pem_private_key(
        security.private_key.encode(), password=None
    )
    if _public_der(peer.public_key()) != _public_der(key.public_key()):
        raise ValueError("Returned client certificate does not match request key")
    if not all(_current(c) for c in (peer, device, ica)):
        raise ValueError("Returned certificate is not currently valid")
    # This is an isolated, single-issuance scratch directory. Never overwrite.
    hub_path = Path(security.device_certs_path(uid))
    if hub_path.exists() or (Path(security.certs_path) / "ica.crt").exists():
        raise FileExistsError("Refusing to overwrite an existing certificate set")
    security.save_device_certs(uid, data)
    return inspect_cache(Path(security.certs_path).parent, uid)
