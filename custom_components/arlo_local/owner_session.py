"""Use the pinned pyaarlo backend without starting a second PyArlo integration."""

from __future__ import annotations

import imaplib
import inspect
import json
import re
import tempfile
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyaarlo
from pyaarlo.backend import ArloBackEnd
from pyaarlo.cfg import ArloCfg
from pyaarlo.constant import DEFAULT_HOST, DEVICES_PATH
from pyaarlo.security_utils import SecurityUtils
from pyaarlo.tfa import Arlo2FAConsole

if TYPE_CHECKING:
    from pyaarlo.backend import MFAHelper

from . import arlo_cert_poc as cert
from .const import PYARLO_VERSION
from .inventory import CameraInfo, Hub, parse_inventory
from .manual_mfa import ManualMFA
from .storage import (
    CredentialRecord,
    CredentialStore,
    atomic_json,
    parse_record,
    private_dir,
    serialize_inventory,
)


class AuthRequired(Exception):
    """Owner credentials or configured MFA need attention."""


class InventoryUnavailable(Exception):
    """Transient or invalid inventory; retain the last successful inventory."""


class IssuanceBlocked(Exception):
    """An issuance is unresolved; never repeat it automatically."""


def authentication_settings(config: Mapping[str, str]) -> dict[str, str]:
    """Compare only settings that affect the upstream authentication session."""
    keys = ["username", "password", "mfa"]
    if config.get("mfa") == "imap":
        keys.extend(("imap_host", "imap_username", "imap_password"))
    return {key: config[key] for key in keys if key in config}


class OwnerConfiguration:
    """Minimal pyaarlo configuration/log facade; no background workers."""

    def __init__(
        self, root: Path, config: Mapping[str, str], manual_mfa: ManualMFA | None = None
    ) -> None:
        self.root = root
        method = config["mfa"]
        self.manual_mfa = manual_mfa
        self.manual = method in ("manual_email", "manual_sms")
        self.authentication_rejected = False
        if method not in ("imap", "push", "manual_email", "manual_sms"):
            raise ValueError("Unsupported MFA method")
        options: dict[str, str | bool | int | float] = {
            "storage_dir": str(root),
            "username": config["username"],
            "password": config["password"],
            "save_session": False,
            "save_state": False,
            "verbose": False,
            "verbose_debug": False,
            "debug": False,
            "dump": False,
            "request_timeout": 15,
            "http_backend": "curl_cffi",
            "tfa_source": "console" if self.manual else method,
            "tfa_type": "sms"
            if method == "manual_sms"
            else "push"
            if method == "push"
            else "email",
            "tfa_timeout": 3,
            "tfa_total_timeout": 75,
        }
        if method == "imap":
            options.update(
                tfa_host=config["imap_host"],
                tfa_username=config["imap_username"],
                tfa_password=config["imap_password"],
            )
        self.cfg = ArloCfg(self, **options)

    # The upstream verbose logger includes authentication bodies and MFA values.
    # No pyaarlo log message from this owner-only facade is forwarded.
    def debug(self, message: str) -> None:
        pass

    def vdebug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        # Classify only the fixed pyaarlo 0.8.0.27 failure prefixes. Never
        # retain or forward the raw message, which can contain auth bodies.
        if message.startswith("login failed: 2fa:") or re.match(
            r"login failed: (?:finish failed: )?(?:401|403)(?:\D|$)", message
        ):
            self.authentication_rejected = True


class GUIConsoleMFA(Arlo2FAConsole):
    """Reuse pyaarlo's console contract without accessing terminal input."""

    def __init__(self, arlo: OwnerConfiguration) -> None:
        super().__init__(arlo)
        self.bridge = arlo.manual_mfa

    def get(self) -> str | None:
        return self.bridge.request_code() if self.bridge is not None else None

    def start(self) -> bool:
        """Do not request a new OTP if no interactive flow is present."""
        return self.bridge is not None and super().start()


class IsolatedBackend(ArloBackEnd):
    """No shared session pickle, event thread, or automatic cloud re-login."""

    def _load_session(self) -> None:
        super()._load_session()
        # Keep a distinct, persistent client identity without pyaarlo's shared
        # session dictionary or persisted auth token.
        path = self._arlo.root / "client_id.json"
        if path.exists():
            record: object = json.loads(path.read_text())
            if not isinstance(record, dict) or not isinstance(
                value := record.get("id"), str
            ):
                raise ValueError("Invalid owner client identity")
            self._user_device_id = str(uuid.UUID(value))
        else:
            self._user_device_id = str(uuid.uuid4())
            atomic_json(path, {"id": self._user_device_id})

    def _relogin(self) -> bool:
        return False

    def _get_tfa(self) -> MFAHelper | str:
        if isinstance(self._arlo, OwnerConfiguration) and self._arlo.manual:
            return GUIConsoleMFA(self._arlo)
        return super()._get_tfa()


class OwnerSession:
    """Executor-only owner operations; serialized per runtime."""

    def __init__(self, store: CredentialStore) -> None:
        self.store = store
        self.backend: IsolatedBackend | None = None
        self._lock = threading.RLock()
        self._config: dict[str, str] | None = None

    def _close_locked(self) -> None:
        backend, self.backend = self.backend, None
        self._config = None
        if backend is not None:
            session = backend._session
            if session is not None:
                session.close()

    def close(self) -> None:
        with self._lock:
            self._close_locked()

    def _login(
        self, config: Mapping[str, str], manual_mfa: ManualMFA | None = None
    ) -> IsolatedBackend:
        if (
            pyaarlo.__version__ != PYARLO_VERSION
            or "retry_401"
            not in inspect.signature(ArloBackEnd._request_tuple).parameters
        ):
            raise RuntimeError("Unsupported pyaarlo backend")
        self._close_locked()
        root = private_dir(self.store.root / "owner_auth", True)
        # Cookies may be saved by pyaarlo; protected by the parent directory.
        backend = IsolatedBackend.__new__(IsolatedBackend)
        facade = OwnerConfiguration(root, config, manual_mfa)
        try:
            IsolatedBackend.__init__(backend, facade)
        except (
            OSError,
            RuntimeError,
            ValueError,
            TypeError,
            KeyError,
            imaplib.IMAP4.error,
        ):
            session = getattr(backend, "_session", None)
            if session is not None:
                session.close()
            if facade.authentication_rejected:
                raise AuthRequired("Owner login/MFA unsuccessful") from None
            raise InventoryUnavailable(
                "Owner authentication temporarily unavailable"
            ) from None
        except BaseException:
            session = getattr(backend, "_session", None)
            if session is not None:
                session.close()
            raise
        if not backend.is_connected:
            session = backend._session
            if session is not None:
                session.close()
            if facade.authentication_rejected:
                raise AuthRequired("Owner login/MFA unsuccessful")
            raise InventoryUnavailable("Owner authentication temporarily unavailable")
        self.backend = backend
        self._config = dict(config)
        return backend

    def _read_inventory(
        self, backend: IsolatedBackend, hub_id: str | None
    ) -> tuple[Hub, dict[str, CameraInfo]]:
        status, body = backend._request_tuple(
            DEVICES_PATH, method="GET", raw=True, timeout=15, retry_401=False
        )
        meta = body.get("meta") if isinstance(body, dict) else None
        if (
            status == 401
            or isinstance(meta, dict)
            and (meta.get("code") in (401, "401") or meta.get("error") == 9204)
        ):
            raise AuthRequired("Owner session expired")
        if status != 200 or not isinstance(body, dict):
            raise InventoryUnavailable("Device inventory unavailable")
        success = (
            meta.get("code") in (200, "200")
            if isinstance(meta, dict)
            else body.get("success") is True
        )
        if not success:
            raise InventoryUnavailable("Device inventory rejected")
        owner_id = backend.user_id
        if not isinstance(owner_id, str) or not owner_id:
            raise AuthRequired("Owner identity unavailable")
        try:
            return parse_inventory(body.get("data"), owner_id, hub_id)
        except ValueError as exc:
            raise InventoryUnavailable("Owner or hub inventory mismatch") from exc

    def inventory(
        self,
        config: Mapping[str, str],
        hub_id: str | None = None,
        force_login: bool = False,
        manual_mfa: ManualMFA | None = None,
    ) -> tuple[Hub, dict[str, CameraInfo]]:
        with self._lock:
            config = authentication_settings(config)
            backend = self.backend
            new_login = (
                force_login
                or backend is None
                or config != self._config
                or not backend.is_connected
            )
            if new_login:
                backend = self._login(config, manual_mfa)
            assert backend is not None
            try:
                return self._read_inventory(backend, hub_id)
            except AuthRequired:
                if new_login:
                    raise
                return self._read_inventory(self._login(config, manual_mfa), hub_id)

    def provision(
        self,
        config: Mapping[str, str],
        hub_id: str | None = None,
        force_login: bool = False,
        manual_mfa: ManualMFA | None = None,
    ) -> CredentialRecord:
        with self._lock, self.store.exclusive():
            hub, cameras = self.inventory(config, hub_id, force_login, manual_mfa)
            current = self.store.current()
            if current and (
                current["hub"]["unique_id"] != hub.unique_id
                or current["hub"]["device_id"] != hub.device_id
            ):
                raise IssuanceBlocked("Configured hub identity changed")
            ledger = self.store.root / "issuance.json"
            if ledger.exists():
                state: object = json.loads(ledger.read_text())
                if not isinstance(state, dict):
                    raise IssuanceBlocked("Invalid issuance ledger")
                if state.get("status") == "candidate":
                    candidate = parse_record(state.get("candidate"))
                    if (
                        candidate["hub"]["unique_id"] != hub.unique_id
                        or candidate["hub"]["device_id"] != hub.device_id
                    ):
                        raise IssuanceBlocked("Retained candidate hub identity changed")
                    if (
                        current
                        and current["directory"] == candidate["directory"]
                        and current.get("validated_utc")
                    ):
                        # The active pointer is the commit point. Recover a
                        # crash or failed ledger write after activation.
                        state = {
                            "status": "committed",
                            "directory": current["directory"],
                        }
                        atomic_json(ledger, state)
                    else:
                        # Reuse an uncommitted candidate, not another issuance.
                        return candidate
                if state.get("status") != "committed":
                    raise IssuanceBlocked(
                        "Earlier certificate request requires reconciliation"
                    )
            scratch = Path(
                tempfile.mkdtemp(prefix="arlo-cert-poc-", dir=self.store.root)
            )
            security = SecurityUtils(str(scratch))
            Path(security.private_key_path).chmod(0o600)
            backend = self.backend
            assert backend is not None
            client = backend._user_device_id
            owner_id = backend.user_id
            if (
                not isinstance(client, str)
                or not isinstance(owner_id, str)
                or not owner_id
                or str(uuid.UUID(client)) != client.lower()
                or client == hub.device_id
            ):
                raise IssuanceBlocked("Invalid distinct owner client ID")
            params = cert.request_params(security, hub, client)
            headers = {
                "Origin": DEFAULT_HOST,
                "Referer": DEFAULT_HOST + "/",
                "x-user-device-id": client,
                "x-forwarded-user": owner_id,
            }
            atomic_json(
                ledger,
                {
                    "status": "request_pending",
                    "directory": scratch.name,
                    "utc": datetime.now(UTC).isoformat(),
                },
            )
            try:
                data = cert.request_once(
                    backend, params, headers, issuance_authorized=True
                )
                report = cert.validate_and_save(security, hub.unique_id, data)
                if report["status"] != "locally_usable_candidate":
                    raise IssuanceBlocked("Invalid certificate set")
                inventory = serialize_inventory(hub, cameras)
                record: CredentialRecord = {
                    "directory": scratch.name,
                    "hub": inventory["hub"],
                    "cameras": inventory["cameras"],
                }
                atomic_json(ledger, {"status": "candidate", "candidate": record})
                return record
            except BaseException as exc:
                failure: dict[str, object] = {
                    "status": "failed_or_uncertain",
                    "directory": scratch.name,
                    "error_type": type(exc).__name__,
                }
                if isinstance(exc, cert.CertificateRequestError):
                    failure["diagnostic"] = exc.diagnostic
                atomic_json(ledger, failure)
                if isinstance(
                    exc, (OSError, RuntimeError, ValueError, TypeError, KeyError)
                ):
                    raise IssuanceBlocked("Certificate issuance unsuccessful") from None
                raise
