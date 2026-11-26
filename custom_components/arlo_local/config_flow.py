"""Native owner setup and reauthentication; no secret values in diagnostics."""

import asyncio
import hashlib
import ipaddress
import re
from collections.abc import Callable, Mapping
from pathlib import Path

import probatio
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.core import callback
from homeassistant.helpers import selector
from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo

from .budget import CONF_MAX_STREAMS, MAX_STREAMS, stream_budget
from .const import DOMAIN, PORT
from .inventory import safe_id
from .manual_mfa import ManualMFA
from .owner_session import (
    AuthRequired,
    InventoryUnavailable,
    IssuanceBlocked,
    OwnerSession,
)
from .rtsp_transport import probe_tls
from .runtime import Runtime
from .storage import CredentialStore

PASSWORD = selector.TextSelector(
    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
)


def validate_endpoint(
    host: object, hostname: object, hub_id: object
) -> tuple[str, str, str]:
    if not isinstance(host, str) or not isinstance(hostname, str):
        raise TypeError("Invalid SmartHub endpoint")
    address = ipaddress.ip_address(host)
    if (
        address.version != 4
        or not address.is_private
        or address.is_loopback
        or address.is_unspecified
    ):
        raise ValueError("Expected private IPv4 SmartHub address")
    hostname = hostname.rstrip(".")
    if not re.fullmatch(r"[A-Za-z0-9.-]+", hostname):
        raise ValueError("Invalid SmartHub hostname")
    return str(address), hostname, safe_id(hub_id)


def credential_config(values: Mapping[str, object]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key in ("username", "password", "mfa"):
        value = values.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError("Missing credentials")
        result[key] = value
    if (
        not result["username"]
        or not result["password"]
        or result["mfa"] not in ("imap", "push", "manual_email", "manual_sms")
    ):
        raise ValueError("Missing credentials")
    if result["mfa"] == "imap":
        for k in ("imap_host", "imap_username", "imap_password"):
            value = values.get(k)
            if not isinstance(value, str) or not value:
                raise ValueError("Missing IMAP configuration")
            result[k] = value
    return result


class ArloLocalConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return ArloLocalOptionsFlow()

    def __init__(self) -> None:
        self.endpoint: dict[str, str] = {}
        self._config: dict[str, str] = {}
        self._job: asyncio.Task[dict[str, str]] | None = None
        self._manual_mfa: ManualMFA | None = None
        self._progress: asyncio.Task[None] | None = None
        self._reauth: ConfigEntry[Runtime] | None = None
        self._reconfigure: ConfigEntry[Runtime] | None = None
        self._error: str | None = None

    async def async_step_zeroconf(
        self, discovery_info: ZeroconfServiceInfo
    ) -> ConfigFlowResult:
        try:
            if discovery_info.port != PORT:
                raise ValueError
            props = {str(k).lower(): v for k, v in discovery_info.properties.items()}
            host, hostname, hub_id = validate_endpoint(
                discovery_info.host, discovery_info.hostname, props.get("deviceid")
            )
        except (ValueError, TypeError, AttributeError):
            return self.async_abort(reason="invalid_discovery")
        self.endpoint = {
            "host": host,
            "hostname": hostname,
            "hub_id": hub_id,
            "service_name": discovery_info.name,
        }
        await self.async_set_unique_id(hub_id)
        self._abort_if_unique_id_configured(
            updates=self.endpoint, reload_on_update=False
        )
        self.context["title_placeholders"] = {"name": hostname}
        return await self.async_step_user()

    def _schema(self) -> probatio.Schema:
        fields: dict[probatio.Marker, object] = {}
        if self._reauth is None:
            fields[probatio.Required("host", default=self.endpoint.get("host", ""))] = (
                str
            )
            fields[
                probatio.Required(
                    "hostname", default=self.endpoint.get("hostname", "VMB5000.local")
                )
            ] = str
            if self._reconfigure is None:
                fields[
                    probatio.Required("hub_id", default=self.endpoint.get("hub_id", ""))
                ] = str
        existing = self._reauth or self._reconfigure
        old = existing.data if existing else {}
        fields[
            probatio.Required(
                "username",
                default=self._config.get("username", old.get("username", "")),
            )
        ] = str
        fields[probatio.Required(probatio.Secret("password"))] = PASSWORD
        fields[
            probatio.Required(
                "mfa", default=self._config.get("mfa", old.get("mfa", "imap"))
            )
        ] = probatio.In(["imap", "push", "manual_email", "manual_sms"])
        fields[
            probatio.Optional(
                "imap_host",
                default=self._config.get(
                    "imap_host", old.get("imap_host", "imap.mail.me.com")
                ),
            )
        ] = str
        fields[
            probatio.Optional(
                "imap_username",
                default=self._config.get("imap_username", old.get("imap_username", "")),
            )
        ] = str
        fields[probatio.Optional(probatio.Secret("imap_password"))] = PASSWORD
        return probatio.Schema(fields)

    async def async_step_user(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        errors = {}
        if self._error:
            errors["base"] = self._error
            self._error = None
        if user_input is not None:
            try:
                config = credential_config(user_input)
                if self._reauth is None:
                    host, hostname, hub_id = validate_endpoint(
                        user_input["host"],
                        user_input["hostname"],
                        self.endpoint["hub_id"]
                        if self._reconfigure
                        else user_input["hub_id"],
                    )
                    if (
                        self.endpoint.get("hub_id")
                        and self.endpoint["hub_id"] != hub_id
                    ):
                        raise ValueError("Discovery identity changed")
                    self.endpoint.update(host=host, hostname=hostname, hub_id=hub_id)
                    await self.async_set_unique_id(hub_id)
                    if self._reconfigure is not None:
                        self._abort_if_unique_id_mismatch()
                    else:
                        self._abort_if_unique_id_configured()
                self._config = config
                if config["mfa"] in ("manual_email", "manual_sms"):
                    self._manual_mfa = ManualMFA()
                return await self.async_step_authenticate()
            except (ValueError, KeyError, TypeError):
                errors["base"] = "invalid_config"
        return self.async_show_form(
            step_id="user", data_schema=self._schema(), errors=errors
        )

    async def async_step_reauth(
        self, entry_data: Mapping[str, object]
    ) -> ConfigFlowResult:
        self._reauth = self._get_reauth_entry()
        self.endpoint = {
            k: self._reauth.data[k] for k in ("host", "hostname", "hub_id")
        }
        return await self.async_step_user()

    async def async_step_reconfigure(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        """Allow changing connection and authentication settings for the same hub."""
        self._reconfigure = self._get_reconfigure_entry()
        self.endpoint = {
            k: self._reconfigure.data[k] for k in ("host", "hostname", "hub_id")
        }
        return await self.async_step_user()

    async def _executor[T, *Ts](self, func: Callable[[*Ts], T], *args: *Ts) -> T:
        task = asyncio.ensure_future(self.hass.async_add_executor_job(func, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _authenticate(self) -> dict[str, str]:
        if self._reconfigure is not None:
            runtime = getattr(self._reconfigure, "runtime_data", None)
            if isinstance(runtime, Runtime):
                args = (self._config, self.endpoint["host"], self.endpoint["hostname"])
                if self._manual_mfa is not None:
                    await runtime.validate_reconfigure(
                        *args, manual_mfa=self._manual_mfa
                    )
                else:
                    await runtime.validate_reconfigure(*args)
            else:
                # An unloaded entry must still validate the same cached hub,
                # without issuing another certificate during reconfiguration.
                root = Path(
                    self.hass.config.path(
                        ".arlo_local", self._reconfigure.data["storage_id"]
                    )
                )
                store = await self._executor(CredentialStore, root)
                owner = OwnerSession(store)
                try:
                    record = await self._executor(store.current)
                    if record is None:
                        raise IssuanceBlocked("No existing credential generation")
                    hub, _ = await self._executor(
                        owner.inventory,
                        self._config,
                        self.endpoint["hub_id"],
                        True,
                        self._manual_mfa,
                    )
                    if hub.unique_id != record["hub"]["unique_id"]:
                        raise IssuanceBlocked("Configured hub identity changed")
                    context, pin = await self._executor(store.context, record)
                    await probe_tls(
                        self.endpoint["host"],
                        PORT,
                        self.endpoint["hostname"],
                        context,
                        pin,
                    )
                finally:
                    await self._executor(owner.close)
            return {}
        if (
            self._reauth is not None
            and getattr(self._reauth, "runtime_data", None) is not None
        ):
            runtime = self._reauth.runtime_data
            config = {**self._reauth.data, **self._config}
            if self._manual_mfa is not None:
                success = await runtime.refresh(
                    force_login=True, config=config, manual_mfa=self._manual_mfa
                )
            else:
                success = await runtime.refresh(force_login=True, config=config)
            if not success:
                raise AuthRequired("Owner reauthentication failed")
            return {}
        hub_id = self.endpoint["hub_id"]
        storage_id = (
            self._reauth.data["storage_id"]
            if self._reauth is not None
            else hashlib.sha256(hub_id.encode()).hexdigest()
        )
        root = Path(self.hass.config.path(".arlo_local", storage_id))
        store = await self._executor(CredentialStore, root)
        owner = OwnerSession(store)
        try:
            hub, cameras = await self._executor(
                owner.inventory, self._config, hub_id, True, self._manual_mfa
            )
            current = await self._executor(store.current)
            if current and (
                current["hub"]["unique_id"] != hub.unique_id
                or current["hub"]["device_id"] != hub.device_id
            ):
                raise IssuanceBlocked("Cached hub identity mismatch")
            reuse = (
                current is not None
                and (await self._executor(store.inspect, current))["status"]
                == "locally_usable_candidate"
            )
            if reuse:
                assert current is not None
                candidate = current
            else:
                candidate = await self._executor(
                    owner.provision, self._config, hub_id, False, self._manual_mfa
                )
            context, pin = await self._executor(store.context, candidate)
            for _ in range(2):
                await probe_tls(
                    self.endpoint["host"], PORT, self.endpoint["hostname"], context, pin
                )
            if reuse:
                await self._executor(store.update_inventory, hub, cameras)
            else:
                await self._executor(store.activate, candidate)
            return {"storage_id": storage_id, "title": hub.name}
        finally:
            await self._executor(owner.close)

    async def async_step_authenticate(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        if self._job is None:
            self._job = self.hass.async_create_task(
                self._authenticate(), "Arlo Local owner authentication"
            )
        if not self._job.done():
            if self._manual_mfa is not None:
                if self._manual_mfa.pending:
                    self._progress = None
                    return self.async_show_progress_done(next_step_id="mfa")
                if self._progress is None or self._progress.done():
                    self._progress = self.hass.async_create_task(
                        self._manual_mfa.wait_for_challenge_or_completion(self._job),
                        "Arlo Local manual MFA challenge",
                    )
                return self.async_show_progress(
                    step_id="authenticate",
                    progress_action="authenticate",
                    progress_task=self._progress,
                )
            return self.async_show_progress(
                step_id="authenticate",
                progress_action="authenticate",
                progress_task=self._job,
            )
        return self.async_show_progress_done(next_step_id="finish")

    async def async_step_mfa(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        """Accept a one-time code only while the owner session requests it."""
        bridge = self._manual_mfa
        if bridge is None or not bridge.pending:
            return await self.async_step_authenticate()
        errors: dict[str, str] = {}
        if user_input is not None:
            code = user_input.get("code")
            if isinstance(code, str) and bridge.submit(code):
                self._progress = None
                return await self.async_step_authenticate()
            errors["base"] = "invalid_mfa"
        return self.async_show_form(
            step_id="mfa",
            data_schema=probatio.Schema(
                {probatio.Required(probatio.Secret("code")): PASSWORD}
            ),
            errors=errors,
        )

    @callback
    def async_remove(self) -> None:
        """Release a waiting auth worker if the user closes the flow."""
        if self._manual_mfa is not None:
            self._manual_mfa.close()
        if self._progress is not None and not self._progress.done():
            self._progress.cancel()
        if self._job is not None and not self._job.done():
            self._job.cancel()

    async def async_step_finish(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        assert self._job is not None
        if self._manual_mfa is not None:
            self._manual_mfa.close()
            self._manual_mfa = None
        self._progress = None
        try:
            result = self._job.result()
        except AuthRequired:
            self._error = "invalid_auth"
        except IssuanceBlocked:
            self._error = "issuance_unresolved"
        except InventoryUnavailable:
            self._error = "inventory_unavailable"
        except (OSError, RuntimeError, TypeError, ValueError, KeyError):
            self._error = "cannot_connect"
        else:
            self._job = None
            if self._reconfigure is not None:
                data = {**self._reconfigure.data, **self.endpoint, **self._config}
                if self._config["mfa"] != "imap":
                    for key in ("imap_host", "imap_username", "imap_password"):
                        data.pop(key, None)
                return self.async_update_reload_and_abort(
                    self._reconfigure, data=data, reason="reconfigure_successful"
                )
            if self._reauth is not None:
                data = {**self._reauth.data, **self._config}
                if self._config["mfa"] != "imap":
                    for k in ("imap_host", "imap_username", "imap_password"):
                        data.pop(k, None)
                if getattr(self._reauth, "runtime_data", None) is None:
                    return self.async_update_reload_and_abort(
                        self._reauth, data=data, reason="reauth_successful"
                    )
                self.hass.config_entries.async_update_entry(self._reauth, data=data)
                return self.async_abort(reason="reauth_successful")
            return self.async_create_entry(
                title="Arlo Local " + result["title"],
                data={
                    **self.endpoint,
                    **self._config,
                    "storage_id": result["storage_id"],
                },
            )
        self._job = None
        return await self.async_step_user()


class ArloLocalOptionsFlow(OptionsFlow):
    """Local settings only; the existing config-entry listener applies them."""

    async def async_step_init(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            limit = stream_budget(user_input)
            return self.async_create_entry(
                title="", data={**self.config_entry.options, CONF_MAX_STREAMS: limit}
            )
        return self.async_show_form(
            step_id="init",
            data_schema=probatio.Schema(
                {
                    probatio.Required(
                        CONF_MAX_STREAMS,
                        default=stream_budget(self.config_entry.options),
                    ): probatio.In(list(range(1, MAX_STREAMS + 1)))
                }
            ),
        )
