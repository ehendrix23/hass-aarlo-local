"""Native owner setup and reauthentication; no secret values in diagnostics."""

import asyncio
import hashlib
import ipaddress
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

import probatio
from homeassistant.components import zeroconf as ha_zeroconf
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowContext,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import selector
from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo
from zeroconf import ServiceStateChange, Zeroconf
from zeroconf import asyncio as zc_asyncio

from .budget import CONF_MAX_STREAMS, MAX_STREAMS, stream_budget
from .const import DOMAIN, PORT, SERVICE_TYPE
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
DISCOVERY_WINDOW = 2.0
DISCOVERY_TIMEOUT_MS = 3000
MANUAL_HUB = "manual:"


class _ArloFlowContext(ConfigFlowContext, total=False):
    """Transient ownership marker; never persisted as entry data."""

    setup_started: bool


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


def discovered_endpoint(info: ZeroconfServiceInfo) -> dict[str, str]:
    """Validate one mDNS advertisement without granting it authentication authority."""
    if info.type != SERVICE_TYPE or info.port != PORT:
        raise ValueError("Invalid SmartHub service")
    props = {str(k).lower(): v for k, v in info.properties.items()}
    # HA prefers the most recently updated address, which may be IPv6.
    # The existing transport requires private IPv4; try the advertised alternatives.
    for address in dict.fromkeys([info.host, *info.addresses]):
        try:
            host, hostname, hub_id = validate_endpoint(
                address, info.hostname, props.get("deviceid")
            )
        except (ValueError, TypeError):
            continue
        return {
            "host": host,
            "hostname": hostname,
            "hub_id": hub_id,
            "service_name": info.name,
        }
    raise ValueError("No usable SmartHub endpoint")


async def discover_hubs(hass: HomeAssistant) -> dict[str, dict[str, str]]:
    """Browse only the Arlo mDNS service using HA's shared Zeroconf instance."""
    names: set[str] = set()

    def changed(
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        if state_change is ServiceStateChange.Removed:
            names.discard(name)
        else:
            names.add(name)

    try:
        async with asyncio.timeout(DISCOVERY_WINDOW + 4):
            zc = await ha_zeroconf.async_get_async_instance(hass)
            browser = zc_asyncio.AsyncServiceBrowser(
                zc.zeroconf, SERVICE_TYPE, handlers=[changed]
            )
            try:
                await asyncio.sleep(DISCOVERY_WINDOW)
                services = await asyncio.gather(
                    *(
                        zc.async_get_service_info(
                            SERVICE_TYPE, name, DISCOVERY_TIMEOUT_MS
                        )
                        for name in sorted(names)
                    ),
                    return_exceptions=True,
                )
            finally:
                await browser.async_cancel()
    except (OSError, RuntimeError, KeyError, HomeAssistantError):
        # Discovery is optional. The manual form remains usable if mDNS is
        # blocked, not initialized, or times out.
        return {}

    hubs: dict[str, dict[str, str]] = {}
    ambiguous: set[str] = set()
    for service in services:
        if (
            not isinstance(service, zc_asyncio.AsyncServiceInfo)
            or service.name not in names
        ):
            continue
        try:
            info = ha_zeroconf.info_from_service(service)
            if info is None:
                continue
            endpoint = discovered_endpoint(info)
        except (ValueError, TypeError, AttributeError):
            continue
        hub_id = endpoint["hub_id"]
        if hub_id in hubs and hubs[hub_id] != endpoint:
            ambiguous.add(hub_id)
        hubs[hub_id] = endpoint
    return {
        hub_id: endpoint for hub_id, endpoint in hubs.items() if hub_id not in ambiguous
    }


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
        self._suggested_endpoint: dict[str, str] = {}
        self._discovered_hubs: dict[str, dict[str, str]] = {}
        self._discovery_attempted = False
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
            self.endpoint = discovered_endpoint(discovery_info)
        except (ValueError, TypeError, AttributeError):
            return self.async_abort(reason="invalid_discovery")
        await self.async_set_unique_id(self.endpoint["hub_id"])
        self._abort_if_unique_id_configured(
            updates=self.endpoint, reload_on_update=False
        )
        self.context["title_placeholders"] = {"name": self.endpoint["hostname"]}
        return await self.async_step_user()

    async def async_step_select_hub(
        self, user_input: dict[str, object] | None = None
    ) -> ConfigFlowResult:
        """Choose a discovered hub, or enter connection details manually."""
        errors: dict[str, str] = {}
        if user_input is not None:
            selected = user_input.get("hub")
            if selected == MANUAL_HUB:
                self._suggested_endpoint = {}
                return await self.async_step_user()
            if isinstance(selected, str) and selected in self._discovered_hubs:
                self._suggested_endpoint = self._discovered_hubs[selected]
                return await self.async_step_user()
            errors["base"] = "invalid_config"
        options: list[selector.SelectOptionDict] = [
            {"value": hub_id, "label": f"{ep['hostname']} ({ep['host']}, {hub_id})"}
            for hub_id, ep in sorted(self._discovered_hubs.items())
        ]
        options.append({"value": MANUAL_HUB, "label": "Enter manually"})
        return self.async_show_form(
            step_id="select_hub",
            data_schema=probatio.Schema(
                {
                    probatio.Required("hub"): selector.SelectSelector(
                        selector.SelectSelectorConfig(
                            options=options, mode=selector.SelectSelectorMode.DROPDOWN
                        )
                    ),
                }
            ),
            errors=errors,
        )

    def _schema(self) -> probatio.Schema:
        fields: dict[probatio.Marker, object] = {}
        endpoint = self.endpoint or self._suggested_endpoint
        if self._reauth is None:
            fields[probatio.Required("host", default=endpoint.get("host", ""))] = str
            fields[
                probatio.Required(
                    "hostname", default=endpoint.get("hostname", "VMB5000.local")
                )
            ] = str
            if self._reconfigure is None:
                fields[
                    probatio.Required("hub_id", default=endpoint.get("hub_id", ""))
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
        if (
            user_input is None
            and self.source == "user"
            and not self._discovery_attempted
        ):
            self._discovery_attempted = True
            discovered = await discover_hubs(self.hass)
            configured_ids = self._async_current_ids()
            self._discovered_hubs = {
                hub_id: ep
                for hub_id, ep in discovered.items()
                if hub_id not in configured_ids
            }
            if len(self._discovered_hubs) > 1:
                return await self.async_step_select_hub()
            if self._discovered_hubs:
                self._suggested_endpoint = next(iter(self._discovered_hubs.values()))
        if self._error:
            errors["base"] = self._error
            self._error = None
        if user_input is not None:
            cast(_ArloFlowContext, self.context)["setup_started"] = True
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
                    if self.source == "user":
                        # Manual setup defaults are suggestions. Preserve the
                        # service name only when the submitted endpoint matches
                        # the advertised endpoint; an edit is manual entry.
                        candidate = self._discovered_hubs.get(hub_id)
                        if candidate and (host, hostname) == (
                            candidate["host"],
                            candidate["hostname"],
                        ):
                            self.endpoint["service_name"] = candidate["service_name"]
                        else:
                            self.endpoint.pop("service_name", None)
                        # Manual submission may replace an idle auto-discovered
                        # form, but never interrupt authentication or a form
                        # that already received user input.
                        for flow in self._async_in_progress(
                            match_context={"unique_id": hub_id, "source": "zeroconf"}
                        ):
                            if flow.get("step_id") == "user" and not cast(
                                _ArloFlowContext, flow["context"]
                            ).get("setup_started"):
                                self.hass.config_entries.flow.async_abort(
                                    flow["flow_id"]
                                )
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
