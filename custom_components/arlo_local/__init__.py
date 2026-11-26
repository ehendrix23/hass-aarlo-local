"""Arlo Local — a native camera integration backed by a shared local distributor."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.device_registry import DeviceEntry
    from homeassistant.helpers.typing import ConfigType, VolDictType

    from .runtime import Runtime


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register opt-in leases on Arlo Local camera entities only."""
    import voluptuous as vol
    from homeassistant.helpers.service import async_register_platform_entity_service

    async_register_platform_entity_service(
        hass,
        "arlo_local",
        "prewarm",
        entity_domain="camera",
        func="async_prewarm",
        schema=cast(
            "VolDictType",
            {
                vol.Optional("duration", default=90): vol.All(
                    vol.Coerce(int), vol.Range(min=5, max=120)
                )
            },
        ),
    )
    async_register_platform_entity_service(
        hass,
        "arlo_local",
        "release_prewarm",
        entity_domain="camera",
        func="async_release_prewarm",
        schema={},
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry[Runtime]) -> bool:
    from homeassistant.const import Platform
    from homeassistant.core import HassJob
    from homeassistant.exceptions import ConfigEntryNotReady
    from homeassistant.helpers import device_registry as dr

    from .runtime import Runtime

    runtime = Runtime(hass, entry)
    try:
        await runtime.setup()
        entry.runtime_data = runtime
        hub_device = dr.async_get(hass).async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={("arlo_local", runtime.hub.unique_id)},
            manufacturer="Arlo",
            name=runtime.hub.name,
            model=runtime.hub.model,
        )
        runtime.hub_registry_id = hub_device.id
        await hass.config_entries.async_forward_entry_setups(entry, [Platform.CAMERA])
        entry.async_on_unload(entry.add_update_listener(runtime.update_options))
        entry.async_on_unload(
            hass.async_add_shutdown_job(HassJob(runtime.close, "Arlo Local shutdown"))
        )
    except asyncio.CancelledError:
        await runtime.close()
        raise
    except Exception as exc:
        await runtime.close()
        raise ConfigEntryNotReady("Arlo Local setup incomplete") from exc
    return True


async def async_remove_config_entry_device(
    hass: HomeAssistant, entry: ConfigEntry[Runtime], device: DeviceEntry
) -> bool:
    """Permit removal only after a successful inventory confirms absence."""
    from homeassistant.helpers import device_registry as dr

    runtime = entry.runtime_data
    identifiers = [uid for domain, uid in device.identifiers if domain == "arlo_local"]
    if len(identifiers) != 1:
        return False
    uid = identifiers[0]
    owned = dr.async_get(hass).async_get_device_by_identifier(
        ("arlo_local", uid), entry.entry_id
    )
    if owned is None or owned.id != device.id:
        return False
    return await runtime.forget_stale_camera(uid)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry[Runtime]) -> bool:
    from homeassistant.const import Platform

    if not await hass.config_entries.async_unload_platforms(entry, [Platform.CAMERA]):
        return False
    await entry.runtime_data.close()
    return True
