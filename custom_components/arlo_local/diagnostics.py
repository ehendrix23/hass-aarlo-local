"""Allowlisted diagnostics with no credentials, identifiers, paths or URLs."""

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .budget import stream_budget
from .const import VERSION
from .runtime import Runtime


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry[Runtime]
) -> dict[str, object]:
    """Report operational state without including any raw entry or network data."""
    runtime = getattr(entry, "runtime_data", None)
    result: dict[str, object] = {
        "version": VERSION,
        "entry_state": entry.state.value,
        "runtime_loaded": isinstance(runtime, Runtime),
    }
    if not isinstance(runtime, Runtime):
        return result
    broker = runtime.broker
    result.update(
        {
            "local_available": runtime.available,
            "camera_count": len(runtime.cameras),
            "max_streams": stream_budget(entry.options),
            "active_upstream_count": sum(
                r.connected_since is not None for r in broker.relays.values()
            )
            if broker
            else 0,
            "admitted_stream_count": broker.admitted_count if broker else 0,
            "native_reader_count": sum(
                len(readers) for readers in broker.native.clients.values()
            )
            if broker
            else 0,
            "warm_lease_count": len(broker.warm_leases) if broker else 0,
            "output_stalls": sum(fence.stalls for fence in broker.fences.values())
            if broker
            else 0,
            "registered_stream_count": len(broker.relays) if broker else 0,
            "distributor_healthy": broker.healthy if broker else False,
            "camera_models": sorted(
                camera.model for camera in runtime.cameras.values()
            ),
            "camera_interface_versions": [
                {"model": camera.model, "interface_version": camera.interface_version}
                for camera in sorted(
                    runtime.cameras.values(),
                    key=lambda camera: (camera.model, camera.interface_version or ""),
                )
            ],
        }
    )
    return result
