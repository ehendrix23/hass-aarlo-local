# Arlo Local for Home Assistant

**Development preview (0.5.0b5). No stable release yet.** This integration is intended for controlled observation-only testing. Do not use it for safety-critical automation, including garage-door actuation, before independent validation. The two-camera 31-minute active-stream endurance gate has not passed.

Arlo Local discovers cameras through a compatible Arlo SmartHub and provides Home Assistant camera entities backed by authenticated local video and snapshots. It runs its own isolated go2rtc distributor and does not manage Home Assistant's existing go2rtc instance or replace Aarlo.

## Requirements and scope

- Home Assistant 2026.11.3 or later (the minimum tested Core version is 2026.11.3).
- A compatible Arlo SmartHub reachable on the same local network. VMB5000 is the physical test platform; compatibility with every camera model is not established.
- The Arlo owner account and MFA for inventory and certificate provisioning. The cloud is still needed for these operations and certificate renewal; the video transport itself is local. Continuous WAN-independent operation has not been accepted.
- `go2rtc` and `ffmpeg` available to the Home Assistant Core process on PATH, with FFmpeg H.264/libx264 support. The released Home Assistant Core 2026.11.3 AMD64 image includes go2rtc 1.9.15 and FFmpeg 8.1.3 with libx264; the image is tested in CI. Marketplace installs the Python integration, not these executables. Actual production HAOS resource load still needs verification.

## Install a preview

1. In Home Assistant, open **Settings > Marketplace**, open its menu, and add the custom repository `https://github.com/ehendrix23/hass-aarlo-local` as an integration (where a category is requested).
2. Select Arlo Local and explicitly select the development preview when offered. Do not install an unreviewed default branch or treat this preview as a stable release.
3. After download, restart Home Assistant when requested. Under **Settings > Devices & services**, add Arlo Local or use the discovered SmartHub.
4. Adding Arlo Local manually searches the local network for SmartHubs using Home Assistant's shared mDNS discovery. A single unconfigured SmartHub pre-populates its IP address, hostname and device ID; multiple hubs are offered as a choice. If discovery is blocked, unavailable, or finds no usable unconfigured hub, enter the connection details manually. Check the details, then authenticate with its owner account. Discovery does not bypass owner-inventory or certificate verification. Supported MFA choices are push, IMAP, manual email and manual SMS.

5. Before opening any local camera stream, configure the integration's maximum active streams to **1**. Use on-demand acquisition only and leave prewarm disabled. Keep Aarlo and existing physical-control paths unchanged while comparing results.

When a manually added hub matches its discovered service, the integration retains that service name for its existing mDNS address-refresh behavior. No camera streams or certificate requests are started by the discovery scan.

For HACS installations, add the same URL as a custom repository with category **Integration** and enable pre-release versions. Keep the installed directory and integration domain `custom_components/arlo_local` unchanged. Do not configure a second entry for the same hub when updating.

Never post account credentials, certificates, keys, tokens or unredacted Home Assistant logs in issues. Back up the existing Home Assistant configuration before installing or upgrading. The integration stores credential material separately from its source directory; do not delete that storage to force another certificate issuance.

## Video behavior

Each active camera has one upstream authenticated RTSP session; local consumers share its distributed video. Configure the per-hub maximum number of active streams in the integration options (1–5, default 3). The limit applies to new admissions; decreasing it does not interrupt existing viewers. Optional warm-only camera leases are limited to two and by the configured budget. Five simultaneous streams have been demonstrated in short physical trials, not accepted as a sustained operational capacity. Audio, recording management, motion and sound sensors are outside this release. Some camera models, including VMC3060B, have unresolved compatibility issues.

Real video demand may reclaim a slot from an optional warm-only camera, but will not disconnect an established real viewer. If all slots are occupied by actual viewers, additional demand fails rather than exceeding the local limit. Failed upstream startup attempts and repeated warm-worker exits use bounded exponential backoff of 1–15 seconds. This local limit does not account for independent Arlo app, cloud or recording activity; coexistence at the hub's full capacity is not established.

The optional `arlo_local.prewarm` action requests a bounded warm lease on a camera entity. `duration` is 5–120 seconds (default 90). This does not assert that video is ready. `arlo_local.release_prewarm` releases the warm lease without stopping independent viewers. No automatic warming is enabled by default.

The H.264 encoder preserves input frame timing and does not synthesize frames to match an inferred constant frame rate. This does not eliminate delayed frames arriving from the SmartHub; recording coexistence and natural session rollover remain unaccepted.

The camera entity exposes a loopback-only HTTP MPEG-TS video source to avoid the native HA stream worker's fixed RTSP startup timeout, including when it reuses a cached stream. The internal snapshot path uses the existing normalized RTSP source. HTTP readers stream-copy the normalized H.264 video without additional camera sessions or video encodes. There is a four-reader limit per camera, a ten-second producer socket timeout, a twelve-second first-media deadline for the HTTP path and a separate three-second established-output advancement watchdog. Generation loss closes affected HTTP and RTSP readers. This adds local remux processes and buffers; production resource consumption has not yet been accepted. The new source is not a guarantee of live optical frame age or a network-accessible camera endpoint.

The distributor staggers planned warm-only renewal; real viewers are not proactively interrupted for that renewal. Unexpected stream loss or a detected timeline discontinuity closes affected consumers so they can reopen and recover. A normalized RTP timeline check allows legitimate 32-bit wrap and detects backward timestamps/source changes. It is not proof of the optical age of a frame, and a consuming application must independently enforce freshness and fail closed.

## Development and verification

The isolated synthetic test suite requires Python 3.14.2, the locked `ha-test` environment, FFmpeg and go2rtc. It uses temporary synthetic certificates and simulated camera endpoints; no live camera credentials are required. See the CI workflow for the reproducible commands. The quality gate requires at least 95% branch coverage in every integration module with branches and complete config-flow coverage.

Physical-camera short recovery tests have passed, including independent and simultaneous stream failures and rejection of a sixth request before upstream admission followed by recovery after capacity becomes available. Synthetic tests exercise native HA startup and cached-worker recovery with a delayed first frame, as well as concurrent HTTP and RTSP readers. Short single-camera physical HTTP streaming has passed, but sustained five-camera native streaming has not. Long-duration natural rollover, recording coexistence, production HA resource load, model compatibility and garage shadow-mode validation remain acceptance gates. See [release notes](CHANGELOG.md).

## Attribution and licensing

Arlo Local uses [pyaarlo](https://github.com/twrecked/pyaarlo), [go2rtc](https://github.com/AlexxIT/go2rtc), [FFmpeg](https://ffmpeg.org/), [PyAV](https://github.com/PyAV-Org/PyAV) and [Home Assistant](https://github.com/home-assistant/core). Those separately installed projects retain their own licenses. No executable binaries or dependency source distributions are bundled. This is an independent project, not an official Arlo product or an Aarlo replacement. The repository owner's choice of license for the original integration code is pending; no additional open-source license is implied by publishing this preview.
