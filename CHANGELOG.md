# Release notes

## 0.5.0b3 — development preview

- Preserve source frame cadence and demuxer time base during H.264 normalization rather than synthesize constant-rate frames.
- Exercise variable-rate input through the actual TLS/RTSP relay and FFmpeg distributor, including frame-count bounds.
- A corrected physical single-camera stream completed a two-minute duration test without decoder errors or excessive relative lag. Upstream delays during external camera activity remain unresolved; recording coexistence, natural session rollover, production load, and garage shadow-mode gates remain open.

## 0.5.0b2 — development preview

- Actual video requests can reclaim an occupied warm-only camera slot; real viewers remain protected.
- Reject new optional warm leases when all upstream slots are occupied, unless the requested camera already has an active session.
- Bound repeated failed producer startups and warm retries using monotonic exponential backoff.
- Add synthetic multi-camera admission and startup refusal regressions, plus exact released Core-image validation.
- Controlled physical-camera priority test passed with a test-only two-slot limit and independent TCP socket samples. Full endurance and external recording coexistence are not accepted yet.

## 0.5.0b1 — development preview

- Shared authenticated local video sessions, camera snapshots and on-demand warm leases.
- Per-camera downstream invalidation and independent reconnect handling.
- Staggered warm-only session renewal, with active viewers protected from planned renewal.
- Normalized video RTP timeline discontinuity detection and wrap handling.
- Process cleanup using verified process identities rather than unconditional group signaling.
- Marketplace/HACS custom-repository metadata, versioned package and automated synthetic quality checks.

Not a production acceptance release. Long-duration natural-session rollover, HAOS runtime prerequisites and load, and observation-only garage integration still require verification. Preserve any accepted prior installation and backups.
