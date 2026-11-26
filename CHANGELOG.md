# Release notes

## 0.5.0b1 — development preview

- Shared authenticated local video sessions, camera snapshots and on-demand warm leases.
- Per-camera downstream invalidation and independent reconnect handling.
- Staggered warm-only session renewal, with active viewers protected from planned renewal.
- Normalized video RTP timeline discontinuity detection and wrap handling.
- Process cleanup using verified process identities rather than unconditional group signaling.
- Marketplace/HACS custom-repository metadata, versioned package and automated synthetic quality checks.

Not a production acceptance release. Long-duration natural-session rollover, HAOS runtime prerequisites and load, and observation-only garage integration still require verification. Preserve any accepted prior installation and backups.
