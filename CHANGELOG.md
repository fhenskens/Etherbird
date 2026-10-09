# Changelog

## 0.4.0 — 2026-10-09

- Add `Lifecycle::lifecycle_failure` with retry/fail policy, latched typed causes,
  `ResourceState::Failed` and `Error::Lifecycle(Arc<E>)`. Initialization and watch
  failures can suspend attempts until explicit `reset_failure` / `reset_failed`.
  Healthy pool slots remain usable; all-failed pools fail admission without growth.
- Add `Lifecycle::operation_failure` with recover/retain policy. Healthy operation
  errors can return normally without resource replacement or nested results.
  Logging classification and explicit replay predicates remain independent.
- Add `Config::validate`, `PoolConfig::validate`, `RetryPolicy::validate`,
  `ConfigError` and checked Supervisor/Pool constructors. Legacy constructors keep
  their accepted configurations and panic behavior. Checked lifecycle backoff must
  be positive and ordered; zero hook deadlines remain immediate deadlines.
- Release framework resource caches and pool entries after awaited shutdown.
  Resource-derived attributes return None; stored configuration and registrations
  remain. External handles, user captures and abandoned timed-out hooks can retain
  resources. Adapters still explicitly close streams and destroy session state.

Migration from 0.3: update exhaustive Error/ResourceState matches for the new
variants; use stored values for post-stop configuration reads rather than resource
attributes. New hooks default to existing recovery behavior. See
[the lifecycle guide](docs/LIFECYCLE_POLICIES.md) for reset, ownership and validation.
