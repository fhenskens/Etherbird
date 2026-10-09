# Lifecycle improvement plan

Status: implemented for 0.4.0; release verification is recorded in
[RELEASE.md](RELEASE.md). Scope agreed from the rocsteady review on 2026-10-09; baseline is
published Etherbird 0.3.1. Work IDs are defined in
[outstanding work](OUTSTANDING_WORK.md#lifecycle-improvements-identified-by-rocsteady).

## Ownership and delivery boundary

Etherbird owns lifecycle scheduling, readiness, admission, recovery, framework
configuration and internal resource ownership. Adapters classify their own errors
and implement authentication, protocol validation and explicit session teardown.
All four EB-ROC items belong here under that boundary. No ROC-specific error codes,
framing, codecs, heartbeat reader or remote-handle rules enter the framework.

Deliver independently reviewable changes in this order: EB-ROC-3, EB-ROC-2,
EB-ROC-4, then EB-ROC-1. Retain existing defaults where possible. Review public
API and behavioral compatibility before each implementation; version documented
behavior changes rather than silently changing accepted configurations or stop
semantics. The design steps below record the implementation plan; the
[lifecycle policy guide](LIFECYCLE_POLICIES.md) defines the implemented APIs.

## 1. EB-ROC-3: checked configuration

- Inventory constructor checks and runtime behavior in Config, PoolConfig and
  RetryPolicy. Record zero-duration and optional-timeout behavior before changes.
- Add reusable validation errors identifying the field and violated relationship,
  plus validation and checked construction entry points. Reject invalid checked
  configurations before spawning tasks or starting lifecycle/channel activity.
- Require coherent pool size and retry bounds. Decide each zero-duration rule
  explicitly: an immediate deadline can be meaningful; None already expresses an
  absent optional bound. Do not simply import rocsteady's positive-duration policy.
- Preserve existing constructors' behavior in a compatible release; document any
  eventual deprecation or stricter migration separately. Keep address, heartbeat,
  wire/call timeout and application queue policy validation in the adapter.
- Verify defaults, boundary and inverted bounds, optional deadlines and constructor
  side-effect counts. Document accepted values and migration examples.

Exit: adapters can delegate framework validation through public APIs without
duplicating rules or losing their own stricter requirements.

## 2. EB-ROC-2: operation recovery policy

- Add an adapter classification hook for operation errors with explicit retain
  and recover decisions; default to today's recover behavior. Keep is_expected
  solely for diagnostics and is_terminal's existing teardown meaning intact.
- On retain, return the original operation error without retiring the resource.
  On recover, preserve generation checks and teardown. Apply the policy through
  direct Supervisor, leases, pooled execution and generated clients.
- Keep replay opt-in and independent: classification never authorizes replay.
  Existing retry predicates and bounds decide whether another attempt is allowed;
  a retained resource may serve that attempt only when replay was explicitly allowed.
- Cancellation continues to retire a potentially interrupted resource; stale
  failures cannot affect its replacement. Audit races with watchdog recovery/stop.
- Test healthy rejection and local validation with unchanged create/setup counts;
  transport and malformed-response errors with replacement; cancellation,
  stale generations and no implicit replay across execution backends.

Exit: adapters return ordinary Result errors without nesting healthy failures
inside successful results, while current recovery defaults remain compatible.

## 3. EB-ROC-4: post-stop ownership

- Audit attributes.last, pool last_resource, entries, creation callbacks,
  disconnect watchers, recovery/late-completion tasks and tracked operations.
  Separate framework caches from caller-owned handles and captured user values.
- Proposed contract: after awaited stop, release framework resource caches and
  stopped pool entries after required teardown/draining. Resource-derived
  attribute reads return None until a new resource is created. Stored values and
  setter/callback registrations remain available for documented restart/replay.
- Preserve resource-derived reads during outages. Standalone Supervisor restart
  creates a fresh resource; permanent pool/proxy stop does not restart. Resolve
  callback ownership cycles without promising release of arbitrary user captures.
- State late connect/disconnect task exceptions precisely: existing timeout
  semantics can leave cleanup work holding a resource beyond stop. Either retain
  and document that bounded-stop exception or change it in a versioned contract;
  do not promise unconditional Weak expiry while such work is still running.
- Test Drop/Weak observations after direct/pooled stop, retirement/replacement,
  callback/watch cleanup, external handles and late completion; verify operation
  draining appropriate to each backend and attribute behavior after restart.
- Document that external Arc/ResourceHandle values can retain resources and that
  teardown must close streams and destroy session state explicitly. Dropping a
  resource is not a secret-erasure guarantee.

Exit: post-stop ownership is documented and unnecessary internal retention is
removed without conflating direct caller-owned futures with pooled draining.

## 4. EB-ROC-1: non-retryable lifecycle failure

- Add a lifecycle failure classification distinct from operation recovery,
  logging and is_terminal. Default to recoverable for compatibility. Cover create,
  created callbacks, connect and setup; specify watchdog behavior separately.
- Represent a failed, non-ready generation with an inspectable, shared typed
  cause. Stop automatic lifecycle attempts, finish required teardown, and wake
  readiness waiters and affected queued/subsequent callers with that cause.
  Diagnostics must not automatically format potentially sensitive adapter errors.
- Design explicit reset/restart semantics before coding. No implicit acquire or
  execute may clear the failure latch. Reset starts fresh attempts using adapter
  configuration; Etherbird need not introduce a general reconfiguration system.
- Define pool aggregation: healthy slots keep serving work; an individual failed
  slot must not poison the whole pool. Avoid repeatedly spawning replacements for
  the same latched failure. Decide when no eligible slot can serve waiting work,
  how causes aggregate, and how explicit reset enables growth again.
- Give shutdown precedence over failure/reset, preserve capacity accounting and
  bounded admission, and maintain generation isolation for late failure reports.
- Test rejected setup never becoming ready, waiting clones waking, stable attempt
  counts after failure, transient recovery, partial-health/all-failed pools,
  reset races, queued cancellation, generated clients and awaited stop.

Exit: an adapter can fail readiness promptly without its own watch/latch and
connector gate, while transient failures still recover automatically.

## Validation and release

For each slice, add focused regressions and run formatting, default and pool
tests, all-target/all-feature Clippy, documentation checks and affected recovery
demos. Exercise supported Windows/Linux/macOS CI and the declared Rust minimum.
Update the feature and managed-client guides with actual APIs and ownership rules.

Use rocsteady as an external acceptance case for fail-fast authentication, healthy
rejections, malformed-response recovery, cancellation, generation-state cleanup
and no replay. Its local patches/path dependencies may be used for development
checks only; published consumers and CI must use a published dependency.

Publish the compatible or appropriately versioned Etherbird release through the
normal release process. Etherbird completion requires implementation, checks,
documentation and release evidence. Rocsteady's subsequent adoption and workaround
removal are downstream tasks and do not block framework implementation completion.

## Implementation record

Implemented on 2026-10-09 for 0.4.0. Default policies preserve retry and
operation recovery; new enum variants and post-stop resource attributes require
the 0.3-to-0.4 migration described in [the changelog](../CHANGELOG.md).

The implementation provides checked Supervisor/Pool constructors, field-specific
configuration validation, operation retain/recover classification, lifecycle
retry/fail classification, typed redacted failure wrappers, explicit reset and
all-failed pool admission. Stop releases resource caches and entries, while stored
configuration and user registrations remain. Late hook tasks and external/user
captures remain explicit ownership exceptions. No protocol-specific behavior was
added to Etherbird.

Local Windows validation includes default and pool-enabled tests, all-target and
all-feature Clippy, documentation generation, Rust 1.88 compilation/tests, the
MQTT fixture demo and the Modbus/WebSocket live recovery demo. Focused regressions
are in [lifecycle_policies.rs](../tests/lifecycle_policies.rs); logging tests verify
that the framework does not format non-retryable causes.

External acceptance used a temporary copy of rocsteady 0.1.0-alpha.1 under ignored
`scratch/rocsteady-lifecycle-consumer`, with a development-only dependency on this
checkout. Its Hooks implement the new classification policies; authentication
watch/latch and connector gating were removed, framework backoff validation was
delegated, and operation paths return ordinary Results without preserve_device_error.
The adapter maps the shared authentication cause to its existing public error shape
and keeps positive protocol deadlines, stop precedence and explicit Session cleanup.
All 43 consumer tests (3 configuration, 11 extension, 18 integration, 11 protocol)
pass on current Rust and Rust 1.88 with all features/targets. This validates the
adoption design; the actual rocsteady dependency remains published 0.3.1.

Reproduction commands from this checkout after preparing that temporary consumer:

```powershell
cargo test --locked
cargo test --locked --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo +1.88.0 test --offline --all-features --lib --tests --target-dir target/msrv-1.88
cargo run --locked --features pool --example mqtt_with_etherbird -- --demo
cargo run --locked --features pool --example live_recovery
cargo test --offline --manifest-path scratch/rocsteady-lifecycle-consumer/Cargo.toml --all-features --all-targets --target-dir target/rocsteady-lifecycle-consumer
cargo +1.88.0 test --offline --manifest-path scratch/rocsteady-lifecycle-consumer/Cargo.toml --all-features --all-targets --target-dir target/rocsteady-msrv-1.88
```

The temporary consumer and its preparation script are local validation artifacts,
not required inputs for framework tests or published consumers. Release checks and
cross-platform CI evidence are tracked in [RELEASE.md](RELEASE.md). Production
rocsteady adoption is a separate downstream task.
