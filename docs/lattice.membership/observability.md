# Membership observability

`Orleans.Lattice.Membership` publishes its telemetry on a single [.NET meter](https://learn.microsoft.com/dotnet/core/diagnostics/metrics), so an OpenTelemetry pipeline can subscribe once and receive every membership metric.

## The meter

Every membership instrument is published on one meter, named by the `LatticeMembershipMetrics.MeterName` constant:

```text
orleans.lattice.membership
```

Recording is guarded by each instrument's `Enabled` flag: when no listener is attached the resolution cache does no measurement work, so the meter is zero-cost on the resolution hot path when nobody is listening.

## Instruments

| Instrument | Name | Kind | Meaning |
|---|---|---|---|
| Resolution-cache hits | `orleans.lattice.membership.resolution_cache.hits` | Counter | One per subject resolution served warm from the per-silo resolution cache. |
| Resolution-cache misses | `orleans.lattice.membership.resolution_cache.misses` | Counter | One per resolution that found no live cache entry and resolved the subject afresh. |
| Directory search latency | `orleans.lattice.membership.directory.search.duration` | Histogram (ms) | One per identity-directory search, timing the provider call the access-administration facade issues. |
| Directory search hits | `orleans.lattice.membership.directory.search.hits` | Counter | One per directory search that returned at least one matching principal. |
| Directory search misses | `orleans.lattice.membership.directory.search.misses` | Counter | One per directory search that returned no matching principal. |

### What the hit / miss counters measure

The per-silo cache turns the credential a caller presents into a resolved subject (id + transitive group closure). Resolution is memoised with a configurable TTL (`LatticeMembershipOptions.ResolutionCacheTtl`, default 5 minutes) and bounded by the inbound token's own expiry, and it is flushed whenever a `sys-membership-*` tree mutates. The counters are recorded at the cache itself:

- A **hit** is counted when the cache serves a warm subject without re-authenticating or reading the directory.
- A **miss** is counted when there is no live entry (never cached, expired past the TTL, past the token's `exp`, or flushed by a membership change) and the cache resolves the subject afresh.

Together they give the cache's hit ratio, which is the signal for tuning `ResolutionCacheTtl`: a low ratio under steady traffic means the TTL is too short (or tokens are short-lived), while a high ratio confirms a burst of calls from the same caller is not re-expanding its group closure every time.

The counters live on the membership meter, not the authorization meter, because the cache they measure lives in this package. `Orleans.Lattice.Membership` sits **below** `Orleans.Lattice.Auth` in the package graph, so sourcing the signal here keeps the layering acyclic - membership never references the authorization meter.

### What the directory search instruments measure

The identity directory (`ILatticeIdentityDirectory`) turns a search term into a bounded page of directory principals - the signal behind the Explorer subject picker's typeahead and the fail-closed create flow. The access-administration facade records one measurement per search around the provider call only, so the histogram isolates directory latency from the facade's mapping and authorization work:

- **Latency** (`directory.search.duration`) times each provider-backed search in milliseconds. For the in-memory static provider this is near-zero; for the Entra Graph provider it reflects the round trip to Microsoft Graph.
- A **hit** (`directory.search.hits`) is counted when a search returns at least one matching principal; a **miss** (`directory.search.misses`) when it returns none. Their ratio is the picker's find-rate, useful for spotting a mis-scoped directory (many misses) or a slow tenant (rising p99 latency).

A cluster with no directory configured never records these instruments: the facade folds straight to an unavailable result without calling the no-op provider, so a token-only deployment shows no directory-search traffic at all.

## Related: policy-coverage on the authorization meter

How many of the members this package resolves actually have an authorization policy configured is an **authorization** signal, not a membership one: it is derived from the compiled policy in `Orleans.Lattice.Auth`, which sits above membership in the package graph. It is published as the `orleans.lattice.auth.snapshot.subjects` observable gauge (distinct users and groups referenced by policy rules) rather than on this meter, so membership never has to reference the authorization policy store. See [Authorization observability](../lattice.auth/observability.md#policy-coverage-gauge).

## See also

- [`Orleans.Lattice.Membership`](README.md) - the identity directory and subject-resolution pipeline these instruments observe.
- [Authorization observability](../lattice.auth/observability.md) - the `orleans.lattice.auth` decision, snapshot, and audit surface built on the subjects this package resolves.
