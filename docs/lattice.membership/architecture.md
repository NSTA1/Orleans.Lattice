---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Architecture

`Orleans.Lattice.Membership` implements the core `ILatticeMembershipContext` seam: it turns the credential on the ambient `LatticeCredentialContext` into the `LatticeSubject` - a subject id plus a transitive group closure - that the access gate evaluates. It also owns the group directory that closure is built from and the read-only identity-directory seam that administration validates against. This page describes the pipeline by behaviour; the public types are listed in the [Public API](api.md).

## Registration

`AddLatticeMembership` refuses to run unless `AddLattice(...)` has been called first. It registers the options and their validators, the view infrastructure for the directory trees' per-key history, and these seams, each replaceable by a later registration where noted:

| Seam | Default | Replaced by |
|---|---|---|
| `ILatticeMembershipContext` | The resolving context described below, shadowing the core anonymous context | - |
| `ILatticeMembershipDirectory` | The tree-backed directory | A host registration made first (`TryAdd`) |
| `ILatticeSubjectMapper` | The default mapper | A host registration made first (`TryAdd`) |
| `ILatticeCredentialAuthenticator` (enumerable) | `AnonymousCredentialAuthenticator`, which claims nothing | Added to by `AddLatticeJwtAuthenticator`, the [Entra](../lattice.membership.entra/architecture.md) and [OIDC](../lattice.membership.oidc/architecture.md) packages, or a host |
| `ILatticeIdentityDirectory` | `NullIdentityDirectory` | `AddStaticIdentityDirectory`, the [Entra Graph](../lattice.membership.entra.graph/architecture.md) package, or a host (last registration wins) |
| `ITenantGroupClaimFilter` | An inactive filter | `Orleans.Lattice.Tenancy`, which installs an always-active filter |

The resolution cache is also subscribed to the core `IMutationObserver` change feed, and a membership epoch synchronizer is registered as both a change-feed observer and a hosted service (see [Resolving a subject](#resolving-a-subject)). A repeated call layers its options delegate and skips this wiring.

## The directory

The directory stores group records in the reserved `sys-membership-groups` tree and each membership edge twice in `sys-membership-edges` - once keyed by member and once keyed by group - so both "groups of a member" and "members of a group" are prefix scans. It reads and writes under system origin, so it is a trusted silo-side seam; caller-facing administration goes through the [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/architecture.md) facade, which authorizes first. Every write uses the normal write path, so it reaches the change feed and the per-key history view.

`GroupsOfAsync` walks nested groups transitively with cycle detection. `AddMemberAsync` enforces the tenant-group nesting invariant for every caller before anything is written, reading only the two ids ([Tenant groups](README.md#tenant-groups)). Removing a group does not cascade to its edges.

## Resolving a subject

On each gated request the core asks the membership context for the current subject:

1. **No credential.** With nothing on the credential context the subject is anonymous, with no directory read.
2. **Cache.** The cache key covers the whole credential, not only its token. A warm entry is returned without authenticating again.
3. **Authenticator selection.** On a miss, the registered authenticators are asked in registration order whether they can handle the credential, and the first that answers yes is used. When none does, or authentication returns no principal (an invalid or expired token), the subject is anonymous. `VerifyCurrentCredentialAsync` tells those two apart for a transport binding: a credential an authenticator claimed but resolved anonymous is `Refused`, one nothing claims is `Unverified`, and a resolved identity is `Accepted`, so a binding can answer a wrong password as unauthenticated rather than as the anonymous caller it resolves to.
4. **Directory groups.** Unless `GroupMergeMode` is `TokenOnly`, the principal's transitive groups are read from the directory.
5. **Mapping.** The subject mapper combines the principal and the directory groups. The default mapper applies `GroupMergeMode`, adds any groups `ClaimToGroups` projects from the claims, copies the principal's claims onto the subject, and resolves an empty, reserved (`anonymous`, `system`) or `t/` subject id as anonymous even when a host-supplied authenticator produced it.
6. **Tenant-group claim filter.** When the filter is active, any claim-derived group (token-asserted, overage or projected) starting with `t/` is removed. Directory-derived groups are never filtered. With the inactive default this costs one `bool` read.
7. **Re-expansion.** Unless the mode is `TokenOnly`, a merged set that includes token-asserted or projected seed groups is run back through the directory's transitive closure, so a token carrying only a child group still picks up its ancestors.

Only a resolved identity is cached; an anonymous result never is. An entry lives for the lesser of `ResolutionCacheTtl` and the credential's own expiry, and the cache holds a bounded number of entries. It is flushed when this silo observes a `sys-membership-*` write and whenever `LatticeMembershipOptions` changes. The change feed fires only on the silo that committed the write, so every other silo learns of it through a cluster-wide membership epoch: the committing silo advances the epoch, and every silo reads it about once a second and flushes its cache when it has moved, when the epoch grain has restarted, or when cluster membership has changed (a silo that died between committing a write and publishing it). A removed member therefore loses the group on every silo within about a second of the write. Each successful read also confirms the cache current for three read intervals, and a warm entry is served only while that confirmation holds, so a silo that cannot read the epoch resolves every caller afresh from the directory rather than serving pre-change groups. Hits and misses are counted on the `orleans.lattice.membership` meter ([Observability](observability.md)).

## Authenticators

`JwtCredentialAuthenticator` is both a usable per-issuer authenticator and the base the provider packages specialise. Authentication always validates through one non-virtual step that obtains the provider's validation parameters and then applies the signature-algorithm allow-list, so a subclass cannot produce parameters that skip the pin. A validated token is mapped to a principal (returning none for a missing or reserved subject) and only then offered to the post-validation enrichment hook. See [Extending `JwtCredentialAuthenticator`](api.md#extending-jwtcredentialauthenticator).

## The identity directory

`ILatticeIdentityDirectory` is a read-only search and resolve view onto an external identity source. It is not consulted during subject resolution. The [administration facade](../lattice.api.auth/architecture.md) uses it for typeahead search and, when `LatticeIdentityDirectoryOptions.ValidationRequired` is set and the active provider is not the no-op one, to refuse an unresolvable or mis-kinded id before writing a group or edge. See [Identity-directory providers](identity-directory-providers.md).

## Source map

- [Registration](../../src/lattice.membership/LatticeMembershipServiceCollectionExtensions.cs)
- [Membership context](../../src/lattice.membership/MembershipContext.cs)
- [Resolution cache](../../src/lattice.membership/MembershipResolutionCache.cs)
- [Membership epoch synchronizer](../../src/lattice.membership/MembershipEpochSynchronizer.cs)
- [Directory](../../src/lattice.membership/LatticeMembershipDirectory.cs)
- [JWT authenticator](../../src/lattice.membership/JwtCredentialAuthenticator.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Auth` architecture](../lattice.auth/architecture.md) - the gate that consumes the resolved subject.
