# Orleans.Lattice.Membership

Optional, opt-in **caller-identity resolution** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Resolves *who is
calling* into a `LatticeSubject` - a stable subject id, the fully
transitively-expanded set of group ids, and an optional flat claim bag - so the
authorization layer (`Orleans.Lattice.Auth`) can make an access decision.

## Design

`AddLatticeMembership()` contributes the real credential-resolving context. It
selects the first registered `ILatticeCredentialAuthenticator` that recognizes
the ambient `LatticeCredential`, maps the resulting principal against an
introspectable `ILatticeMembershipDirectory`, and **expands group membership to
its full transitive closure with cycle detection**, so downstream policy always
evaluates a flat, uniform group set. Nested (group-in-group) membership is
supported, and token-asserted groups are themselves expanded through the
directory closure.

The directory dogfoods reserved `sys-membership-*` `ILattice` trees (users,
groups, and each membership edge stored for forward/reverse scans), so every
record is readable through the ordinary scan / change-feed surface and every
mutation is durably auditable through an auto-enabled per-key history view.
Resolution is served from a per-silo cache bounded by the minimum of the
configured lifetime and the inbound token's own expiry, and flushed on any
`sys-membership-*` mutation, so a membership change is reflected without a
process restart.

- **Opt-in and zero-cost when absent.** Core ships only an allow-nothing
  default membership context that always resolves `Anonymous`; nothing runs
  until `AddLatticeMembership()` is called.
- **Extensible authenticators.** A built-in `JwtCredentialAuthenticator`
  (issuer / audience / signing-key / lifetime validation plus claim-to-subject
  and claim-to-groups mapping, registered per trusted issuer via
  `AddLatticeJwtAuthenticator`) is the base for provider-specific authenticators
  such as `Orleans.Lattice.Membership.Entra`; `AnonymousCredentialAuthenticator`
  is the fallback default.

## Registration

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeMembership()
    .AddLatticeJwtAuthenticator(options => { /* issuer, audience, signing key */ });
```

Group-merge policy (`Union` / `TokenOnly` / `DirectoryOnly`), the resolution-cache
lifetime, history retention, and an optional claim-to-group projection are
configured through `LatticeMembershipOptions`.

See the
[Membership documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.membership/README.md)
for the full guide and the tracked feature index.
