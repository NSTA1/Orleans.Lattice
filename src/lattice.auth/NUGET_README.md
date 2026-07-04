# Orleans.Lattice.Auth

Optional, opt-in **authorization and fail-closed enforcement** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Turns the resolved
`LatticeSubject` from `Orleans.Lattice.Membership` into an enforced access
decision at the data-plane boundary - **byte-for-byte identical to the pre-gate
behaviour, and zero runtime cost, when `AddLatticeAuth` is not registered**.

## Design

`AddLatticeAuth()` supplies the real access gate. It stores rules in a dogfooded
`sys-auth-policy` tree through an `ILatticeAuthorizationPolicyStore`: a
`LatticeAuthorizationRule` binds a subject selector (user / group), a scope
(whole-tree / key / prefix), an operation set, and an `Allow` / `Deny` effect,
and every edit is durably auditable through the store's history. A background
maintainer compiles the rule set into an immutable, monotonically-versioned
in-memory snapshot, rebuilding on every policy change observed through the
change feed, and an `ILatticeDecisionEngine` evaluates a request against that
snapshot with **Deny-wins precedence and prefix specificity**.

Enforcement wires the gate into **every** user-originated mutation and read:

- Writes / deletes / CRDT-apply / bulk-load / lifecycle admin throw
  `LatticeAuthorizationDeniedException` on a denial (carrying only tree id,
  operation, subject id, and reason - never a value).
- A point read of a denied key reports **absent** (no existence oracle); range
  and multi-key reads prune to the authorized subset.
- A range delete is **hard-denied all-or-nothing** (a partial-coverage allow
  refuses rather than narrows), and atomic / cross-tree batches authorize every
  leg **before** any leg is applied, so a single denied key aborts wholesale.

The path is **fail-closed**: an unauthenticated caller resolves to `Anonymous`
and default-denies. A configurable set of bootstrap administrators is the
break-glass root-of-trust bypass. Internal machinery (replication-apply, saga
legs, view maintenance) runs system-origin and never self-filters.

## Consistency and observability

Cross-cluster policy convergence ships in two modes (per the epic's design):
**eventual** last-writer-wins by default, plus an opt-in **strict** epoch fence
(`LatticeAuthOptions.StrictConsistencyTrees`) that closes the cross-cluster
revoke window at the cost of availability - off by default and zero-cost when
off. Every decision is observable through the `orleans.lattice.auth` OpenTelemetry
meter and an optional value-free `ILatticeAuthAuditSink`, both emitted strictly
after the decision so they can never change or delay it.

## Registration

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeMembership()
    .AddLatticeAuth(options =>
    {
        options.DefaultEffect = LatticeEffect.Deny;
        options.BootstrapAdministrators.Add("root-admin");
    });
```

Must be registered after `AddLatticeMembership()`.

See the
[Auth documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.auth/README.md)
and the
[security posture](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.auth/security-posture.md)
for the full guide and the tracked feature index.
