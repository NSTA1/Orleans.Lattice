# Installable apps

This module checks the design of the installable-app security seams (issue
#6051): the cluster-wide `AppInstall` capability that gates an install or
upgrade, the manifest digest pin that ties an install to the manifest an
operator reviewed, the role compiler that never emits a scopeless capability,
the `app:` rule namespace only system origin may write, the alias
tree-ownership guard, and the app UI bridge that answers a workspace verb. It
is not a machine-checked refinement proof of the C# implementation; the
[refinement note](Refinement.md) states which production seams its detectors
reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `AppsInstall` | 7 | 0 | 9 | 15 | 15 | 4,320 |

## AppsInstall

**Authorization is the F1 contract.** The module declares the abstract
operator `Allowed(caller, op, scope)` of `spec/auth-decision/` (contract
version 1) and its cfg binds it to `ContractAllowed`, the contract's minimal
grant-set definition: one caller holds `AppInstall` over the cluster-wide
tree, one holds `AppInstall` over a single data tree only, and one holds
nothing. Nothing here depends on how `Allowed` reaches its verdict beyond
those grants, so the module holds for any definition satisfying C1-C6.

**Actors and actions.** An operator reviews the manifest the source serves
and records its digest (`Review`); the source may change the manifest
afterwards (`ChangeManifest`). Any caller, or system origin, installs or
upgrades the app, optionally pinned to the reviewed digest (`Install`). The
role compiler admits the entries of a role ceiling (`CompileRoles`), and
activation persists the app's rules under system origin (`ActivateRules`).
An operator or system origin writes or deletes an app-owned rule id
(`WriteAppRule`, `DeleteAppRule`). Any caller, system origin included,
assigns an alias through whichever ownership guard the silo binds: the
ledger-backed guard, the default guard of a host without the apps package,
or a guard whose ledger read faults (`SetAlias`). A member of the app's role
calls the UI bridge naming an app, a revision and a tree (`BridgeAuthorize`).

**Properties.**

- `InstallOnlyWithClusterWideAppInstall`: an install or upgrade that writes
  was made by system origin or by a caller holding `AppInstall` over the
  cluster-wide tree, never a narrower scope.
- `InstalledEqualsReviewed`: a pinned install that writes installs exactly the
  content the operator reviewed.
- `RoleCompilerNeverEmitsScopeless`: no compiled role grant carries a
  scopeless capability or reaches a tree that is not an ordinary data tree.
- `AppRulesSystemOriginOnly`: every app-owned rule was last written by system
  origin, a non-system write or delete never lands, and a refused delete does
  not disclose whether the rule exists.
- `AliasOwnershipPreserved`: every alias resolves to a target whose derivation
  root has the alias name's owner, and a default decision or a guard fault
  writes nothing, whoever the caller.
- `WorkspaceNonDisclosure`: for a member of the app's role, an app that is not
  installed at the requested revision is answered exactly as an app that does
  not exist, and a verb is allowed only with the role grant on the tree.

**Bounds.** Three callers, two manifest contents with distinct digests, three
capabilities (two of them scopeless), three trees (one reserved), one
app-owned rule id, two alias names, three alias targets (one a derived copy of
the app's tree), three guards, two app ids, two revisions and three requested
trees. Every property is a safety property over the last step or the state, so
these bounds reach every counterexample the mutations target. There are no
liveness properties.

## Refinement and mutations

- [Refinement note](Refinement.md), including the deliberate abstraction gaps.
- [Mutations](mutations/): every property and every action of `Next` has a
  falsifying mutation; no action is exempt.
