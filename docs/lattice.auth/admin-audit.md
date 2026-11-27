---
agent_spec: "docs/agents/governance/audit-events.json"
---

# Admin audit trail

`Orleans.Lattice.Auth` records every privileged administrative decision and mutation in a durable **admin audit trail** ([#3638](https://github.com/NSTA1/Orleans.Lattice/issues/3638)). Each record says who acted, what they attempted, on which target, and how it ended, including refusals. The trail is on by default wherever `AddLatticeAuth` is registered.

It is separate from the [decision trail](observability.md#the-audit-sink). The decision trail records sampled data-plane authorization decisions; the admin audit trail records every administrative call, unsampled.

## What is recorded

| Facade | Actions |
|---|---|
| `ILatticeAuthAdmin` | `RuleUpsert`, `RuleRemove` (including an operator's break-glass removal of a tenant-tier rule), `GroupUpsert`, `GroupRemove`, `GroupMemberAdd`, `GroupMemberRemove` |
| `ILatticeTenantPolicyAdmin` | `RuleUpsert`, `RuleRemove` |
| `ILatticeTenantDirectoryAdmin` | `GroupUpsert`, `GroupRemove`, `GroupMemberAdd`, `GroupMemberRemove`, `TenantMemberAdd`, `TenantMemberRemove` |
| `ILatticeTenantAccessAdmin` | `TenantAdministratorAdd`, `TenantAdministratorRemove` |
| `ILatticeTenantAdmin` | `TenantCreate`, `TenantSuspend`, `TenantResume`, `TenantDelete`, `TenantQuotasSet` |
| `ILatticeTenantGrantAdmin` | `GrantOffer`, `GrantApprove`, `GrantReject`, `GrantRevoke` |
| `ILatticeTenantRegionAdmin` | `TenantRegionsAllow`, `TenantResidencySet`, `TenantRegionAdvance` |
| `ILatticeAppsControl` | `AppInstall`, `AppUpgrade`, `AppEnable`, `AppDisable`, `AppUninstall`, `AppConsentUpdate`, `AppRoleBindingsUpdate` |
| `ILatticeTreeAdmin` | `AliasSet` |

A refused read or introspection call on these facades (a listing, an explain, a status read) is recorded as a `Read` denial. Successful reads are not recorded. The app-control capability probe is not recorded either, because a probe expects to be refused.

Not recorded here:

- data-plane authorization decisions, which are the [decision trail](observability.md#the-audit-sink);
- tenancy runtime decisions (quota admission, rate limiting, placement), which stay metrics and logs;
- gRPC calls refused before authentication, which have no principal and stay interceptor logs.

## The record

Each record is a `LatticeAdminAuditRecord` in the reserved `sys-auth-admin-audit` tree.

| Field | Meaning |
|---|---|
| `OperationId` | Identifies one facade call. The call's intent and its final record share it. |
| `Outcome` | `Pending`, `Committed`, `Failed`, `Denied` or `Indeterminate`. Only `Committed` claims a change happened. |
| `Action` | The `LatticeAdminAuditAction`. |
| `ActorSubjectId`, `ActorKind` | The subject the facade authorized, resolved from the caller context, never from the request. `ActorKind` is `Subject` or `BootstrapAdministrator`. |
| `ActorTenant` | The caller's active tenant, where the facade knows it. |
| `TargetScope` | `cluster`, or `t/{tenant}` for a tenant's own administration. |
| `TargetKind`, `TargetId` | The object acted on, by identifier only. |
| `AnchorTreeId`, `AnchorKey` | The record the action writes last: its commit point. |
| `AnchorBaseVersion`, `AnchorBaseHash` | The anchor as it was before the commit. |
| `CommittedRevisionHlc`, `AnchorValueHash` | On a commit, the anchor revision the call produced. |
| `ReasonCode` | A stable code from `LatticeAdminAuditReasonCodes`. |
| `Detail` | A fixed set of scalar parameters per action, for example a rule's effect or an app's version. |
| `OriginRegion` | The region that wrote the record. |
| `TimestampUtc`, `CompletedUtc` | When the record (or intent) was written and when it was finalized. |

**No state copy.** A record never stores the changed state. To see what changed, read the anchor key's history from the record's `CommittedRevisionHlc` with `ILattice.ScanEntryHistoryAsync`. The anchor trees keep durable history views by default.

**No secret material.** A record carries identifiers, enumerations, hashes, timestamps and allow-listed scalars only. It never carries rule bodies, manifests, consent, credentials or exception messages.

**Bounded caller text.** `TargetId`, `ActorTenant` and `Detail` are built from the caller's arguments, before authorization and validation. A value longer than 256 characters is stored as its first 64 characters followed by `...[truncated len={length} xxh64={hash}]`, and a record keeps at most 8 detail entries, so an unauthorized caller cannot make denials store large text.

## How a call is recorded

1. **Authorize.** A refusal writes one `Denied` record, then the refusal is returned. If that write fails, the refusal is still returned and the failure is counted.
2. **Intent.** Before anything is written, a `Pending` intent is written at `pending/{operationId}`. If the intent cannot be written, the call is refused with `LatticeAdminAuditUnavailableException` and changes nothing.
3. **Commit.** The facade runs its change, unchanged.
4. **Finalize.** The intent moves to its final key, `{scope}/{ticks}\u001f{operationId}`, with the observed outcome:
   - `Committed` only when the call's own store result reports a change: an unconditional write that returned, or a removal or transition the store reports it performed;
   - `Failed` with `failed.no-change` when the store reports no change and nothing at all wrote the anchor since the intent;
   - `Indeterminate` when the store reports no change but something wrote the anchor (for example a concurrent removal that pre-empted this one), or when that cannot be proven;
   - `Failed` for a validation error, a conflict, a quota refusal or a missing target;
   - `Denied` for a tier-confinement refusal after authorization.

   The anchor alone never says what this call did, because a concurrent or replicated write can move it either way. The only fact it proves is "nothing wrote it since the intent": its version equals the version observed before the commit, or, for an anchor absent before, its history holds no revision of any kind since the intent's time less a five-minute clock-skew allowance (a delete between an absent before and an absent after counts as a write). A history view may lag, so at finalize time only an untruncated write-ahead-log window proves an absence.

   The final record is written create-only, so a call is finalized at most once.
5. **Reconcile.** A failure that may have struck after the commit landed (a timeout, a cancellation, a storage fault), a crash between commit and finalize, or an operation that returns before it settles (an app activation still running after its accept window), leaves the intent pending. Each silo's reconciler resolves its own region's intents that are older than `AdminAuditReconcileLease`, from the same "nothing wrote it since the intent" test:
   - anything wrote the anchor: `Indeterminate`;
   - nothing wrote it: the operation may still be running, so the reconciler waits, and records `Failed`, `failed.not-committed` only once the intent is older than `AdminAuditMaxPendingAge` (by then the history view is trusted to prove an absence);
   - the evidence cannot prove either (no history, or an anchor tree without versions): the reconciler waits, and records `Indeterminate` after `AdminAuditMaxPendingAge`.

   The reconciler never records `Committed`. An `Indeterminate` record still names the actor, so every committed call is attributed by exactly one record, and no record claims a change that did not happen. An intent whose final record already exists (the caller finalized but its intent delete was lost) is simply removed.

## Replication and restore

The admin audit tree replicates with the other audit tree when `ReplicateLatticeSystemTrees(includeAudit: true)` is set. A replicated record is applied as data and is never audited again. A change applied by replication or by a restore never passes through a facade, so it creates no record. Each region resolves only its own intents, from its own history.

## Retention

Final records are kept until `AdminAuditTrailTimeToLive` (default: kept indefinitely); the reconciler removes older ones about once an hour. Pending intents are never aged out.

## Configuration

| Option | Default | Meaning |
|---|---|---|
| `EnableAdminAuditTrail` | `true` | Records the admin audit trail. While it is on, a mutation whose intent cannot be written is refused. |
| `AdminAuditTrailTimeToLive` | `null` | How long a final record is kept; `null` keeps every record. |
| `AdminAuditReconcileLease` | 2 minutes | How long an intent may stay pending before the reconciler resolves it. |
| `AdminAuditMaxPendingAge` | 1 hour | An intent whose anchor was not written is recorded `failed.not-committed` only after this; an intent the evidence cannot resolve is recorded `Indeterminate` after it. Set it above the longest administrative operation. |
| `AdminAuditReconcileInterval` | 1 minute | How often each silo's reconciler sweeps. |

```csharp verify
siloBuilder.AddLatticeAuth(options =>
{
    // On by default; set a retention bound for final records.
    options.AdminAuditTrailTimeToLive = TimeSpan.FromDays(400);
});
```

## Observability

`orleans.lattice.auth.admin_audit.write_failures` counts admin audit writes that failed. Any non-zero rate means the trail is degraded; see [Observability](observability.md#instruments).
