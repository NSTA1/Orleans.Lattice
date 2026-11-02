---
agent_spec: "docs/agents/api/schema.json"
---

# Schema enforcement

Schema enforcement adds per-tree, server-side validation of the values an
opted-in tree's write operations carry (see
[which writes are checked](#setting-a-policy-on-a-tree)). It is provided by the
`Orleans.Lattice.Schema` companion
package and is strictly opt-in: a tree with no policy behaves exactly like a
plain lattice.

## Registering enforcement

Call `AddLatticeSchemaEnforcement` on the silo builder after `AddLattice`:

```csharp verify
using Orleans.Lattice.Schema;

siloBuilder.AddLatticeSchemaEnforcement(options =>
{
    // The global half of strict ingest: let the interceptor inspect system-origin
    // (ingest) writes. Each tree's policy must also opt in (see below).
    options.StrictIngest = true;

    // Also validate the result of a CRDT merge (default off).
    options.ValidateCrdtMergeResults = false;

    // Cap the bytes captured in a dead-letter preview.
    options.DeadLetterPreviewMaxBytes = 4096;
});
```

### Enforcement options

See [configuration](configuration.md) for every silo-wide option, its default, registration-only switches and the composition order.

## Setting a policy on a tree

A policy is an ordered set of [rules](#rule-kinds). Install one with the
in-process `ILatticeSchemaAdmin` service. It performs no authorization of its own;
remote callers reach it through the `SchemaAdmin`-gated
[schema API facade](../lattice.api.schema/README.md) (see
[Capability gate](README.md#capability-gate)):

```csharp verify
using Orleans.Lattice.Schema;

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaAdmin>();

// Every value written to "orders" must now be well-formed JSON.
var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
await admin.SetPolicyAsync("orders", policy, cancellationToken);

// Inspect or clear it later.
var current = await admin.GetPolicyAsync("orders", cancellationToken);
await admin.ClearPolicyAsync("orders", cancellationToken);
```

Once a policy is installed, a local write of a non-compliant value throws a
`LatticeSchemaViolationException` and is never persisted. A CRDT delta is checked
at write time only when the delta's body parses as JSON (for a delta that carries
a version envelope, the body it holds - see
[composition with versioning](#composition-with-versioning)); an opaque delta is
accepted, and only the opt-in merge-result observer (`ValidateCrdtMergeResults`)
sees the merged value.

A policy change takes effect on the silo that served the admin call as soon as
`SetPolicyAsync` or `ClearPolicyAsync` returns, and on every other silo within
`LatticeSchemaEnforcementOptions.PropagationInterval` (one second by default; see
[configuration](configuration.md)). Propagation is eventual, not synchronous:
during that window a write through another silo can still be judged against the
previous policy.

The check runs on the tree's value-carrying write operations: `SetAsync`,
`SetIfVersionAsync`, `GetOrSetAsync`, `SetManyAsync`, `SetManyWherePredicateAsync`,
the atomic batches (`SetManyAtomicAsync`, `SetManyAtomicWhereAsync`, and the
whole-value upserts of a cross-tree atomic batch), the CRDT delta applies
(`ApplyCrdtDeltaAsync`, `ApplyCrdtDeltaManyAsync`) and the bulk loads
(`BulkLoadAsync`, `BulkAppendChunkAsync`). Cross-tree atomic tombstone deletes and
CRDT-delta entries are not whole-value upserts, so they are not inspected by the
cross-tree preflight pass. A batch is checked before any of it is written, so one
non-compliant value in a local batch fails the whole call. A tree merge
(`MergeAsync`) folds the source tree's entries straight into the shards without
passing through the check, so merged values are not validated.

## Rule kinds

A `LatticeSchemaRule` is created with one of these factories. A value must satisfy
every rule the policy carries: the rules are checked in order, and the first one the
value fails rejects it with that rule's reason:

| Factory | Enforces |
|---|---|
| `LatticeSchemaRule.Json()` | The value is well-formed UTF-8 JSON. |
| `LatticeSchemaRule.Utf8()` | The value is well-formed UTF-8. |
| `LatticeSchemaRule.MaxLength(n)` | The value is at most `n` bytes. `n` must be non-negative; `SetPolicyAsync` rejects a rule built without the factory that carries a negative limit. |
| `LatticeSchemaRule.Regex(pattern, memberPath?)` | The value (or a named JSON member) matches a regex. |
| `LatticeSchemaRule.Structured(predicate)` | A JSON document satisfies a `LatticePredicateNode` (the same predicate IR used by [predicate operations](../lattice/predicated-operations.md)). Besides comparisons and string tests, it can use the [structural kinds](../lattice/predicated-operations.md#structural-predicate-kinds): a type test (`TypeOf`), the `Length` operand (created with `LengthOf`), a quantifier over an array's items (`Every`) and the current document (`Self`). |

Every factory also takes an optional `description`, which replaces the rule's default
violation reason when the rule fails. A `Regex` rule's `memberPath` is a dotted path
to a string member, and its pattern is compiled with `RegexOptions.NonBacktracking`
when the policy is set, so `SetPolicyAsync` rejects a pattern that cannot be compiled
that way - as it does a policy aimed at one of the reserved `sys-schema-*` trees -
with an `ArgumentException` rather than failing on a later write.

```csharp verify
using Orleans.Lattice.Schema;

// JSON, no larger than 64 KiB, with a non-empty "id" member.
var policy = new LatticeSchemaPolicy(new[]
{
    LatticeSchemaRule.Json(),
    LatticeSchemaRule.MaxLength(64 * 1024),
    LatticeSchemaRule.Regex(".+", memberPath: "id"),
});

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaAdmin>();
await admin.SetPolicyAsync("orders", policy, cancellationToken);
```

## Strict-mode ingest

Replication apply and backup restore are **trusted by default**: their bytes are
stored verbatim, because a peer or a backup is assumed to have been validated at
its origin. This avoids rejecting trusted ingest merely because it does not meet a local schema policy; it is not a storage-liveness guarantee.

Opt into re-validation with `StrictIngest`. In strict mode an ingested item that
violates the policy is diverted to the tree's [dead-letter
queue](dead-letter-queue.md) instead of being applied, so a bad item is neither
silently accepted nor allowed to stall the ingest stream.

Strict mode only sees ingest that reaches the tree's write operations as a
system-origin write, which in practice is three replication paths. One is the
typed-CRDT path: a replicated CRDT delta (judged, like a local delta, only when
its body parses as JSON) or a full-state CRDT row during bootstrap. Another is the
entries of a replicated atomic batch, which the receiver stages one at a time
through the tree's write operations until the batch's commit arrives. A
dead-lettered entry of such a batch is left out of what the receiver stages, and
the receiver commits the batch's other entries when the commit arrives. An entry
is judged at the same point whether its prepare arrives before or after the
receiver holds the batch's commit decision, so both arrival orders reach the same
verdict: a refused entry is diverted, not applied. The third is a bootstrap: every
last-writer-wins row a receiver drains from a peer's export is judged too. An
export carries committed state only, so it no longer records whether a row was
written by an atomic batch or by a plain write, and it can be the only delivery of
a batch the source withheld (for example after a coordinated restore), so the
receiver judges every drained row rather than let the export become a way past the
check. A steady-state plain
(non-atomic) last-writer-wins replication apply and a backup restore merge or
bulk-load straight into the tree's shards, so even in strict mode their values
are stored verbatim and never dead-lettered.

Strict ingest requires **two** flags to line up, and takes effect only when both
are set:

- the **global** switch on the options (`StrictIngest = true`), which is what makes
  the interceptor inspect system-origin writes (the ingest described above) at
  all; and
- the **per-tree** flag on that tree's policy, set via the
  `LatticeSchemaPolicy(rules, strictIngest: true)` constructor.

With the global switch off, system-origin writes are never inspected, so a
per-tree strict flag has no effect. With the global switch on but a tree's policy
leaving strict off, that tree's ingest is still trusted and its items are applied
as-is. Only a tree whose policy sets the per-tree flag, on a silo whose options
enable the global switch, dead-letters a non-compliant ingested item.

One caveat applies when the silo also registers
[schema versioning](schema-versioning.md): both add-ons then share one composed
write interceptor, which is consulted for system-origin writes when *either*
add-on's global `StrictIngest` is on, and each stage applies only its own per-tree
check. Enabling versioning's global switch alone therefore also dead-letters a
non-compliant ingested item for any tree whose enforcement policy sets the
per-tree flag.

## Bringing existing data into compliance

Installing a stricter policy does not retroactively rewrite the values already
stored. To migrate them, run a background **remediation**: a crash-safe
shadow-build that rewrites every existing value with a
[`LatticeValueTransform`](value-transforms.md), re-validates each against the
target policy, and only cuts the tree over to the remediated data if *every*
value passes.

```csharp verify
using Orleans.Lattice.Schema;

var remediation = client.ServiceProvider.GetRequiredService<ILatticeSchemaRemediationAdmin>();

var report = await remediation.RemediateAsync(
    treeId: "orders",
    transform: LatticeValueTransform.Passthrough(),
    targetPolicy: new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }),
    cancellationToken);

if (report.DidAbort)
{
    // The first offending key/value; the original tree was left untouched.
    Console.WriteLine($"Remediation aborted at '{report.OffendingKey}': {report.Reason}");
}
```

Remediation runs a read-only **dry-run gate** first: if any value cannot be
rewritten to satisfy the target policy, the build aborts with the first offending
key and reason, and the original tree is left completely untouched - no alias
change, no policy change. On a versioning-enabled tree that includes a stored
value that cannot be upcast to the tree's target version (see
[composition with versioning](#composition-with-versioning)). The build then rewrites every value into a fresh
destination tree, re-validating each one; an offender found at that stage aborts
the same way and the partial destination is discarded (soft-deleted). Only a fully
successful build cuts the logical tree over to the remediated destination: it
installs the target policy, then repoints the tree via physical-tree aliasing - moving
the destination's shard map onto the tree in the same registry write as the alias, so
no reader pairs the source with the destination's map or the reverse - then
arms a retained redirect that steers already-materialised readers to the new data.
A remediation holds the tree's alias reservation from the moment it starts until it
completes or aborts - including while an interrupted one waits to be requested
again - so a delete of the tree, and an explicit alias change through
`ILatticeTreeAdmin.SetTreeAliasAsync`, are refused for that whole time, and a remediation
is refused with `InvalidOperationException` while the tree is deleted, a delete is
pending, or a resize or restore holds the reservation; see
[Deleting an aliased tree](../lattice/tree-deletion.md#deleting-an-aliased-tree).
The destination inherits everything about the tree except its values. It is
registered with the shard map the tree routes by (and with it the virtual slot
count), the split allocation mark, the shard-count, leaf-sizing (`MaxLeafKeys`,
`MaxInternalChildren`) and WAL-partition pins, and the per-tree runtime overrides a
resize also keeps: event publishing, projection digest maintenance and its disable
latch, history retention, and the cache-value and WAL retention ceilings. The build
lays every value out by that map, so a resharded or pinned tree - including an app
tree's declared `virtualShardCount` - keeps its topology and sizing across the
cutover; an eager schema-version migration builds its destination the same way.
Host-level named options (`ConfigureLattice(name)`) do not follow the alias, as for
a resize.
The destination is registered as derived from the tree, so after cutover a delete,
recover or purge of the tree acts on the remediated copy the alias targets. The
cutover's alias swap is put to the host's `ITreeOwnershipGuard` like every alias
change (see
[Ownership-bounded aliasing](../lattice/tree-registry.md#ownership-bounded-aliasing)).
A refused swap throws `LatticeTreeOwnershipDeniedException` out of the cutover and
leaves the remediation in flight at `Cutover`, with the target policy already
installed on the tree.

A remediation publishes only the copy of the tree it captured when it started. At
cutover the tree must still resolve to that physical tree (or, when a cutover
resumes after its swap, to the remediation's own destination). If another alias
change has moved the tree, the cutover is refused: a target policy the cutover
installed in the same attempt is withdrawn (the tree's previous policy restored,
or the policy cleared if it had none), nothing is swapped, the remediated copy is
discarded, and the alias reservation is released. A cutover that installed its
target policy and was then interrupted before its swap does not know, when it
resumes and finds the tree superseded, which policy that attempt replaced: it
refuses without touching the policy and leaves the target policy installed, so
check the tree's policy after a superseded remediation. The report ends `DidAbort` with no
`OffendingKey` and a `Reason` naming the physical tree that superseded it; a
tracked operation records `Failed` with `outcome` = `aborted` and that reason, and
no `offendingKey`.

The build copies at the logical level and does not shadow-forward writes that land
on the source while it runs, so run a remediation while the tree is
write-quiescent: a write accepted after the dry-run scan but before cutover is not
carried into the destination and is superseded by the alias swap.

The dry run and the build walk the source in key order with a scan, but they take
each key's value from a point read, which is routed to the shard that owns the key.
The remediated copy therefore holds exactly what a reader of the original was served,
even when a shard holds a stale copy of a key it does not own (an atomic write that
overlaps an online reshard can leave one), and a key a point read finds absent is not
copied.

Remediation is idempotent and resumable. It persists its intent and then works in
bounded slices of values, each resuming strictly after the last value the phase
durably recorded; a slice interrupted by a fault records the values it had already
processed before the fault surfaces. Re-issuing the same `RemediateAsync` call (the
same transform and target policy) after a silo failover therefore resumes from the
last recorded slice rather than from the start. Nothing resumes it on its own - an
interrupted remediation stays in flight until it is requested again - and a call
with different parameters while one is in flight throws
`InvalidOperationException`. A call that joins an in-flight remediation follows
only that remediation; if it ends under the call that started it and a later
remediation replaces its outcome before the joining call can read it, the joining
call throws `InvalidOperationException` naming the followed remediation and applies
nothing further.

`RemediateAsync` drives the remediation to a terminal state before it returns its
`LatticeSchemaRemediationReport`, one slice per call into the cluster, so no single
call runs for the whole remediation. A slice is bounded by time as well as by value
count: once it has run for 10 seconds it stops after the value in hand and returns,
well inside Orleans' default 30-second grain call timeout. A slice call that still times out on a
loaded cluster is retried from the slice's recorded cursor rather than failing the
run, so a value is never written twice; only five timeouts in a row fail it, with the
last timeout as the cause. To start one without waiting, and to follow
its phase and values processed or cancel it before cutover, use the
[schema operations](../lattice.api.schema/operations.md) of the control facade.
Poll a running or last-known remediation with
`ILatticeSchemaRemediationAdmin.GetRemediationStatusAsync`. The status read never
waits behind a running remediation or migration: it answers at once with the last
phase and count the run has durably recorded, never one it is still persisting. The report carries the
`Phase` (`Idle`, `DryRun`, `Build`, `Cutover`, `Completed`, `Aborted` or
`Cancelled`, with `Succeeded`, `DidAbort` and `WasCancelled` as shorthands),
`InProgress`, `ScannedCount` (the values the current phase has processed; the whole
tree once completed), `DestinationTreeId`, and `OperationId` (the tracked
operation's id when a schema operation started the run), and - on an abort - the first
`OffendingKey`, the `Reason` (the policy violation, or the transform's failure
message), and `OffendingValuePreview`: at most `DeadLetterPreviewMaxBytes` leading
bytes of the transformed value, or of the original value when the transform itself
threw. A remediation cancelled before cutover discards its partial destination and
leaves the original tree untouched; one already cutting over cannot be cancelled.

To measure compliance without rewriting anything, call
`ILatticeSchemaComplianceAdmin.ScanComplianceAsync(treeId, cancellationToken)`. It is a
pure read on ordinary read authority: it scans every current value against the tree's
current compiled policy and returns a `LatticeSchemaComplianceReport` carrying
the audited `TreeId`, `HasPolicy`, `RuleCount` (the number of rules in the policy
scanned against), `CompliantCount`, `NonCompliantCount`, `ScannedCount`, and a
`RuleBreakdown` of `LatticeSchemaComplianceRuleCount` (`Reason`, `Count`) rows,
grouped by the reason of the first rule each non-compliant value failed. Each
value is judged as a client read serves it. On a versioning-enabled tree a stored
value that cannot be decoded to the tree's target version (no registered upcast
path, or stamped newer than the target) is counted as non-compliant, under the
reason enforcement gives such a value (`The value's version envelope cannot be
decoded to the tree's target shape.` followed by the decoder's message, so there
is one breakdown row per distinct message); the scan still completes. An
ungoverned tree returns an ungoverned report (`HasPolicy` is `false` and every
count is zero). Remote callers should start the
accept-then-poll compliance operation with
`ILatticeSchemaComplianceOperations.StartComplianceScanAsync`: it returns a
`LatticeOperationHandle` immediately, then reports `Counting` and `Scanning` phases
with `entries` progress and the same report encoded in the operation result. The
blocking `ILatticeSchemaControl.ScanComplianceAsync` verb was removed in this major
version after its 9.9.0 `LATTICE0002` deprecation.

The copy-and-cutover protocol - the alias reservation, intent with a fresh
destination and the captured source, crash recovery from banked progress, the
re-stamp at the target, the policy install, the swap fenced on the captured
source, the source's redirects, and the refusal of a superseded cutover - is
model-checked by the `SchemaRemediationCutover` TLA+ module; its
[refinement note](../../spec/schema/SchemaRemediationCutoverRefinement.md) maps
each step to the code and lists what is not checked, including writes that land
during the build.

## Composition with versioning

When a tree uses both enforcement and [versioning](schema-versioning.md), a value
is validated against its **target (post-upcast) shape**, since that is the
compliant form: on the write path the enforcement stage validates the plain value
before the versioning stage wraps it in the envelope.

A value that arrives already enveloped - a replicated atomic-batch entry or CRDT
delta, or a re-write of stored bytes - is judged by the body a reader is served:
its envelope stripped and the body upcast to the tree's target version. A value
that cannot be decoded to the target (stamped newer than the target, or with no
upcast path to it) is a violation, so a local write fails closed and a strict-mode
ingested item is dead-lettered. An admitted value is stored with its envelope
unchanged. On a silo that does not register versioning no read strips an
envelope, so there the whole value is judged. A value the policy refuses, or that
strict ingest diverts, never reaches a
[materialised view](../lattice/materialised-views.md#projection-input-on-a-versioned-source);
the composition is checked by
[`SchemaViewComposition`](../../spec/schema-view/README.md).

Advancing the target version and re-stamping existing values
(`AdvanceAndMigrateAsync`) is a single shadow build: upcast each value, validate it
against the tree's **existing** policy, cut over, aborting on the first value that
violates it or cannot be upcast (see
[eager background migration](schema-versioning.md#eager-background-migration)).
The migration leaves the policy unchanged, so tightening the policy is the
separate remediation above.

A remediation of a versioned tree reads each stored value and decodes it as the
read path does - stripping its version envelope and upcasting it to the target -
so the transform sees exactly what a client read returns, and the target policy
validates the plain transformed body. Each remediated value is then written back
enveloped at the version the transform ran on: the tree's target version (a
legacy, unstamped value included), or, for a value of another schema family, its
own stamp. After cutover the tree's values stay self-describing, so a later target
advance upcasts them and an eager version migration upcasts them from that stamp.
A remediation of an unversioned tree writes plain values.

A stored value that cannot be upcast to the target (no registered upcaster hop, or
stamped newer than the target) is an offending value like any other: the
remediation returns the abort report rather than throwing, names that key, and
releases its alias reservation, so the tree is never left in flight and nothing is
cut over. Register the missing hop, or correct the target, then start the
remediation again.

## See also

- [Value transforms](value-transforms.md) - the transform IR remediation applies.
- [Dead-letter queue](dead-letter-queue.md) - where strict-mode diversions go.
- [Schema versioning](schema-versioning.md) - the sibling capability.
