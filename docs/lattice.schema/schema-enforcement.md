# Schema enforcement

Schema enforcement adds per-tree, server-side validation of every value written
to an opted-in tree. It is provided by the `Orleans.Lattice.Schema` companion
package and is strictly opt-in: a tree with no policy behaves exactly like a
plain lattice.

## Registering enforcement

Call `AddLatticeSchemaEnforcement` on the silo builder after `AddLattice`:

```csharp verify
using Orleans.Lattice.Schema;

siloBuilder.AddLatticeSchemaEnforcement(options =>
{
    // The global half of strict ingest: let the interceptor inspect replicated /
    // restored writes. Each tree's policy must also opt in (see below).
    options.StrictIngest = true;

    // Also validate the result of a CRDT merge (default off).
    options.ValidateCrdtMergeResults = false;

    // Cap the bytes captured in a dead-letter preview.
    options.DeadLetterPreviewMaxBytes = 4096;
});
```

### Enforcement options

`LatticeSchemaEnforcementOptions` holds the silo-wide switches; per-tree behaviour
(the rules and the per-tree strict flag) lives in each tree's policy.

| Option | Type | Default | Effect |
|---|---|---|---|
| `StrictIngest` | `bool` | `false` | The global half of [strict-mode ingest](#strict-mode-ingest). While it is `false` the enforcement stage does not ask to see system-origin (replication apply / restore) writes, so trusted ingest pays nothing - but see the caveat there for a silo that also registers schema versioning. |
| `ValidateCrdtMergeResults` | `bool` | `false` | Registers a post-merge observer that validates each merged value against the tree's policy. It never rejects or rewrites a merge: a violation becomes a non-mutating `LatticeMergeOutcome.AcceptWithEvent` annotation, which the core does not currently surface to any log, metric, or event sink. The flag is read only from the delegate passed to the first `AddLatticeSchemaEnforcement` call; setting it through `ConfigureLatticeSchemaEnforcement` or a repeat `AddLatticeSchemaEnforcement` call does not register the observer. |
| `DeadLetterPreviewMaxBytes` | `int` | `4096` | The maximum number of leading value bytes copied into the `ValuePreview` of a dead-letter entry the enforcement stage writes, and into a remediation (or eager version migration) abort's `OffendingValuePreview`. A value below `1` is treated as `1`. |

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
at write time only when the delta itself parses as JSON; an opaque delta is
accepted, and only the opt-in merge-result observer (`ValidateCrdtMergeResults`)
sees the merged value.

## Rule kinds

A `LatticeSchemaRule` is created with one of these factories. A value must satisfy
every rule the policy carries: the rules are checked in order, and the first one the
value fails rejects it with that rule's reason:

| Factory | Enforces |
|---|---|
| `LatticeSchemaRule.Json()` | The value is well-formed UTF-8 JSON. |
| `LatticeSchemaRule.Utf8()` | The value is well-formed UTF-8. |
| `LatticeSchemaRule.MaxLength(n)` | The value is at most `n` bytes. |
| `LatticeSchemaRule.Regex(pattern, memberPath?)` | The value (or a named JSON member) matches a regex. |
| `LatticeSchemaRule.Structured(predicate)` | A JSON document satisfies a `LatticePredicateNode` (the same predicate IR used by [predicate operations](../lattice/predicated-operations.md)). |

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
its origin. That keeps ingest fail-open - it must never block.

Opt into re-validation with `StrictIngest`. In strict mode an ingested item that
violates the policy is diverted to the tree's [dead-letter
queue](dead-letter-queue.md) instead of being applied, so a bad item is neither
silently accepted nor allowed to stall the ingest stream.

Strict ingest requires **two** flags to line up, and takes effect only when both
are set:

- the **global** switch on the options (`StrictIngest = true`), which is what makes
  the interceptor inspect system-origin (replication apply / restore) writes at
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
change, no policy change. The build then rewrites every value into a fresh
destination tree, re-validating each one; an offender found at that stage aborts
the same way and the partial destination is discarded (soft-deleted). Only a fully
successful build cuts the logical tree over to the remediated destination: it
installs the target policy, then repoints the tree via physical-tree aliasing, then
arms a retained redirect that steers already-materialised readers to the new data.

The build copies at the logical level and does not shadow-forward writes that land
on the source while it runs, so run a remediation while the tree is
write-quiescent: a write accepted after the dry-run scan but before cutover is not
carried into the destination and is superseded by the alias swap.

Remediation is idempotent and resumable: it persists its intent and each phase
transition before acting, so re-issuing the same `RemediateAsync` call (the same
transform and target policy) after a silo failover resumes from the last persisted
phase. Nothing resumes it on its own - an interrupted remediation stays in flight
until it is requested again - and a call with different parameters while one is in
flight throws `InvalidOperationException`.

`RemediateAsync` drives the remediation to a terminal state before it returns its
`LatticeSchemaRemediationReport`; poll a running or last-known remediation with
`ILatticeSchemaRemediationAdmin.GetRemediationStatusAsync`. The report carries the
`Phase` (`Idle`, `DryRun`, `Build`, `Cutover`, `Completed`, or `Aborted`, with
`Succeeded` and `DidAbort` as shorthands), `InProgress`, `ScannedCount`,
`DestinationTreeId`, and `OperationId`, and - on an abort - the first
`OffendingKey`, the `Reason` (the policy violation, or the transform's failure
message), and `OffendingValuePreview`: at most `DeadLetterPreviewMaxBytes` leading
bytes of the transformed value, or of the original value when the transform itself
threw.

To measure compliance without rewriting anything, call
`ILatticeSchemaComplianceAdmin.ScanComplianceAsync(treeId, cancellationToken)`. It is a
pure read on ordinary read authority: it scans every current value against the tree's
current compiled policy and returns a `LatticeSchemaComplianceReport` carrying
the audited `TreeId`, `HasPolicy`, `CompliantCount`, `NonCompliantCount`,
`ScannedCount`, and a `RuleBreakdown` of `LatticeSchemaComplianceRuleCount`
(`Reason`, `Count`) rows, grouped by the reason of the first rule each
non-compliant value failed. An ungoverned tree returns an ungoverned report
(`HasPolicy` is `false` and every count is zero).

## Composition with versioning

When a tree uses both enforcement and [versioning](schema-versioning.md), a value
is validated against its **target (post-upcast) shape**, since that is the
compliant form: on the write path the enforcement stage validates the plain value
before the versioning stage wraps it in the envelope. Advancing the target version
and re-stamping existing values (`AdvanceAndMigrateAsync`) is a single shadow
build: upcast each value, validate it against the tree's **existing** policy, cut
over, aborting on the first offending key. The migration leaves the policy
unchanged, so tightening the policy is the separate remediation above.

## See also

- [Value transforms](value-transforms.md) - the transform IR remediation applies.
- [Dead-letter queue](dead-letter-queue.md) - where strict-mode diversions go.
- [Schema versioning](schema-versioning.md) - the sibling capability.
