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

## Setting a policy on a tree

A policy is an ordered set of [rules](#rule-kinds). Install one with the
`SchemaAdmin`-gated `ILatticeSchemaAdmin` service:

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
`LatticeSchemaViolationException` and is never persisted.

## Rule kinds

A `LatticeSchemaRule` is created with one of these factories; a policy validates a
value against every rule it carries, in order:

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
change, no policy change. Only a fully successful build cuts the logical tree over
to the remediated destination (via physical-tree aliasing and a retained-redirect
that steers already-materialised readers to the new data) and installs the target
policy. Remediation is idempotent and survives a silo failover: it persists its
intent and resumes from the last phase on reactivation.

Poll a running or last-known remediation with
`ILatticeSchemaRemediationAdmin.GetRemediationStatusAsync`.

## Composition with versioning

When a tree uses both enforcement and [versioning](schema-versioning.md), a value
is validated against its **target (post-upcast) shape**, since that is the
compliant form. Advancing the target version and tightening the policy is a single
shadow build: upcast, validate against the new policy, cut over, aborting on the
first offending key.

## See also

- [Value transforms](value-transforms.md) - the transform IR remediation applies.
- [Dead-letter queue](dead-letter-queue.md) - where strict-mode diversions go.
- [Schema versioning](schema-versioning.md) - the sibling capability.
