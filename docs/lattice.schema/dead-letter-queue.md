# Dead-letter queue

The dead-letter queue (DLQ) is where a tree's schema machinery parks a rejected
system-origin item instead of applying it to the governed tree. After the
append succeeds, the interceptor returns a dead-letter decision rather than a
schema-violation exception, so the offending value need not be repaired before
other ingest can proceed. This is not an unconditional liveness guarantee:
the store write is awaited, and storage failure or cancellation propagates to
the intercepted write. Diversion does not acknowledge an item that could not
be recorded. An operator inspects the recorded item out of band. Only
ingest that reaches a tree's write operations can land here, which in practice is
a replicated typed-CRDT entry or an entry of a replicated atomic batch; a plain
(non-atomic) last-writer-wins replication apply and a backup restore write below
the strict check and never produce an entry (see
[strict-mode ingest](schema-enforcement.md#strict-mode-ingest)). A dead-lettered
entry of a replicated atomic batch is left out of that batch, and the receiver
commits the batch's other entries.

## What lands in the DLQ

An entry is created only in [strict-ingest](schema-enforcement.md#strict-mode-ingest)
mode. Each entry records the offending key, a bounded preview of the value
(capped by the writing add-on's `DeadLetterPreviewMaxBytes` option), the full byte
length, a human-readable reason, the source, and a UTC timestamp. The source is one
of:

| `LatticeSchemaDeadLetterSource` | Meaning |
|---|---|
| `Replication` | A system-origin ingested item other than a bulk-load item - in practice a replicated typed-CRDT delta or full-state row, or an entry of a replicated atomic batch - failed strict validation. |
| `Restore` | A bulk-load item (`BulkLoadAsync` or `BulkAppendChunkAsync`) arriving as system-origin ingest failed enforcement's strict validation. No shipped ingest path issues one - a backup restore writes below the strict check - so in practice this source does not occur. |
| `LocalRejected` | Reserved for a rejected local write retained for inspection. Not produced by the current release, which fails local writes closed (see below). |

The `Restore` source is assigned only by enforcement. An item the versioning stage
dead-letters (a version that is newer than the target or cannot be upcast) is
always recorded with the `Replication` source, even when it arrived through a
bulk load.

A direct local write that violates a policy fails closed: it is *rejected* to the
caller with `LatticeSchemaViolationException` and nothing is made durable. The
rejected value is **not** mirrored to the DLQ. Only system-origin ingest that
reaches a tree's write operations lands entries here, so in the current release
every entry carries the `Replication` or `Restore` source. `LocalRejected` is a
reserved source for a future opt-in that would also retain the rejected local
value; no code path produces it today.

## Reading it from the schema admin

The in-process `ILatticeSchemaAdmin` exposes the queue directly. It performs no
authorization of its own; the [schema API facade](../lattice.api.schema/README.md)
authorizes remote reads of the queue on Read authority (see
[Capability gate](README.md#capability-gate)):

```csharp verify
using Orleans.Lattice.Schema;

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaAdmin>();

int count = await admin.CountDeadLettersAsync("orders", cancellationToken);

await foreach (var entry in admin.ListDeadLettersAsync("orders", cancellationToken))
{
    Console.WriteLine($"{entry.TimestampUtc:o} {entry.Source} '{entry.Key}': {entry.Reason}");
}
```

## Reading it through the State API

The read-only [cluster State API](../lattice.api.state/README.md) surfaces the same
queue for dashboards and the Explorer, paginated and subject to the API's tree
read-visibility gate. Its read-only query surface (`ILatticeStateQuery`) exposes `GetDeadLetterCountAsync`
and a paginated `ListDeadLettersAsync` that takes a `DeadLetterQueueRequest`
(`TreeId`, `PageSize`, `PageToken`) and returns a `DeadLetterQueuePage` - a list of
`DeadLetterEntryRecord` plus a `NextPageToken` for the next page. Each record
carries the offending `Key`, a bounded `ValuePreview` (with `PreviewTruncated` and
the full `ValueByteLength`), the `Reason`, the `Source` (a `DeadLetterSourceKind`),
and `TimestampUtc`.

The DLQ store (`ILatticeSchemaDeadLetterStore`, registered by either schema
add-on) is an **optional** dependency: if the schema package is not installed,
the count is zero and the page is empty rather than an error, as they also are
for a caller that may not read the tree. The
bundled Explorer app renders this page as a per-tree DLQ panel, and the gRPC State
API binding projects the same records over the wire.

## Scope

The current release surfaces the queue read-only: list, count, and inspect. Replay
/ requeue of a dead-lettered item and retention / cap policies are documented
follow-ups, not part of this release.

## See also

- [Schema enforcement](schema-enforcement.md) - strict ingest is what fills the queue.
- [Schema versioning](schema-versioning.md) - un-upcastable ingest is dead-lettered too.
