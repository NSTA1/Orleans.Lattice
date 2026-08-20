# Dead-letter queue

The dead-letter queue (DLQ) is where a tree's schema machinery parks an item it
rejected *without failing the operation that produced it*. Its purpose is
fail-open ingest: a schema violation arriving over replication or restore, or a
value that cannot be upcast, must never stall the stream or crash the silo - it is
diverted here for an operator to inspect and act on out of band.

## What lands in the DLQ

An entry is created only in [strict-ingest](schema-enforcement.md#strict-mode-ingest)
mode. Each entry records the offending key, a bounded preview of the value
(capped by `DeadLetterPreviewMaxBytes`), the full byte length, a human-readable
reason, the source, and a UTC timestamp. The source is one of:

| `LatticeSchemaDeadLetterSource` | Meaning |
|---|---|
| `Replication` | A replicated apply from a peer cluster failed strict validation. |
| `Restore` | A backup restore item failed strict validation. |
| `LocalRejected` | Reserved for a rejected local write retained for inspection. Not produced by the current release, which fails local writes closed (see below). |

A direct local write that violates a policy fails closed: it is *rejected* to the
caller with `LatticeSchemaViolationException` and nothing is made durable. The
rejected value is **not** mirrored to the DLQ. Only system-origin ingest
(replication apply and restore) lands entries here, so in the current release
every entry carries the `Replication` or `Restore` source. `LocalRejected` is a
reserved source for a future opt-in that would also retain the rejected local
value; no code path produces it today.

## Reading it from the schema admin

The `SchemaAdmin`-gated `ILatticeSchemaAdmin` exposes the queue directly:

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
the full `ValueByteLength`), the `Reason`, a `DeadLetterSourceKind`, and
`TimestampUtc`.

The DLQ store is an **optional** dependency: if the schema package is not
installed, the count is zero and the page is empty rather than an error. The
bundled Explorer app renders this page as a per-tree DLQ panel, and the gRPC State
API binding projects the same records over the wire.

## Scope

The current release surfaces the queue read-only: list, count, and inspect. Replay
/ requeue of a dead-lettered item and retention / cap policies are documented
follow-ups, not part of this release.

## See also

- [Schema enforcement](schema-enforcement.md) - strict ingest is what fills the queue.
- [Schema versioning](schema-versioning.md) - un-upcastable ingest is dead-lettered too.
