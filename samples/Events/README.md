# Events

## What it shows

Orleans.Lattice publishes **metadata-only event notifications** on a per-tree
Orleans stream, so caches, projections, audit pipelines, and dashboards can react
to mutations without polling. This sample enables publication, subscribes to a
tree's event stream, performs four writes (three sets and a delete), and prints
each `LatticeTreeEvent` as it arrives. Events carry only the key name and
operation kind - never the value bytes - so a subscriber that needs the new value
issues its own `GetAsync`.

## Run it

```
dotnet run --project samples/Events
```

## Expected output

```
Silo starting... ready.

Subscribed to stream 'orleans.lattice.events' for tree 'catalog'.

Performing writes:
  set    sku-1
  set    sku-2
  set    sku-1 (update)
  delete sku-2

Received 4 event(s):
  Set      tree=catalog key=sku-1 shard=-
  Set      tree=catalog key=sku-2 shard=-
  Set      tree=catalog key=sku-1 shard=-
  Delete   tree=catalog key=sku-2 shard=-
```

## When to use

- Out-of-process, fire-and-forget reactions to tree mutations: cache
  invalidation, UI refreshes, dashboards, or feeding an audit projection.
- You only need to know *what* changed (key + kind), not the new bytes.

## When not to use

- When you need the full value on the write path synchronously - use an
  [`IMutationObserver`](../../docs/lattice/api.md#mutation-observers) instead
  (in-process, value-carrying, but it adds latency to every write).
- As a durable change log: events are best-effort and not persisted. A silo
  crash between the write and the publish loses the event; the write survives.
  Use a durable stream provider (EventHubs, AzureQueue) for stronger delivery.

## Notes

- Lattice never registers a stream provider for you. This sample adds
  `AddMemoryStreams("Default")` plus its `PubSubStore` and names the provider in
  `EventStreamProviderName`. Memory streams are at-most-once per activation; for
  at-least-once delivery use a durable provider.

## Feature doc

- [Tree Events](../../docs/lattice/events.md)
