---
agent_spec: "docs/agents/deployment.yaml"
---

# Storage pressure

The storage axis reports whether any write-ahead-log storage account is hot and,
when one is, recommends spreading its partitions across more accounts. It is
**signal-only**: acting on a recommendation means calling the `ILatticeAdmin`
WAL-move surface yourself. Nothing on this axis ever changes the compute
`ScaleValue`.

## What it measures

`StoragePressure` is a cluster-aggregate snapshot with:

- `OverThreshold` - `true` when budgeted retained WAL bytes have crossed the
  configured threshold (the advisory fraction of the summed per-tree
  `WalMaxRetainedBytes` ceilings). Each tree contributes its own resolved
  ceiling to that sum rather than one silo-wide value, and the comparison is
  made once over the summed bytes and ceilings, not tree by tree; a tree with
  no ceiling contributes neither bytes nor budget to the comparison.
- `WalRetainedBytes` - total retained WAL bytes across every catalogue key,
  over the trees whose figure was complete (see `ExcludedPartialTrees`).
- `Accounts` - a `WalAccountPressure` per `IWalStorageProviderCatalog` key that
  backs a WAL partition, ordered by key (never `null`; empty when nothing is
  tracked).
- `Recommendation` - an optional `WalRebalanceRecommendation`, or `null` when no
  rebalance is warranted or `StorageRecommendationsEnabled` is `false`.
- `ExcludedPartialTrees` - how many trees were left out because their
  storage-usage report was partial, so its byte figure is a lower bound: a WAL
  partition or shard root did not answer (for a WAL partition, with no recent
  figure to stand in), a storage surface does not support byte accounting, or
  the cluster roll-up could not reach the tree at all. Weighing such a tree
  would understate the pressure on exactly the account too busy to answer, so
  its bytes and ceiling are left out of every byte and capacity comparison; its
  saturation still counts toward throughput classification (issue #6017).

## Throughput-bound versus capacity-bound

Each account is classified by `WalPressureClassification`, because the two kinds
of pressure have different remedies:

| Classification | What it means | The remedy |
|---|---|---|
| `None` | Healthy: neither backend-saturated nor over the retained-bytes threshold. | Nothing to do. |
| `ThroughputBound` | A single hot account has topped out its backend write rate (its per-tree `WalSaturationState` is `Throttled` or `Saturated`, in practice around 22-24 thousand entries per second for one storage account) continuously for `AccountSaturationWindow`. | Spread the account's hot partitions across more accounts (a WAL move). Adding retention headroom does not help. |
| `CapacityBound` | Retained WAL bytes have grown past `RetainedBytesAdvisoryRatio` of the account's budget - the sum of the effective per-tree `WalMaxRetainedBytes` ceilings of the trees holding partitions there. | Reclaim retained bytes or provision more retention. Spreading throughput does not help. |

An account that is both saturated for the window and over its retained-bytes
threshold is classified `ThroughputBound`; its `OverThreshold` flag still reads
`true`.

The saturation input is the answering silo's own WAL saturation signal, read
per tree under the id the tree's WAL is written under: the collector resolves
each tree's physical tree id (a fresh routing read) and looks the signal up by
that id, so a tree that a resize, a restore or an alias moved onto a physical
copy reads its copy's saturation rather than an always-`Healthy` logical name.
If that resolve fails, the collector reads the signal under the tree's own id.
The answering silo tracks how long each tree has stayed
non-`Healthy` across its own samples; an account takes the worst state and the
longest such run among the trees with partitions on it. Two silos can therefore
classify the same account differently.

`WalAccountPressure.OverThreshold` is the capacity-bound trigger specifically -
`true` when that account's budgeted retained bytes crossed the advisory fraction
of its budget. The budget is the sum of the effective per-tree ceilings
attributed to that account, so each tree's own ceiling counts rather than one
silo-wide number: trees on different accounts cross at different points, while
trees sharing an account are weighed together against their summed budget.

## The rebalance recommendation

When a hot account is found and recommendations are enabled, the collector emits
one `WalRebalanceRecommendation`. A throughput-bound account takes precedence
over a capacity-bound one; among throughput-bound accounts the worst saturation
wins, then the most retained bytes, and among capacity-bound accounts the most
retained bytes, with ties broken by key. The target is the first registered key
(in ordinal order) other than the hot one whose account is classified `None`,
or that backs no partition at all. The recommendation names:

- `Tree` and `Partition` - the WAL partition to relocate: a partition on the
  hot account, preferring one whose tree is not `Healthy` when the account is
  throughput-bound.
- `CurrentProviderKey` - the catalogue key that backs it today.
- `TargetProviderKey` - a registered key with spare headroom to accept it, when
  `HasHeadroom` is `true`.
- `HasHeadroom` - `false` when every registered key is already hot; then
  `TargetProviderKey` is empty and the remedy is to provision or register another
  account before any move can help.
- `Rationale` - a human-readable explanation of why the move is recommended.
- `Classification` - whether the current account is throughput-bound or
  capacity-bound, so you know whether a move (spread throughput) or added
  retention will actually help.

## Acting on a recommendation

The collector never moves anything. To act on a recommendation, drive the
`ILatticeAdmin` move workflow:

```csharp verify
using System.Threading;
using Orleans.Lattice;
using Orleans.Lattice.Scaling;

async Task RebalanceAsync(
    ILatticeScalingSignal signal,
    ILatticeAdmin admin,
    CancellationToken cancellationToken)
{
    ScalingSignal current = await signal.GetScalingSignalAsync(cancellationToken);
    if (current.Storage.Recommendation is not { HasHeadroom: true } recommendation)
    {
        return; // nothing actionable, or every account is already hot
    }

    // 1. Plan the move to the recommended target account.
    _ = await admin.PlanWalMoveAsync(
        recommendation.Tree,
        recommendation.Partition,
        recommendation.TargetProviderKey,
        cancellationToken);

    // 2. Execute it.
    _ = await admin.ExecuteWalMoveAsync(
        recommendation.Tree,
        recommendation.Partition,
        recommendation.TargetProviderKey,
        options: null,
        cancellationToken);

    // 3. Later, reclaim the vacated source once the move has settled.
    _ = await admin.ReclaimMovedWalSourceAsync(
        recommendation.Tree,
        recommendation.Partition,
        recommendation.CurrentProviderKey,
        cancellationToken);
}
```

When `HasHeadroom` is `false`, register another WAL storage account under a new
key with `AddLatticeWalStorageProvider` on every silo first - the
`IWalStorageProviderCatalog` has no runtime registration method and lists the
keys registered at startup, and every silo must register the same set - then
re-read the signal: the next recommendation will name the new account as the
target.

## Turning it off

Set `StorageRecommendationsEnabled = false` to keep the `OverThreshold` flag and
the per-account breakdown while suppressing the `Recommendation` (it stays
`null`). This is useful when an external system owns WAL placement and you only
want the storage axis for observability.
