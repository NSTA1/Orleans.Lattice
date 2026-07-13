# Storage pressure

The storage axis reports whether any write-ahead-log storage account is hot and,
when one is, recommends spreading its partitions across more accounts. It is
**signal-only**: acting on a recommendation means calling the `ILatticeAdmin`
WAL-move surface yourself. Nothing on this axis ever changes the compute
`ScaleValue`.

## What it measures

`StoragePressure` is a cluster-aggregate snapshot with:

- `OverThreshold` - `true` when aggregate retained WAL bytes have crossed the
  configured threshold (the advisory fraction of `LatticeOptions.WalMaxRetainedBytes`).
- `WalRetainedBytes` - total retained WAL bytes across every catalogue key.
- `Accounts` - a `WalAccountPressure` per `IWalStorageProviderCatalog` key that
  backs a WAL partition (never `null`; empty when nothing is tracked).
- `Recommendation` - an optional `WalRebalanceRecommendation`, or `null` when no
  rebalance is warranted or `StorageRecommendationsEnabled` is `false`.

## Throughput-bound versus capacity-bound

Each account is classified by `WalPressureClassification`, because the two kinds
of pressure have different remedies:

| Classification | What it means | The remedy |
|---|---|---|
| `None` | Healthy: neither backend-saturated nor over the retained-bytes threshold. | Nothing to do. |
| `ThroughputBound` | A single hot account has topped out its backend write rate (its per-tree `WalSaturationState` is `Throttled` or `Saturated`, in practice around 22-24 thousand entries per second for one storage account) continuously for `AccountSaturationWindow`. | Spread the account's hot partitions across more accounts (a WAL move). Adding retention headroom does not help. |
| `CapacityBound` | Retained WAL bytes have grown past `RetainedBytesAdvisoryRatio` of `WalMaxRetainedBytes`. | Reclaim retained bytes or provision more retention. Spreading throughput does not help. |

`WalAccountPressure.OverThreshold` is the capacity-bound trigger specifically -
`true` when that account's retained bytes crossed the advisory fraction.

## The rebalance recommendation

When a hot account is found and recommendations are enabled, the collector emits
one `WalRebalanceRecommendation` naming:

- `Tree` and `Partition` - the WAL partition to relocate.
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

When `HasHeadroom` is `false`, register another WAL storage account with the
`IWalStorageProviderCatalog` first, then re-read the signal - the next
recommendation will name the new account as the target.

## Turning it off

Set `StorageRecommendationsEnabled = false` to keep the `OverThreshold` flag and
the per-account breakdown while suppressing the `Recommendation` (it stays
`null`). This is useful when an external system owns WAL placement and you only
want the storage axis for observability.
