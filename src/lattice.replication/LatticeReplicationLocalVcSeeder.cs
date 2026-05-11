using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationLocalVcSeeder"/> implementation.
/// Walks every shard of the named tree via
/// <see cref="IShardRootGrain.GetLeftmostLeafIdAsync"/> +
/// <see cref="IBPlusLeafGrain.GetLiveRawEntriesAsync"/> +
/// <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/>, accumulates the
/// pointwise-max <see cref="VersionVector"/> across every non-null
/// <see cref="LwwEntry.VectorClock"/> slot, then pins the computed
/// frontier on the per-tree
/// <see cref="IReplicationHighWaterMarkGrain"/> and primes the
/// producer-side <see cref="LocalVectorClockCache"/>.
/// <para>
/// The walk is shard-sequential; each shard''s leaf chain is walked
/// in-order via the leaf-sibling pointers. The seeder takes a single
/// <see cref="VersionVector"/> as the in-memory accumulator and merges
/// each leaf''s entries' VC slots in-place via
/// <see cref="VersionVector.MergeFrom"/> - allocation-free per leaf.
/// </para>
/// </summary>
internal sealed class LatticeReplicationLocalVcSeeder(
    IGrainFactory grainFactory,
    IShardCountProvider shardCounts,
    ILatticeMergeModeResolver modeResolver,
    LocalVectorClockCache localVcCache)
    : IReplicationLocalVcSeeder
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IShardCountProvider _shardCounts =
        shardCounts ?? throw new ArgumentNullException(nameof(shardCounts));
    private readonly ILatticeMergeModeResolver _modeResolver =
        modeResolver ?? throw new ArgumentNullException(nameof(modeResolver));
    private readonly LocalVectorClockCache _localVcCache =
        localVcCache ?? throw new ArgumentNullException(nameof(localVcCache));

    /// <inheritdoc />
    public async Task<LocalVcSeedReport> SeedFromTreeAsync(
        string treeName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        cancellationToken.ThrowIfCancellationRequested();

        // No-op for trees not configured for replication. The seeder
        // is a no-cost call against a non-replicated tree so admin
        // tooling can run it indiscriminately across every restored
        // tree without first inspecting the replication-mode map.
        if (_modeResolver.Resolve(treeName) is null)
        {
            return new LocalVcSeedReport(
                TreeName: treeName,
                Frontier: null,
                EntriesScanned: 0,
                SeedApplied: false);
        }

        var shardCount = await _shardCounts.GetShardCountAsync(treeName, cancellationToken).ConfigureAwait(false);

        // Accumulator: pointwise-max across every non-null
        // VectorClock slot encountered during the walk. MergeFrom
        // is in-place pointwise-max, so the loop body is
        // allocation-free per leaf.
        var frontier = new VersionVector();
        long entriesScanned = 0;

        for (var shardIndex = 0; shardIndex < shardCount; shardIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var shardKey = $"{treeName}/{shardIndex}";
            var shard = _grainFactory.GetGrain<IShardRootGrain>(shardKey);
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(false);

            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var leaf = _grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                var liveRaw = await leaf.GetLiveRawEntriesAsync().ConfigureAwait(false);

                foreach (var entry in liveRaw)
                {
                    entriesScanned++;
                    if (entry.VectorClock is { } vc)
                    {
                        frontier.MergeFrom(vc);
                    }
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(false);
            }
        }

        // Durable seed: pin the computed frontier on the per-tree
        // HWM grain. asOfHlc is HybridLogicalClock.Zero because
        // the intra-cluster path has no cross-cluster snapshot HLC
        // concept. The grain's PinSnapshotAsync contract preserves
        // asOfHlc verbatim in the call shape but does not consult
        // it; the frontier is the authoritative new vector.
        var hwmGrain = _grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeName);
        await hwmGrain.PinSnapshotAsync(HybridLogicalClock.Zero, frontier, cancellationToken).ConfigureAwait(false);

        // In-memory prime: advance the producer-side cache per-origin
        // so post-restore outbound emits read the seeded frontier
        // without waiting for a fresh
        // IReplicationHighWaterMarkGrain.GetVectorAsync cold-start.
        // The cache is the producer-side counterpart to the
        // receiver-side HWM grain; both must reflect the seeded
        // frontier for the silo to be in a consistent post-restore
        // state.
        foreach (var (origin, hlc) in frontier.Entries)
        {
            _localVcCache.AdvanceForeign(treeName, origin, hlc);
        }

        return new LocalVcSeedReport(
            TreeName: treeName,
            Frontier: frontier.Clone(),
            EntriesScanned: entriesScanned,
            SeedApplied: true);
    }
}