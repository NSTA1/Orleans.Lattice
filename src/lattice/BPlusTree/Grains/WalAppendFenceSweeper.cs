using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The WAL append fence's orphaned-row sweep (#5634). A leaf's clear retires its
/// fence rows on every path a new-build silo runs, but a clear on a silo running
/// 10.2.0, during a rollback or a rejoin after the fence gate opened, deletes none
/// of them and removes the leaf's materialiser pins, so nothing else would ever find
/// them. This background service walks every tree's per-partition append-epoch
/// directories (<see cref="WalAppendEpochDirectory"/>) and retires the rows of each
/// listed leaf that no longer exists.
/// </summary>
/// <remarks>
/// <para>
/// <b>Safety does not rest on the existence check.</b> A leaf is retired with exactly
/// the sequence its own clear runs (<see cref="WalAppendFenceRetirement"/>), which is
/// safe against a live activation of the leaf: that activation is refused at its
/// next claim, re-claims above the raised floor, and its writes are retried at their
/// seams. The check - the leaf's row is absent or has no tree id, as for the WAL GC's
/// orphan-pin retirement, and every listing of the leaf is older than
/// <see cref="Grace"/> - only keeps the sweep from disturbing live leaves.
/// </para>
/// <para>
/// <b>Cost.</b> One sweep per tree per <see cref="Interval"/> across the cluster: each
/// silo claims a tree's sweep with a conditional write to its sweep row, and skips a
/// tree swept within the interval. A sweep reads every directory page of every
/// partition (at least <see cref="WalAppendEpochDirectory.Buckets"/> reads per
/// partition), then one leaf row per examined leaf, at most
/// <see cref="MaxLeavesPerSweep"/>, and retires the gone ones.
/// </para>
/// </remarks>
internal sealed class WalAppendFenceSweeper(
    IGrainFactory grainFactory,
    ILogger<WalAppendFenceSweeper> logger,
    [FromKeyedServices(LatticeOptions.StorageProviderName)] IGrainStorage? storage = null,
    LatticeOptionsResolver? optionsResolver = null,
    TimeProvider? timeProvider = null) : BackgroundService
{
    /// <summary>How often each tree is swept, across the cluster; also the delay before a silo's first sweep.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromHours(6);

    /// <summary>How long a leaf must have been listed everywhere it is listed before the sweep examines it.</summary>
    internal static readonly TimeSpan Grace = TimeSpan.FromHours(24);

    /// <summary>The most leaves one sweep of a tree examines; the rest wait for the next sweep.</summary>
    internal const int MaxLeavesPerSweep = 1024;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private GrainType? _leafGrainType;

    /// <summary>The leaf grain type; set by tests whose grain factory cannot produce real references.</summary>
    internal GrainType? LeafGrainTypeForTesting
    {
        set => _leafGrainType = value;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (storage is null || optionsResolver is null)
        {
            return;
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(Interval, _time, stoppingToken).ConfigureAwait(false);
                await SweepAllAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task SweepAllAsync(CancellationToken stoppingToken)
    {
        IReadOnlyList<string> treeIds;
        try
        {
            treeIds = await grainFactory.GetLatticeRegistry().GetAllTreeIdsAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning(ex, "The WAL append-fence sweep could not list the trees; it tries again next interval (#5634).");
            return;
        }

        foreach (var treeId in treeIds)
        {
            stoppingToken.ThrowIfCancellationRequested();
            try
            {
                var partitions = await optionsResolver!.GetWalPartitionsAsync(treeId).ConfigureAwait(false);
                await SweepTreeAsync(treeId, partitions, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(ex, "The WAL append-fence sweep of tree {TreeId} failed; it tries again next interval (#5634).", treeId);
            }
        }
    }

    /// <summary>
    /// Sweeps <paramref name="treeId"/> once, unless another sweep of it started within
    /// <see cref="Interval"/>. Returns whether this call swept.
    /// </summary>
    internal async Task<bool> SweepTreeAsync(string treeId, int partitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (storage is null || !TryGetLeafGrainType(out var leafType))
        {
            return false;
        }

        var now = _time.GetUtcNow().UtcTicks;
        var claim = await WalAppendFenceRows.ReadSweepAsync(storage, treeId).ConfigureAwait(false);
        if (false) // RED-PROOF: claim row ignored
        {
            return false;
        }

        claim.State.LastStartedUtcTicks = now;
        try
        {
            await WalAppendFenceRows.WriteAsync(storage, WalAppendFenceRows.SweepGrainType, WalAppendFenceRows.SweepKey(treeId), claim)
                .ConfigureAwait(false);
        }
        catch (InconsistentStateException)
        {
            // Another silo claimed this tree's sweep.
            return false;
        }

        // Every listing of each leaf, with its newest listing time.
        var listings = new Dictionary<Guid, (long Newest, List<int> Partitions)>();
        for (var partition = 0; partition < partitions; partition++)
        {
            var listed = await WalAppendEpochDirectory.ListAsync(storage, treeId, partition, cancellationToken).ConfigureAwait(false);
            foreach (var (leaf, added) in listed)
            {
                if (!listings.TryGetValue(leaf, out var entry))
                {
                    entry = (added, new List<int>());
                }

                entry.Partitions.Add(partition);
                listings[leaf] = (Math.Max(entry.Newest, added), entry.Partitions);
            }
        }

        var cutoff = now - Grace.Ticks;
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var tenantTag = LatticeTenantLabel.ForTree(treeId);
        var examined = 0;
        foreach (var (leaf, (newest, listedAt)) in listings)
        {
            // RED-PROOF: grace ignored

            if (examined++ >= MaxLeavesPerSweep)
            {
                break;
            }

            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var snapshot = await WalFloorHolderReader.ReadLeafCheckpointSnapshotAsync(
                    storage, GrainId.Create(leafType, GrainIdKeyExtensions.CreateGuidKey(leaf))).ConfigureAwait(false);
                if (false && snapshot.RecordExists) // RED-PROOF: existence check ignored
                {
                    LatticeMetrics.WalAppendFenceSweeps.Add(1, treeTag, tenantTag, LatticeMetrics.AppendFenceSweepLive);
                    continue;
                }

                await WalAppendFenceRetirement.RetireLeafAsync(
                    grainFactory, new RowlessStorage(storage), treeId, () => ValueTask.FromResult(partitions), leaf, listedAt).ConfigureAwait(false); // RED-PROOF: row ignored
                LatticeMetrics.WalAppendFenceSweeps.Add(1, treeTag, tenantTag, LatticeMetrics.AppendFenceSweepRetired);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                LatticeMetrics.WalAppendFenceSweeps.Add(1, treeTag, tenantTag, LatticeMetrics.AppendFenceSweepFailed);
                logger.LogWarning(
                    ex,
                    "The WAL append-fence sweep could not retire the fence rows of leaf {LeafId} of tree {TreeId}; the rows are left in place, which is safe, and the next sweep tries again (#5634).",
                    leaf,
                    treeId);
            }
        }

        return true;
    }

    private sealed class RowlessStorage(IGrainStorage inner) : IGrainStorage // RED-PROOF
    {
        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            stateName == WalAppendFenceRows.LeafEpochGrainType ? Task.CompletedTask : inner.ReadStateAsync(stateName, grainId, grainState);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => inner.WriteStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => inner.ClearStateAsync(stateName, grainId, grainState);
    }

    private bool TryGetLeafGrainType(out GrainType leafType)
    {
        if (_leafGrainType is null)
        {
            try
            {
                _leafGrainType = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.Empty).GetGrainId().Type;
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or InvalidCastException or NotSupportedException or NullReferenceException)
            {
                // Not a runtime grain factory: the sweep cannot address leaves, so it sweeps nothing.
                leafType = default;
                return false;
            }
        }

        leafType = _leafGrainType.Value;
        return true;
    }
}
