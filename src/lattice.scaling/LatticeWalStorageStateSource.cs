using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Production <see cref="IWalStorageStateSource"/>: assembles the cluster-aggregate
/// WAL storage sample from the core administrative surface. It reads the per-tree
/// retained-byte roll-up and tree set from
/// <see cref="Orleans.Lattice.ILatticeAdmin.GetTotalStorageUsageAsync(System.Threading.CancellationToken)"/>
/// (which reads each shard root's O(1) incrementally-maintained byte totals and
/// never walks a leaf chain), the per-tree partition placement from
/// <see cref="Orleans.Lattice.ILatticeAdmin.GetWalPlacementAsync(string, System.Threading.CancellationToken)"/>,
/// the per-tree backend saturation regime from
/// <see cref="Orleans.Lattice.IWalSaturationSignal"/>, and the registered
/// catalogue keys from <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/>.
/// <para>
/// It tracks how long each tree has been continuously saturated across ticks so
/// the collector can debounce a transient blip via
/// <see cref="LatticeScalingSignalOptions.AccountSaturationWindow"/>. When no grain
/// factory is available (the package added outside a silo, as in a bare unit-test
/// container) or the administrative call fails, it degrades to an empty sample
/// rather than throwing, so a scrape still yields a well-formed zero storage
/// pressure. Runs only on the facade's sample timer, never per scrape.
/// </para>
/// </summary>
internal sealed class LatticeWalStorageStateSource(
    TimeProvider timeProvider,
    IWalStorageProviderCatalog? catalog = null,
    IWalSaturationSignal? saturationSignal = null,
    IGrainFactory? grainFactory = null,
    ILogger<LatticeWalStorageStateSource>? logger = null) : IWalStorageStateSource
{
    // LatticeConstants.AdminGrainKey is internal to the core assembly; the literal
    // is mirrored here (the single cluster-wide admin grain key) because the
    // scaling package is not on core's InternalsVisibleTo list.
    private const string AdminGrainKey = "_lattice_admin";

    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly IWalStorageProviderCatalog? _catalog = catalog;
    private readonly IWalSaturationSignal? _saturationSignal = saturationSignal;
    private readonly IGrainFactory? _grainFactory = grainFactory;
    private readonly ILogger _logger = logger ?? NullLogger<LatticeWalStorageStateSource>.Instance;

    // Continuity tracking: first time each tree was observed non-healthy. Accessed
    // only from the single sampling timer, so no synchronisation is required.
    private readonly Dictionary<string, DateTimeOffset> _saturatedSince = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async ValueTask<WalStorageSample> SampleAsync(CancellationToken cancellationToken)
    {
        var admin = _grainFactory?.GetGrain<ILatticeAdmin>(AdminGrainKey);
        if (admin is null)
        {
            return default;
        }

        try
        {
            var usage = await admin.GetTotalStorageUsageAsync(cancellationToken).ConfigureAwait(false);
            var perTree = usage.Trees;
            if (perTree.IsDefaultOrEmpty)
            {
                PruneContinuity(Array.Empty<string>());
                return new WalStorageSample { CatalogKeys = SnapshotCatalogKeys() };
            }

            var now = _timeProvider.GetUtcNow();
            var trees = new WalTreeSample[perTree.Length];
            var liveTreeIds = new string[perTree.Length];
            for (var i = 0; i < perTree.Length; i++)
            {
                var report = perTree[i];
                cancellationToken.ThrowIfCancellationRequested();
                liveTreeIds[i] = report.TreeId;

                var placement = await admin.GetWalPlacementAsync(report.TreeId, cancellationToken).ConfigureAwait(false);
                var partitions = MapPartitions(placement);

                var saturation = _saturationSignal?.GetCurrentState(report.TreeId) ?? WalSaturationState.Healthy;
                var saturatedFor = TrackContinuity(report.TreeId, saturation, now);

                trees[i] = new WalTreeSample
                {
                    TreeId = report.TreeId,
                    WalRetainedBytes = report.WalRetainedBytes,
                    Partial = report.Partial,
                    Saturation = saturation,
                    SaturatedFor = saturatedFor,
                    Partitions = partitions,
                };
            }

            PruneContinuity(liveTreeIds);

            return new WalStorageSample
            {
                Trees = trees,
                CatalogKeys = SnapshotCatalogKeys(),
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Failed to sample WAL storage state; reporting an empty sample.");
            return default;
        }
    }

    private static WalPartitionSample[] MapPartitions(WalPlacement placement)
    {
        var source = placement.Partitions;
        if (source.IsDefaultOrEmpty)
        {
            return Array.Empty<WalPartitionSample>();
        }

        var mapped = new WalPartitionSample[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            mapped[i] = new WalPartitionSample
            {
                Partition = source[i].Partition,
                ProviderKey = source[i].ProviderKey,
            };
        }

        return mapped;
    }

    private TimeSpan TrackContinuity(string treeId, WalSaturationState saturation, DateTimeOffset now)
    {
        if (saturation == WalSaturationState.Healthy)
        {
            _saturatedSince.Remove(treeId);
            return TimeSpan.Zero;
        }

        if (!_saturatedSince.TryGetValue(treeId, out var since))
        {
            _saturatedSince[treeId] = now;
            return TimeSpan.Zero;
        }

        var elapsed = now - since;
        return elapsed > TimeSpan.Zero ? elapsed : TimeSpan.Zero;
    }

    private void PruneContinuity(IReadOnlyList<string> liveTreeIds)
    {
        if (_saturatedSince.Count == 0)
        {
            return;
        }

        // Drop continuity entries for trees no longer present so the map cannot
        // grow unbounded as trees come and go.
        List<string>? stale = null;
        foreach (var tracked in _saturatedSince.Keys)
        {
            var present = false;
            for (var i = 0; i < liveTreeIds.Count; i++)
            {
                if (string.Equals(liveTreeIds[i], tracked, StringComparison.Ordinal))
                {
                    present = true;
                    break;
                }
            }

            if (!present)
            {
                (stale ??= new List<string>()).Add(tracked);
            }
        }

        if (stale is null)
        {
            return;
        }

        for (var i = 0; i < stale.Count; i++)
        {
            _saturatedSince.Remove(stale[i]);
        }
    }

    private string[] SnapshotCatalogKeys()
    {
        var keys = _catalog?.Keys;
        if (keys is null || keys.Count == 0)
        {
            return Array.Empty<string>();
        }

        var snapshot = new string[keys.Count];
        var i = 0;
        foreach (var key in keys)
        {
            snapshot[i++] = key;
        }

        return snapshot;
    }
}
