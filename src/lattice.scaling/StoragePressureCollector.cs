using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Production <see cref="IStoragePressureCollector"/>: reduces the cluster-aggregate
/// WAL storage sample (<see cref="IWalStorageStateSource"/>) into a normalised
/// <see cref="StoragePressure"/>. Correlates per-partition WAL placement with
/// per-tree backend saturation and retained bytes to classify each catalogue-key
/// account as throughput-bound, capacity-bound, or healthy, and - when a hot
/// account is found - suggests a <see cref="WalRebalanceRecommendation"/> naming a
/// target key with headroom (or, when every registered key is hot, advising the
/// operator to provision another account).
/// <para>
/// This is strictly report-only. The storage axis it produces is carried through
/// the <see cref="ScalingSignalComputer"/> for observability but never inflates the
/// compute scale value; adding storage accounts, not silo replicas, is what
/// relieves storage pressure. The classification and recommendation logic is pure
/// over its <see cref="IWalStorageStateSource"/> input, so it is exercised
/// deterministically in unit tests with a substituted source.
/// </para>
/// </summary>
internal sealed class StoragePressureCollector(
    IWalStorageStateSource source,
    IOptions<LatticeScalingSignalOptions> scalingOptions,
    IOptions<LatticeOptions> latticeOptions,
    ILogger<StoragePressureCollector>? logger = null) : IStoragePressureCollector
{
    private readonly IWalStorageStateSource _source = source;
    private readonly IOptions<LatticeScalingSignalOptions> _scalingOptions = scalingOptions;
    private readonly IOptions<LatticeOptions> _latticeOptions = latticeOptions;
    private readonly ILogger _logger = logger ?? NullLogger<StoragePressureCollector>.Instance;

    /// <summary>
    /// Mutable per-account accumulator used only while reducing the sample. Held
    /// in a dictionary keyed by catalogue key; not a wire type.
    /// </summary>
    private struct Accumulator
    {
        public long RetainedBytes;
        public WalSaturationState WorstSaturation;
        public TimeSpan MaxSaturatedFor;
    }

    /// <inheritdoc />
    public async ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken)
    {
        WalStorageSample sample;
        try
        {
            sample = await _source.SampleAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "WAL storage sample failed; reporting a zero storage-pressure snapshot.");
            return default;
        }

        var trees = sample.Trees;
        if (trees.Count == 0)
        {
            return default;
        }

        var options = _scalingOptions.Value;
        var capacityThreshold = ResolveCapacityThreshold(options, _latticeOptions.Value.WalMaxRetainedBytes);
        var window = options.AccountSaturationWindow;

        // Reduce per-tree slices into per-account accumulators and the aggregate
        // retained-byte total. A struct-enumerator for/index loop avoids the LINQ
        // and iterator allocations a GroupBy would incur on this timer path.
        var accumulators = new Dictionary<string, Accumulator>(StringComparer.Ordinal);
        var aggregateBytes = 0L;
        for (var t = 0; t < trees.Count; t++)
        {
            var tree = trees[t];
            aggregateBytes += tree.WalRetainedBytes;

            var partitions = tree.Partitions;
            var count = partitions.Count;
            if (count == 0)
            {
                continue;
            }

            var baseBytes = tree.WalRetainedBytes / count;
            var remainder = tree.WalRetainedBytes % count;
            for (var p = 0; p < count; p++)
            {
                var partition = partitions[p];
                var key = partition.ProviderKey ?? IWalStorageProviderCatalog.DefaultProviderKey;
                var slice = baseBytes + (p == 0 ? remainder : 0);

                accumulators.TryGetValue(key, out var accum);
                accum.RetainedBytes += slice;
                if (tree.Saturation > accum.WorstSaturation)
                {
                    accum.WorstSaturation = tree.Saturation;
                }

                if (tree.SaturatedFor > accum.MaxSaturatedFor)
                {
                    accum.MaxSaturatedFor = tree.SaturatedFor;
                }

                accumulators[key] = accum;
            }
        }

        var accounts = BuildAccounts(accumulators, capacityThreshold, window);

        var overThreshold = capacityThreshold > 0 && aggregateBytes >= capacityThreshold;

        WalRebalanceRecommendation? recommendation = null;
        if (options.StorageRecommendationsEnabled)
        {
            recommendation = BuildRecommendation(accounts, trees, sample.CatalogKeys);
        }

        return new StoragePressure
        {
            OverThreshold = overThreshold,
            WalRetainedBytes = aggregateBytes,
            Accounts = accounts,
            Recommendation = recommendation,
        };
    }

    private static long ResolveCapacityThreshold(LatticeScalingSignalOptions options, long? configuredCeiling)
    {
        // Threshold lives on core LatticeOptions; a null/zero ceiling means "no
        // capacity classification". Ratio is clamped to (0, 1].
        if (configuredCeiling is not > 0)
        {
            return 0L;
        }

        var ratio = options.RetainedBytesAdvisoryRatio;
        if (ratio <= 0d)
        {
            ratio = LatticeScalingSignalOptions.DefaultRetainedBytesAdvisoryRatio;
        }
        else if (ratio > 1d)
        {
            ratio = 1d;
        }

        return (long)(configuredCeiling.Value * ratio);
    }

    private WalAccountPressure[] BuildAccounts(
        Dictionary<string, Accumulator> accumulators,
        long capacityThreshold,
        TimeSpan window)
    {
        if (accumulators.Count == 0)
        {
            return Array.Empty<WalAccountPressure>();
        }

        var accounts = new WalAccountPressure[accumulators.Count];
        var i = 0;
        foreach (var pair in accumulators)
        {
            var accum = pair.Value;
            var overThreshold = capacityThreshold > 0 && accum.RetainedBytes >= capacityThreshold;
            var classification = Classify(accum, overThreshold, window);
            accounts[i++] = new WalAccountPressure
            {
                ProviderKey = pair.Key,
                WalRetainedBytes = accum.RetainedBytes,
                Saturation = accum.WorstSaturation,
                Classification = classification,
                OverThreshold = overThreshold,
            };
        }

        // Deterministic ordinal ordering so the reported breakdown - and any test
        // asserting against it - is stable regardless of dictionary iteration order.
        Array.Sort(accounts, static (a, b) => string.CompareOrdinal(a.ProviderKey, b.ProviderKey));
        return accounts;
    }

    private static WalPressureClassification Classify(Accumulator accum, bool overThreshold, TimeSpan window)
    {
        var throughputBound =
            accum.WorstSaturation != WalSaturationState.Healthy &&
            (window <= TimeSpan.Zero || accum.MaxSaturatedFor >= window);

        if (throughputBound)
        {
            return WalPressureClassification.ThroughputBound;
        }

        return overThreshold ? WalPressureClassification.CapacityBound : WalPressureClassification.None;
    }

    private static WalRebalanceRecommendation? BuildRecommendation(
        WalAccountPressure[] accounts,
        IReadOnlyList<WalTreeSample> trees,
        IReadOnlyCollection<string> catalogKeys)
    {
        // Pick the hottest account: throughput-bound accounts win (the acute case),
        // ranked by saturation then retained bytes; otherwise the worst
        // capacity-bound account by retained bytes. Deterministic ordinal tie-break.
        var hotIndex = -1;
        var hotIsThroughput = false;
        for (var i = 0; i < accounts.Length; i++)
        {
            var account = accounts[i];
            var isThroughput = account.Classification == WalPressureClassification.ThroughputBound;
            var isCapacity = account.Classification == WalPressureClassification.CapacityBound;
            if (!isThroughput && !isCapacity)
            {
                continue;
            }

            if (hotIndex < 0)
            {
                hotIndex = i;
                hotIsThroughput = isThroughput;
                continue;
            }

            if (isThroughput && !hotIsThroughput)
            {
                hotIndex = i;
                hotIsThroughput = true;
                continue;
            }

            if (isThroughput == hotIsThroughput && IsHotter(account, accounts[hotIndex], isThroughput))
            {
                hotIndex = i;
            }
        }

        if (hotIndex < 0)
        {
            return null;
        }

        var hot = accounts[hotIndex];
        var classification = hot.Classification;
        var (tree, partition) = FindHotPartition(trees, hot.ProviderKey, hotIsThroughput);

        var target = SelectTargetWithHeadroom(accounts, catalogKeys, hot.ProviderKey);
        if (target is not null)
        {
            return new WalRebalanceRecommendation
            {
                Tree = tree,
                Partition = partition,
                CurrentProviderKey = hot.ProviderKey,
                TargetProviderKey = target,
                HasHeadroom = true,
                Classification = classification,
                Rationale = string.Create(
                    CultureInfo.InvariantCulture,
                    $"Account '{hot.ProviderKey}' is {Describe(classification)}; move partition {partition} of tree '{tree}' to '{target}' (which has headroom) via ILatticeAdmin.PlanWalMoveAsync then ExecuteWalMoveAsync, and reclaim the source with ReclaimMovedWalSourceAsync once verified."),
            };
        }

        return new WalRebalanceRecommendation
        {
            Tree = tree,
            Partition = partition,
            CurrentProviderKey = hot.ProviderKey,
            TargetProviderKey = string.Empty,
            HasHeadroom = false,
            Classification = classification,
            Rationale = string.Create(
                CultureInfo.InvariantCulture,
                $"Account '{hot.ProviderKey}' is {Describe(classification)} and every registered WAL account is hot; provision and register another account (AddLatticeWalStorageProvider) before relocating partition {partition} of tree '{tree}' with ILatticeAdmin.PlanWalMoveAsync / ExecuteWalMoveAsync."),
        };
    }

    private static bool IsHotter(WalAccountPressure candidate, WalAccountPressure current, bool throughput)
    {
        if (throughput && candidate.Saturation != current.Saturation)
        {
            return candidate.Saturation > current.Saturation;
        }

        if (candidate.WalRetainedBytes != current.WalRetainedBytes)
        {
            return candidate.WalRetainedBytes > current.WalRetainedBytes;
        }

        return string.CompareOrdinal(candidate.ProviderKey, current.ProviderKey) < 0;
    }

    private static (string Tree, int Partition) FindHotPartition(
        IReadOnlyList<WalTreeSample> trees,
        string providerKey,
        bool preferSaturatedTree)
    {
        var fallback = (Tree: string.Empty, Partition: 0);
        var haveFallback = false;
        for (var t = 0; t < trees.Count; t++)
        {
            var tree = trees[t];
            var partitions = tree.Partitions;
            for (var p = 0; p < partitions.Count; p++)
            {
                // Normalise the raw partition key the same way the reduce loop
                // does when it builds per-account totals, so a null provider key
                // (the default account) matches the normalised account key the
                // hot-account selection carries. Comparing the raw key here would
                // never match the default account, collapsing the advice to the
                // empty-tree, partition-zero fallback.
                var partitionKey = partitions[p].ProviderKey ?? IWalStorageProviderCatalog.DefaultProviderKey;
                if (!string.Equals(partitionKey, providerKey, StringComparison.Ordinal))
                {
                    continue;
                }

                var candidate = (tree.TreeId, partitions[p].Partition);
                if (!preferSaturatedTree || tree.Saturation != WalSaturationState.Healthy)
                {
                    return candidate;
                }

                if (!haveFallback)
                {
                    fallback = candidate;
                    haveFallback = true;
                }
            }
        }

        return fallback;
    }

    private static string? SelectTargetWithHeadroom(
        WalAccountPressure[] accounts,
        IReadOnlyCollection<string> catalogKeys,
        string hotKey)
    {
        // A registered key has headroom when it is not the hot key and its account
        // pressure is None (a key backing no partition is absent from `accounts`,
        // so it is implicitly healthy with zero bytes). Ordinal-ordered scan makes
        // the choice deterministic.
        string? best = null;
        foreach (var key in catalogKeys)
        {
            if (string.Equals(key, hotKey, StringComparison.Ordinal))
            {
                continue;
            }

            if (!HasHeadroom(accounts, key))
            {
                continue;
            }

            if (best is null || string.CompareOrdinal(key, best) < 0)
            {
                best = key;
            }
        }

        return best;
    }

    private static bool HasHeadroom(WalAccountPressure[] accounts, string key)
    {
        for (var i = 0; i < accounts.Length; i++)
        {
            if (string.Equals(accounts[i].ProviderKey, key, StringComparison.Ordinal))
            {
                return accounts[i].Classification == WalPressureClassification.None;
            }
        }

        // Registered but backing no partition: fully idle, so it has headroom.
        return true;
    }

    private static string Describe(WalPressureClassification classification) => classification switch
    {
        WalPressureClassification.ThroughputBound => "throughput-bound (backend write rate saturated)",
        WalPressureClassification.CapacityBound => "capacity-bound (retained bytes over the advisory threshold)",
        _ => "under pressure",
    };
}
