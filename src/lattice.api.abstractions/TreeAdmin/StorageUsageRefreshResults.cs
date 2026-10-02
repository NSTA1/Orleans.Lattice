using System.Collections.Immutable;
using System.Globalization;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Converts between the cluster totals of a storage-usage refresh and the string
/// result map its tracked operation (<see cref="StorageUsageRefreshOperation.Kind"/>)
/// records. The map carries the cluster totals only, never the per-tree rows, so it
/// stays small however many trees the cluster holds; read the refreshed per-tree
/// figures with <see cref="ILatticeTreeAdmin.GetStorageUsageAsync"/>.
/// </summary>
public static class StorageUsageRefreshResults
{
    /// <summary>Result key: the number of trees measured.</summary>
    public const string TreeCountKey = "treeCount";

    /// <summary>Result key: retained write-ahead-log bytes across all trees.</summary>
    public const string WalRetainedBytesKey = "walRetainedBytes";

    /// <summary>Result key: persisted snapshot bytes across all trees.</summary>
    public const string SnapshotBytesKey = "snapshotBytes";

    /// <summary>Result key: persisted leaf-state bytes across all trees.</summary>
    public const string LeafStateBytesKey = "leafStateBytes";

    /// <summary>Result key: total persisted bytes across all trees.</summary>
    public const string TotalBytesKey = "totalBytes";

    /// <summary>Result key: <c>true</c> when at least one tree reported a partial reading.</summary>
    public const string PartialKey = "partial";

    /// <summary>Result key: when the refresh was sampled, as a round-trip (<c>O</c>) UTC timestamp.</summary>
    public const string SampledAtKey = "sampledAt";

    /// <summary>Encodes the cluster totals of <paramref name="summary"/> as a result map.</summary>
    /// <param name="summary">The refreshed summary. Must not be <c>null</c>.</param>
    /// <returns>The result map.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="summary"/> is <c>null</c>.</exception>
    public static IReadOnlyDictionary<string, string> ToResultMap(ClusterStorageUsageSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);
        return new Dictionary<string, string>(7, StringComparer.Ordinal)
        {
            [TreeCountKey] = summary.TreeCount.ToString(CultureInfo.InvariantCulture),
            [WalRetainedBytesKey] = summary.WalRetainedBytes.ToString(CultureInfo.InvariantCulture),
            [SnapshotBytesKey] = summary.SnapshotBytes.ToString(CultureInfo.InvariantCulture),
            [LeafStateBytesKey] = summary.LeafStateBytes.ToString(CultureInfo.InvariantCulture),
            [TotalBytesKey] = summary.TotalBytes.ToString(CultureInfo.InvariantCulture),
            [PartialKey] = summary.Partial ? "true" : "false",
            [SampledAtKey] = summary.SampledAt.UtcDateTime.ToString("O", CultureInfo.InvariantCulture),
        };
    }

    /// <summary>
    /// Rebuilds the cluster totals of a succeeded refresh from its result map, as a
    /// <see cref="ClusterStorageUsageSummary"/> with <see cref="ClusterStorageUsageSummary.Deep"/>
    /// set and no per-tree rows.
    /// </summary>
    /// <param name="result">The operation's result map. Must not be <c>null</c>.</param>
    /// <param name="summary">The rebuilt totals when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the map describes a storage-usage refresh.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <c>null</c>.</exception>
    public static bool TryReadSummary(IReadOnlyDictionary<string, string> result, out ClusterStorageUsageSummary? summary)
    {
        ArgumentNullException.ThrowIfNull(result);
        summary = null;
        if (!result.TryGetValue(TreeCountKey, out var treeCountText)
            || !int.TryParse(treeCountText, NumberStyles.None, CultureInfo.InvariantCulture, out var treeCount)
            || !TryReadLong(result, WalRetainedBytesKey, out var wal)
            || !TryReadLong(result, SnapshotBytesKey, out var snapshot)
            || !TryReadLong(result, LeafStateBytesKey, out var leafState)
            || !TryReadLong(result, TotalBytesKey, out var total)
            || !result.TryGetValue(PartialKey, out var partialText)
            || !bool.TryParse(partialText, out var partial)
            || !result.TryGetValue(SampledAtKey, out var sampledText)
            || !DateTimeOffset.TryParse(sampledText, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var sampledAt))
        {
            return false;
        }

        summary = new ClusterStorageUsageSummary
        {
            TreeCount = treeCount,
            WalRetainedBytes = wal,
            SnapshotBytes = snapshot,
            LeafStateBytes = leafState,
            TotalBytes = total,
            Partial = partial,
            Deep = true,
            SampledAt = sampledAt,
            Trees = ImmutableArray<TreeStorageUsageSnapshot>.Empty,
        };
        return true;
    }

    private static bool TryReadLong(IReadOnlyDictionary<string, string> result, string key, out long value)
    {
        value = 0;
        return result.TryGetValue(key, out var text)
            && long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value);
    }
}
