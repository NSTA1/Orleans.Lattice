using System.Runtime.CompilerServices;
using Orleans.Lattice;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Helper that adapts a whole-tree <see cref="ISnapshotProvider"/> export into a
/// range-scoped one by filtering the producer's entry stream on the client side.
/// It is the default implementation behind
/// <see cref="ISnapshotProvider.ExportAsync(string, IReadOnlyList{LeafReReplayRange}, HybridLogicalClock, CancellationToken)"/>:
/// it preserves the snapshot's <see cref="SnapshotStream.AsOfHlc"/> and
/// <see cref="SnapshotStream.CausalStableFrontier"/> verbatim (so receivers pin
/// the same resume cut as a whole-tree export at the same as-of HLC) and yields
/// only the entries whose key falls inside at least one of the supplied
/// half-open ranges.
/// <para>
/// The filter is the union of the ranges, evaluated with the same ordinal
/// half-open <c>[StartKey, EndKey)</c> membership as
/// <see cref="LeafReReplayRange.Contains(string?)"/>, so the scoped export
/// localises on byte-identical boundaries to the targeted leaf re-replay repair
/// pass. An empty range list scopes to the union of zero ranges and yields no
/// entries.
/// </para>
/// </summary>
internal static class ScopedSnapshotStream
{
    /// <summary>
    /// Exports a whole-tree snapshot from <paramref name="provider"/> and wraps
    /// its entry stream with a range filter. The metadata
    /// (<see cref="SnapshotStream.TreeName"/>,
    /// <see cref="SnapshotStream.AsOfHlc"/>,
    /// <see cref="SnapshotStream.CausalStableFrontier"/>) is carried through
    /// unchanged.
    /// </summary>
    /// <param name="provider">The whole-tree snapshot provider to scope.</param>
    /// <param name="treeName">The logical tree id to export. Must be non-null and non-empty.</param>
    /// <param name="ranges">The half-open covering ranges to scope the export to. Must be non-null.</param>
    /// <param name="asOfHlc">Strict upper-bound timestamp; <see cref="HybridLogicalClock.Zero"/> disables the bound.</param>
    /// <param name="cancellationToken">Observed on the up-front export and on every yielded entry.</param>
    /// <returns>A range-scoped <see cref="SnapshotStream"/> over the provider's whole-tree export.</returns>
    public static async Task<SnapshotStream> CreateAsync(
        ISnapshotProvider provider,
        string treeName,
        IReadOnlyList<LeafReReplayRange> ranges,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentNullException.ThrowIfNull(ranges);

        var full = await provider.ExportAsync(treeName, asOfHlc, cancellationToken).ConfigureAwait(false);
        return new SnapshotStream(
            full.TreeName,
            full.AsOfHlc,
            full.CausalStableFrontier,
            Filter(full.Entries, ranges, cancellationToken));
    }

    private static async IAsyncEnumerable<SnapshotEntry> Filter(
        IAsyncEnumerable<SnapshotEntry> source,
        IReadOnlyList<LeafReReplayRange> ranges,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var entry in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (InAnyRange(entry.Key, ranges))
            {
                yield return entry;
            }
        }
    }

    private static bool InAnyRange(string? key, IReadOnlyList<LeafReReplayRange> ranges)
    {
        for (var i = 0; i < ranges.Count; i++)
        {
            if (ranges[i].Contains(key))
            {
                return true;
            }
        }
        return false;
    }
}
