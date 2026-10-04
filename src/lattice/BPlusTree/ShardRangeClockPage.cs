using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One bounded batch of a shard-level range clock probe (see
/// <see cref="IShardRootGrain.GetRangeClockBoundedAsync"/>): the highest
/// <see cref="HybridLogicalClock"/> held by the leaves the batch visited, over a
/// key range a range delete is about to cover (issue #4530).
/// <para>
/// A leaf's clock has merged the stamp of every row and prepared bucket it
/// holds, so a range delete stamped above the maximum over every leaf in its
/// range sorts above everything already written there - including the prepares
/// of a saga decided before the delete was issued.
/// </para>
/// <para>
/// <see cref="ResumeFromInclusive"/> is the key to resume from, as for
/// <see cref="ShardRangeDeletePage"/>, or <see langword="null"/> when this shard's
/// portion of the range has been probed.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardRangeClockPage)]
[Immutable]
internal readonly record struct ShardRangeClockPage
{
    /// <summary>The highest leaf clock this batch observed; <see cref="HybridLogicalClock.Zero"/> when it visited none.</summary>
    [Id(0)] public HybridLogicalClock MaxClock { get; init; }

    /// <summary>
    /// The key to resume from, as the next batch's inclusive lower bound, or
    /// <see langword="null"/> when this shard has no more of the range to probe.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
