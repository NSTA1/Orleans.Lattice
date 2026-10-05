using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per physical tree, the replication consumers that need every saga decision
/// on the tree kept (issues #4533 and #4534). While any hold is outstanding the
/// transaction registry suspends every decision purge for the tree. A shipper
/// holds the tree through a re-seed and the replay that follows it, so a saga
/// in flight at the re-seed's export keeps its decision while the replay may
/// still read it; each consumer removes its own hold. Keyed by the physical
/// tree id whose log the consumer reads; consumers are identified by a string
/// derived from their grain id.
/// </summary>
[Alias(TypeAliases.IWalPurgeHoldGrain)]
internal interface IWalPurgeHoldGrain : IGrainWithStringKey
{
    /// <summary>
    /// Durably records (or widens) <paramref name="consumerId"/>'s hold to
    /// cover <paramref name="trimmedThrough"/>, taken partition by partition
    /// as the maximum with any existing hold.
    /// </summary>
    Task AddAsync(string consumerId, long[] trimmedThrough);

    /// <summary>Durably removes <paramref name="consumerId"/>'s hold. Idempotent.</summary>
    Task RemoveAsync(string consumerId);

    /// <summary>
    /// Durably removes <paramref name="consumerId"/>'s hold when
    /// <paramref name="positions"/> covers it: for every partition the hold
    /// names, the consumer's durable read position is past the trimmed offset,
    /// so it delivered every trimmed record before the trim. A
    /// <see langword="null"/> <paramref name="positions"/> removes the hold
    /// unconditionally. Checked and removed in one turn, so a hold a concurrent
    /// trim widened is never released by an older read.
    /// </summary>
    /// <returns><see langword="true"/> when a hold was removed.</returns>
    Task<bool> ReleaseIfCoveredAsync(string consumerId, long[]? positions);

    /// <summary>Returns every outstanding hold.</summary>
    [AlwaysInterleave]
    Task<IReadOnlyDictionary<string, WalPurgeHold>> GetAsync();
}
