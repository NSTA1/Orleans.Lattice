namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// One consumer's entry in <see cref="IWalPurgeHoldGrain"/>: for each WAL
/// partition, the highest offset a forced trim removed past the consumer's
/// unshipped cursor (empty for a hold no trim recorded, such as a replay's),
/// and when the hold was first taken.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalPurgeHold)]
internal sealed record WalPurgeHold
{
    /// <summary>Per partition, the highest offset a forced trim removed; <c>-1</c> for an untouched partition.</summary>
    [Id(0)]
    public long[] TrimmedThrough { get; init; } = [];

    /// <summary>When the hold was first taken.</summary>
    [Id(1)]
    public DateTimeOffset Since { get; init; }
}
