namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>Persisted state of <see cref="IWalPurgeHoldGrain"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.WalPurgeHoldState)]
internal sealed class WalPurgeHoldState
{
    /// <summary>The outstanding holds, by consumer id.</summary>
    [Id(0)]
    public Dictionary<string, WalPurgeHold> Holds { get; set; } = new(StringComparer.Ordinal);
}
