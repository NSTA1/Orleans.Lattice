namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persisted state of the <see cref="Grains.IWalCopyLogHoldGrain"/>: the bounded
/// hold one copy builder keeps on its physical copy's WAL (issue #5471).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalCopyLogHoldState)]
internal sealed class WalCopyLogHoldState
{
    /// <summary>Whether the hold is registered with the copy's offset consumers and holds its log.</summary>
    [Id(0)]
    public bool Holding { get; set; }

    /// <summary>The physical tree the copy is built from, whose offset consumers the hold waits for.</summary>
    [Id(1)]
    public string? SourcePhysicalTreeId { get; set; }

    /// <summary>The source log's live offset consumers at arm time: the hold lasts until each has registered on the copy.</summary>
    [Id(2)]
    public List<GrainId> Consumers { get; set; } = [];

    /// <summary>Whether the copy has been cut over (its logical tree's alias now names it).</summary>
    [Id(3)]
    public bool Published { get; set; }

    /// <summary>The UTC tick at which the hold lapses unless renewed (while building) or released earlier (once published).</summary>
    [Id(4)]
    public long ExpiresAtUtcTicks { get; set; }
}
