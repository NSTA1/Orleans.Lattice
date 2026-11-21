namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The write-ahead log position a full snapshot export opened at (issue
/// #5794): every partition's readable head, read before the export's registry
/// snap0, on the physical tree the export read. A shipper whose peer's lineage
/// moved uses it to tell whether an export taken before it took the peer off
/// the log still covers every record the log no longer retains.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationExportCut)]
internal sealed class ReplicationExportCut
{
    /// <summary>The export epoch the cut belongs to.</summary>
    [Id(0)]
    public long Epoch { get; init; }

    /// <summary>The physical tree whose write-ahead log the heads were read from.</summary>
    [Id(1)]
    public string WalTreeId { get; init; } = string.Empty;

    /// <summary>Each partition's readable head when the export opened.</summary>
    [Id(2)]
    public long[] OpenHeads { get; init; } = [];
}
