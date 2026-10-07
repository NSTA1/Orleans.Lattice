namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The decision rows imports from one source recorded and the boundary that
/// retires them (issue #4524).
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ImportedDecisionSet)]
internal sealed class ImportedDecisionSet
{
    /// <summary>
    /// The latest import's own export boundary, or <see langword="null"/> when
    /// that import's source did not capture one: the rows are then retained.
    /// </summary>
    [Id(0)]
    public CrossTreeSiblingBoundary? ExportBoundary { get; set; }

    /// <summary>The imported saga ids whose decision rows are still retained.</summary>
    [Id(1)]
    public HashSet<Guid> TransactionIds { get; set; } = [];
}
