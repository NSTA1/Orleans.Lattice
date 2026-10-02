namespace Orleans.Lattice.Operations;

/// <summary>The persisted state of one <see cref="LatticeOperationIndexGrain"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationIndexState)]
internal sealed class LatticeOperationIndexState
{
    /// <summary>The indexed operations, newest-first by start time, ties by ordinal id.</summary>
    [Id(0)] public List<LatticeOperationIndexEntry> Entries { get; set; } = [];
}
