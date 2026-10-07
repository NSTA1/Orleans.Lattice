namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="ICrossTreeHoldTrackerGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossTreeHoldTrackerState)]
internal sealed class CrossTreeHoldTrackerState
{
    /// <summary>Each participant tree's recorded boundary.</summary>
    [Id(0)]
    public Dictionary<string, CrossTreeHoldBoundary> Boundaries { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The participant trees the hold has released.</summary>
    [Id(1)]
    public HashSet<string> Released { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Every participant was released; the boundaries were dropped.</summary>
    [Id(2)]
    public bool Completed { get; set; }
}
