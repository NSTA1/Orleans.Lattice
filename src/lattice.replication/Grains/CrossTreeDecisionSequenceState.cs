namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="ICrossTreeDecisionSequenceGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossTreeDecisionSequenceState)]
internal sealed class CrossTreeDecisionSequenceState
{
    /// <summary>The last sequence issued; <c>0</c> before the first.</summary>
    [Id(0)]
    public long Counter { get; set; }

    /// <summary>The sequences issued and not yet confirmed, by operation id.</summary>
    [Id(1)]
    public Dictionary<string, long> Pending { get; set; } = new(StringComparer.Ordinal);
}
