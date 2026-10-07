namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="ICrossTreePeerEnrolmentGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossTreePeerEnrolmentState)]
internal sealed class CrossTreePeerEnrolmentState
{
    /// <summary>The enrolled peer cluster ids.</summary>
    [Id(0)]
    public HashSet<string> Peers { get; set; } = new(StringComparer.Ordinal);
}
