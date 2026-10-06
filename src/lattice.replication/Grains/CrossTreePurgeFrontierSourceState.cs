namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="ICrossTreePurgeFrontierSourceGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CrossTreePurgeFrontierSourceState)]
internal sealed class CrossTreePurgeFrontierSourceState
{
    /// <summary>The trees a cross-tree decision was ever sequenced on.</summary>
    [Id(0)]
    public HashSet<string> Trees { get; set; } = new(StringComparer.Ordinal);
}
