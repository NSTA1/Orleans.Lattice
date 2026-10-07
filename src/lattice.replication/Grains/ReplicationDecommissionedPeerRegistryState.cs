namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="IReplicationDecommissionedPeerRegistryGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationDecommissionedPeerRegistryState)]
internal sealed class ReplicationDecommissionedPeerRegistryState
{
    /// <summary>Every decommissioned peer cluster id, mapped to when it was decommissioned.</summary>
    [Id(0)]
    public Dictionary<string, DateTimeOffset> DecommissionedAtUtc { get; set; } = new(StringComparer.Ordinal);
}
