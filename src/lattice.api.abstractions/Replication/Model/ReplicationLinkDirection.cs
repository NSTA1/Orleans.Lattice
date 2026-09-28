namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Which way a replication link carries entries, relative to the region that
/// produced the report. Mirrors the replication engine's contact direction; it
/// is declared separately so the abstractions package carries no dependency on
/// the engine.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationLinkDirection)]
public enum ReplicationLinkDirection
{
    /// <summary>The local region ships the tree's entries to the peer.</summary>
    Outbound = 0,

    /// <summary>The local region applies the tree's entries authored by the peer.</summary>
    Inbound = 1,
}
