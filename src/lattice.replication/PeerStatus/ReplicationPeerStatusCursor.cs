namespace Orleans.Lattice.Replication;

/// <summary>
/// An exclusive lower bound in the peer-status read order: the key of the last
/// row a caller has already been handed. The tree is the effective tree id the
/// row was reported under (tenant-qualified for a tenant's own tree), so a
/// cursor only ever carries an id the caller was shown.
/// </summary>
/// <param name="Tree">The effective tree id of the last row returned.</param>
/// <param name="Peer">The peer cluster id of the last row returned.</param>
/// <param name="Direction">The contact direction of the last row returned.</param>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationPeerStatusCursor)]
[Immutable]
internal readonly record struct ReplicationPeerStatusCursor(
    [property: Id(0)] string Tree,
    [property: Id(1)] string Peer,
    [property: Id(2)] ReplicationContactDirection Direction);
