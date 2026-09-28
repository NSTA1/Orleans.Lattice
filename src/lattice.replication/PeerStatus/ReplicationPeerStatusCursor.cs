namespace Orleans.Lattice.Replication;

/// <summary>
/// An exclusive lower bound in the peer-status read order: the key of the last
/// row a caller has already been handed. The tree is the <b>display</b> tree id
/// (the effective id with the caller's own tenant qualification removed), so a
/// cursor never has to carry a composed id the caller was not shown.
/// </summary>
/// <param name="Tree">The display tree id of the last row returned.</param>
/// <param name="Stripped">
/// Whether that row's display id was produced by removing the caller's tenant
/// qualification. Breaks the tie between a tenant's own <c>t/{tenant}/x</c> and a
/// bare <c>x</c>, which share a display id.
/// </param>
/// <param name="Peer">The peer cluster id of the last row returned.</param>
/// <param name="Direction">The contact direction of the last row returned.</param>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationPeerStatusCursor)]
[Immutable]
internal readonly record struct ReplicationPeerStatusCursor(
    [property: Id(0)] string Tree,
    [property: Id(1)] bool Stripped,
    [property: Id(2)] string Peer,
    [property: Id(3)] ReplicationContactDirection Direction);
