namespace Orleans.Lattice.Replication;

/// <summary>
/// One <c>(tree, peer, direction)</c> row of the per-peer replication telemetry
/// state behind <see cref="ReplicationPeerStats"/>, carried off-silo by the
/// peer-status read path. It is the serializable twin of
/// <see cref="ReplicationPeerSnapshot"/> and carries exactly the fields that
/// state already records - nothing is derived or added here.
/// </summary>
/// <param name="Tree">The effective (logical, possibly tenant-composed) replicated tree id.</param>
/// <param name="Peer">The remote peer cluster id.</param>
/// <param name="Direction">The contact direction this row describes.</param>
/// <param name="EntriesBehind">WAL entries yet to ship to the peer (outbound rows only; zero on inbound rows).</param>
/// <param name="BytesBehind">Payload bytes yet to ship to the peer (outbound rows only; zero on inbound rows).</param>
/// <param name="ConsecutiveErrors">Consecutive contact-attempt failures since the last success in this direction.</param>
/// <param name="LastContactSeconds">
/// Seconds since the last successful contact in this direction, measured on the
/// reporting silo's clock, or <see cref="double.NaN"/> when the peer has never
/// been contacted in this direction.
/// </param>
/// <param name="InFlight">Outbound shipped-but-unacknowledged batches (zero on inbound rows).</param>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationPeerStatusRow)]
[Immutable]
internal readonly record struct ReplicationPeerStatusRow(
    [property: Id(0)] string Tree,
    [property: Id(1)] string Peer,
    [property: Id(2)] ReplicationContactDirection Direction,
    [property: Id(3)] long EntriesBehind,
    [property: Id(4)] long BytesBehind,
    [property: Id(5)] long ConsecutiveErrors,
    [property: Id(6)] double LastContactSeconds,
    [property: Id(7)] long InFlight);
