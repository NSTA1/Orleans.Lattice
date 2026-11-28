namespace Orleans.Lattice.Replication;

/// <summary>
/// What a shipper does with its place in the log while its tree resolves no
/// write class - the tree was disabled at runtime, or its merge mode became
/// ambiguous (issue #5699). A shipper must not ship a record its peer would
/// drop, and must not advance its cursor past one, or the write is lost.
/// </summary>
internal enum ReplicationWriteClassRetention
{
    /// <summary>
    /// The shipper stops shipping, withdraws its offset-consumer registration,
    /// releases its purge and replay holds, and marks the peer for re-seed.
    /// When the tree resolves a write class again the shipper re-attaches and
    /// the peer is re-seeded. Nothing pins the log while the tree is off.
    /// </summary>
    DetachAndReseed = 0,
}
