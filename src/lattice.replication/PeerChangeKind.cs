namespace Orleans.Lattice.Replication;

/// <summary>
/// Discriminator for <see cref="PeerChanged"/> notifications surfaced by
/// <see cref="IReplicationTopology"/>. The two values exhaustively describe
/// every observable change to the peer membership set: a peer that was not
/// previously known has appeared, or a peer that was previously known has
/// disappeared. Replacing a peer's wire-level endpoint is intentionally
/// not modelled here: the transport-agnostic topology surface only deals
/// in stable cluster identifiers, and endpoint resolution is the
/// transport implementation's concern.
/// </summary>
public enum PeerChangeKind
{
    /// <summary>
    /// A peer cluster id has been observed in the topology for the first
    /// time (or for the first time since it was previously removed).
    /// Hosts react by activating any per-peer drivers required for the
    /// new peer (the canonical example is one
    /// <c>IReplicationShipperGrain</c> per <c>(replicated tree, peer)</c>).
    /// </summary>
    Added = 0,

    /// <summary>
    /// A peer cluster id previously surfaced as <see cref="Added"/> has
    /// been withdrawn from the topology. Hosts react by stopping any
    /// outbound traffic to the peer; the canonical path is per-driver-
    /// defined (the shipper grain stays activated for the remainder of
    /// its keepalive window, but the producer-side doorbell ring stops
    /// firing because the peer no longer appears in the snapshot).
    /// </summary>
    Removed = 1,
}
