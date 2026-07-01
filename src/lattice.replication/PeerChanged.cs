namespace Orleans.Lattice.Replication;

/// <summary>
/// Single change notification emitted by <see cref="IReplicationTopology"/>
/// when the configured peer membership set transitions. Carries the
/// affected peer cluster id and a <see cref="PeerChangeKind"/>
/// discriminator describing whether the peer was added to or removed
/// from the topology.
/// <para>
/// Notifications are emitted only for net membership changes, not for
/// every <c>IOptionsMonitor</c> reload. The default topology
/// implementation diffs each reload against the last-seen set and
/// suppresses no-op reloads; subscribers therefore see exactly one
/// <see cref="PeerChangeKind.Added"/> event for a newly-appearing peer and exactly one
/// <see cref="PeerChangeKind.Removed"/> event for a withdrawn peer, regardless of how
/// many reload callbacks the underlying configuration emits.
/// </para>
/// </summary>
/// <param name="PeerClusterId">
/// Stable identifier of the peer cluster whose membership has changed.
/// Never <see langword="null"/>, never empty, never whitespace.
/// </param>
/// <param name="Kind">
/// Whether <paramref name="PeerClusterId"/> was added to or removed
/// from the topology.
/// </param>
public readonly record struct PeerChanged(string PeerClusterId, PeerChangeKind Kind);
