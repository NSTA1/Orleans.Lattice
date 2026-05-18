namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side seam consumed by the bootstrap state machine
/// (<c>LatticeBootstrapCoordinatorGrain</c>) when it drains a
/// snapshot into the local tree. The seam is separate from
/// <see cref="ISnapshotProvider"/> so that a single silo can be both
/// a snapshot <i>sender</i> (its <see cref="ISnapshotProvider"/> is
/// the local tree, exposed to peer receivers via
/// <see cref="LatticeRemoteSnapshotService"/>) and a snapshot
/// <i>receiver</i> (its <see cref="IBootstrapSnapshotSource"/> is the
/// cross-cluster <c>RemoteSnapshotProvider</c> that drains from an
/// upstream peer) at the same time, without one role overwriting
/// the other's DI slot.
/// <para>
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// registers a default that resolves to the cross-cluster
/// <c>RemoteSnapshotProvider</c> when an
/// <see cref="IRemoteSnapshotTransport"/> is registered in the same
/// service collection (the active-active default) and to a wrapper
/// over the local <see cref="ISnapshotProvider"/> otherwise (the
/// local-only fallback used in single-cluster recovery tests).
/// </para>
/// </summary>
public interface IBootstrapSnapshotSource : ISnapshotProvider
{
}
