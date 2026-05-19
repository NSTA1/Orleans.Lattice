namespace Orleans.Lattice.Replication;

/// <summary>
/// Runtime-observable source of replication peer membership. Replaces
/// the previous "read <see cref="LatticeReplicationOptions.ReplicationPeers"/>
/// once at silo startup" model so hosts can add or remove peers at
/// runtime without restarting the silo.
/// <para>
/// The contract is intentionally narrow: a synchronous
/// <see cref="CurrentPeers"/> snapshot for callers that just need the
/// current set, plus a <see cref="Subscribe"/> push channel for callers
/// that need to react to net membership changes. Endpoint resolution
/// (cluster id to wire-level <see cref="Uri"/>) stays the transport
/// implementation's responsibility: this seam is transport-agnostic
/// and only deals in stable peer cluster identifiers.
/// </para>
/// <para>
/// Implementations must be safe for concurrent use. The default
/// <see cref="OptionsReplicationTopology"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// projects <see cref="LatticeReplicationOptions.ReplicationPeers"/>
/// via <see cref="Microsoft.Extensions.Options.IOptionsMonitor{TOptions}.OnChange(System.Action{TOptions, string})"/>;
/// hosts that source their topology from a service registry,
/// configuration provider, or any other dynamic surface can replace it
/// by registering their own singleton before <c>AddLatticeReplication</c>
/// runs.
/// </para>
/// <para>
/// This seam governs the <em>activation</em> side of the replication
/// pipeline only. Several operational reads still consult
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> directly:
/// <c>ShardedReplogSink</c>'s per-append doorbell fan-out and
/// <c>ReplicationMaintenanceGrain</c>'s fall-off-log probe. In the
/// default configuration this is invisible because both sides project
/// the same <see cref="Microsoft.Extensions.Options.IOptionsMonitor{TOptions}"/>
/// instance. Hosts that replace the default topology with a source
/// that does <em>not</em> mirror its membership into
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> will see
/// per-concern divergence: activation follows this topology, while
/// doorbell rings and fall-off probes follow the options snapshot. See
/// <c>docs/lattice.replication/replication-drivers.md</c> ("Topology
/// vs. ReplicationPeers: who owns what") for the full table and
/// recommended discipline.
/// </para>
/// <para>
/// The seam is deliberately <em>not</em> modelled as
/// <see cref="IObservable{T}"/>: the BCL observable contract forces
/// callers to implement <see cref="IObserver{T}"/> for what would
/// otherwise be a one-line lambda subscription, and an
/// <c>IObservable&lt;T&gt;</c>-shaped API tempts callers to pull in a
/// reactive-extensions dependency that the replication package
/// otherwise does not need. A direct <see cref="Subscribe(System.Action{PeerChanged})"/>
/// overload keeps the call site to one lambda and reserves the freedom
/// to add an <c>IObservable&lt;T&gt;</c> adapter later without
/// breaking the primary surface.
/// </para>
/// </summary>
public interface IReplicationTopology
{
    /// <summary>
    /// Returns the current set of peer cluster ids the local silo is
    /// configured to ship to. The returned collection is a point-in-time
    /// snapshot; subsequent runtime changes are observable through
    /// <see cref="Subscribe"/>. Never <see langword="null"/>, may be
    /// empty when no peers are configured.
    /// </summary>
    IReadOnlyCollection<string> CurrentPeers { get; }

    /// <summary>
    /// Subscribes <paramref name="onChange"/> to receive
    /// <see cref="PeerChanged"/> notifications for every subsequent net
    /// membership change. The callback is invoked on the thread that
    /// applied the underlying configuration change; subscribers must
    /// not block inside the callback.
    /// <para>
    /// The returned <see cref="IDisposable"/> unsubscribes the callback
    /// when disposed. Disposing twice is safe; the second
    /// <see cref="IDisposable.Dispose"/> call is a no-op.
    /// </para>
    /// <para>
    /// The initial snapshot is <em>not</em> replayed through
    /// <paramref name="onChange"/>: subscribers should call
    /// <see cref="CurrentPeers"/> to read the current set, then
    /// subscribe for subsequent deltas. Replaying the snapshot is
    /// avoidable in the subscriber by capturing the snapshot before
    /// <c>Subscribe</c> and would force every subscription site to
    /// short-circuit "did I already see this peer".
    /// </para>
    /// </summary>
    /// <param name="onChange">
    /// Callback invoked once per net peer membership change. Must not
    /// be <see langword="null"/>.
    /// </param>
    IDisposable Subscribe(Action<PeerChanged> onChange);
}
