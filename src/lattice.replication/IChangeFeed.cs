using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Pure-pull, cursor-driven subscriber API over the per-shard
/// write-ahead log. Lets in-process consumers (the outbound ship loop,
/// custom bridges, integration tests, in-process projections) read every
/// captured <see cref="WalRecord"/> for a tree without touching the
/// primary state and without depending on transport-shaped acks.
/// <para>
/// The contract is deliberately neutral: there is no peer id, no
/// per-call ack envelope, no notion of "live" vs. "snapshot" mode.
/// Consumers pass an <see cref="HybridLogicalClock"/> cursor on each
/// call and receive every entry they have not yet seen, in HLC
/// ascending order. To stream forward, a consumer remembers the
/// timestamp of the last entry it observed and re-subscribes with
/// that value as the new cursor.
/// </para>
/// <para>
/// <b>Scope: locally-authored writes only.</b> The feed surfaces the
/// WAL, and the WAL is appended <i>only</i> by mutations that
/// originate as fresh user-authored writes on this cluster - i.e.
/// calls reaching <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>, 
/// <see cref="ILattice.SetAsync(string, byte[], TimeSpan, CancellationToken)"/>, 
/// <see cref="ILattice.DeleteAsync(string, CancellationToken)"/>, and
/// <see cref="ILattice.DeleteRangeAsync(string, string, CancellationToken)"/>.
/// Entries installed by the receiver-side apply pipeline
/// (<see cref="IReplicationApplier"/> / <c>IReplicationApplyGrain</c>),
/// by shard-split shadow-forward, by tree-merge / online-resize
/// forwards, or by snapshot / bootstrap bulk-load are deliberately
/// <b>not</b> WAL-appended on the destination - re-emitting them
/// would cause the producer-side ship loop to re-ship them as
/// local-origin writes, looping the cluster (the
/// <c>includeLocalOrigin=false</c> filter on <see cref="Subscribe"/>
/// is a wire-shape cycle-break for the remote shipper, not the
/// authority on what enters the feed in the first place). The
/// <see cref="IMutationObserver"/> hook in the core library has the
/// same scope by design - its "Coverage gaps" remarks describe the
/// same boundary.
/// </para>
/// <para>
/// <b>Reading the feed for "every observable state change" is wrong.</b>
/// In a multi-cluster topology, a value installed on this cluster by a
/// remote apply will <i>not</i> appear in this feed - even though the
/// state is now visible to local readers. Consumers that need to react
/// to every observable state change (audit projections, anti-entropy
/// verifiers, dashboards that surface peer-cluster activity, sample
/// apps that visualise cross-cluster traffic) must subscribe at the
/// apply seam instead by registering an
/// <see cref="IReplicationApplier"/> decorator: the decorator wraps
/// the canonical applier, sees every receiver-side install, and is
/// invoked on the same thread that performs the merge so it has the
/// full <see cref="WalRecord"/> in hand. The change feed remains
/// the right surface for "ship this cluster's authored writes
/// elsewhere"; the apply-decorator is the right surface for "react to
/// every byte that lands in this cluster's state".
/// </para>
/// </summary>
public interface IChangeFeed
{
    /// <summary>
    /// Yields every captured <see cref="WalRecord"/> for
    /// <paramref name="treeName"/> with
    /// <see cref="WalRecord.Timestamp"/> strictly greater than
    /// <paramref name="cursor"/>. Entries are emitted in HLC ascending
    /// order; ties are broken by the order in which the merge consumes
    /// them across partitions and is therefore unspecified - consumers
    /// must treat the feed as a multiset under equal HLCs.
    /// <para>
    /// The enumeration takes a snapshot of the WAL at call time and
    /// completes once that snapshot is exhausted. To pick up entries
    /// committed after the call, a consumer re-subscribes with an
    /// updated cursor; this matches the cursor-driven, pure-pull model
    /// described in the replication design.
    /// </para>
    /// <para>
    /// The feed is <b>locally-authored writes only</b>: see the
    /// type-level remarks for the full statement. In particular, this
    /// method does not yield entries installed by the receiver-side
    /// apply pipeline; consumers that want to observe remote applies
    /// must decorate <see cref="IReplicationApplier"/> instead.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// Logical tree id whose change feed is being consumed. Only
    /// entries with <see cref="WalRecord.TreeId"/> equal to this
    /// value are yielded. Must not be <see langword="null"/>.
    /// </param>
    /// <param name="cursor">
    /// Strict lower-bound timestamp; the feed yields entries with
    /// <c>entry.Timestamp &gt; cursor</c>. Pass
    /// <see cref="HybridLogicalClock.Zero"/> to read from the start of
    /// the WAL.
    /// </param>
    /// <param name="includeLocalOrigin">
    /// When <see langword="true"/> (the default), entries authored by
    /// the local cluster are included in the stream. When
    /// <see langword="false"/>, entries whose
    /// <see cref="WalRecord.OriginClusterId"/> matches the configured
    /// local <see cref="LatticeReplicationOptions.ClusterId"/> are
    /// filtered out - the cursor-driven cycle-break used by remote
    /// shippers. Defaults to <see langword="true"/> because in-process
    /// projections and background materialisers need to observe
    /// local-origin mutations. Note that this flag filters
    /// <i>within</i> the locally-authored feed; it does not surface
    /// remote-apply installations (those never enter the WAL on this
    /// cluster - see the type-level remarks).
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed between every page read and every yielded entry.</param>
    IAsyncEnumerable<WalRecord> Subscribe(
        string treeName,
        HybridLogicalClock cursor,
        bool includeLocalOrigin = true,
        CancellationToken cancellationToken = default);
}
