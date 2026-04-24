namespace Orleans.Lattice;

/// <summary>
/// Extensibility hook invoked synchronously from inside the Lattice grain
/// write path after the mutation has been durably persisted and before the
/// grain method returns. Intended for change-feed producers, replication
/// write-ahead logs, and external audit consumers.
/// <para>
/// Register observers in the silo DI container (for example via
/// <c>services.AddSingleton&lt;IMutationObserver, MyObserver&gt;()</c>).
/// Observers are resolved as <see cref="IEnumerable{T}"/> so multiple can
/// coexist; exceptions thrown by one observer are logged and do not
/// short-circuit the others. The hook is zero-cost when no observer is
/// registered.
/// </para>
/// </summary>
/// <remarks>
/// <para><b>Threading and latency.</b> <see cref="OnMutationAsync"/> runs
/// on the originating grain's single-threaded scheduler and is awaited
/// inline before the grain method returns. Every millisecond spent inside
/// the observer is a millisecond added to the caller's write latency and
/// a millisecond during which no other call to that grain can be
/// dispatched. Implementations must return quickly — do not issue
/// synchronous network I/O, database writes, or external HTTP calls
/// directly from the hook. The canonical safe pattern is to enqueue a
/// copy of the <see cref="LatticeMutation"/> onto a
/// <c>System.Threading.Channels.Channel&lt;LatticeMutation&gt;</c> and
/// drain it from a background <c>IHostedService</c>, so the grain write
/// path only pays the cost of a channel write.
/// </para>
/// <para><b>Failure semantics.</b> Exceptions thrown by the observer are
/// caught, logged as a warning, and suppressed — the write has already
/// been persisted and cannot be rolled back. Observers that need at-least-once
/// delivery must durably record the mutation themselves (for example to a
/// local WAL or outbox) before returning, and retry out-of-band. A silent
/// throw does not fail the caller.
/// </para>
/// <para><b>Re-entrancy.</b> The hook fires inside the grain activation
/// that owns the mutation. Calling back into the same <see cref="ILattice"/>
/// tree from the observer is not a deadlock (Orleans allows it) but it
/// compounds write-path latency and, if the observer mutates the same key,
/// will reorder relative to concurrent callers. Prefer enqueue-and-drain
/// for any follow-on Lattice writes.
/// </para>
/// <para><b>Coverage gaps.</b> Observers see every <i>originating</i>
/// write (<see cref="MutationKind.Set"/>, <see cref="MutationKind.Delete"/>,
/// <see cref="MutationKind.DeleteRange"/>) but deliberately do <b>not</b>
/// see downstream convergence traffic: <c>MergeEntriesAsync</c> /
/// <c>MergeManyAsync</c>, shard-split shadow-forward, saga compensation
/// rollback replays, and snapshot / restore bulk loads are silent because
/// they are replays of mutations already published at their origin. In a
/// multi-cluster topology the replication package is responsible for
/// surfacing cross-cluster events — the core hook is local-origin only.
/// </para>
/// <para><b>DeleteRange shape.</b> A <see cref="MutationKind.DeleteRange"/>
/// event is published <b>once per shard</b> that received the range
/// (<i>not</i> once per tombstoned key and <i>not</i> once per user call),
/// and is emitted <b>even when a given shard matched zero live keys</b>
/// — replication consumers must propagate the range to peer clusters
/// unconditionally because peer clusters may hold keys in it. A single
/// <c>ILattice.DeleteRangeAsync</c> invocation against an N-shard tree
/// therefore produces up to N identical-payload
/// <see cref="MutationKind.DeleteRange"/> mutations (same
/// <see cref="LatticeMutation.Key"/> / <see cref="LatticeMutation.EndExclusiveKey"/>),
/// which is idempotent by design but noisy. Downstream consumers that
/// need exactly-once delivery per user call must dedup on
/// <c>(TreeId, Key, EndExclusiveKey)</c>. Contrast with the tree-event
/// stream (<see cref="LatticeTreeEvent"/>), which collapses the fan-out
/// into a single event at the <c>LatticeGrain</c> level.
/// <see cref="LatticeMutation.Timestamp"/> is
/// <c>HybridLogicalClock.Zero</c> because a single range may produce many
/// per-leaf HLCs that cannot be faithfully collapsed into a single
/// timestamp. Observers that need per-key granularity must scan the range
/// themselves.
/// </para>
/// <para><b>Ordering.</b> Within a single leaf grain, observer invocations
/// for successive mutations on the same key are strictly ordered.
/// Ordering across keys, across leaves, or across trees is <b>not</b>
/// guaranteed — the hook fires on whichever grain committed the write,
/// and different grains run on different schedulers. Consumers that need
/// a global order must impose one downstream (for example via the HLC on
/// each <see cref="LatticeMutation"/>).
/// </para>
/// <para><b>Fan-out cost.</b> A single observer registration applies to
/// every tree in the silo. In multi-tenant deployments the observer
/// should fast-path (or filter by <see cref="LatticeMutation.TreeId"/>)
/// before doing any real work, otherwise the hot path pays the observer
/// cost on trees that do not need observation.
/// </para>
/// </remarks>
public interface IMutationObserver
{
    /// <summary>
    /// Invoked once per durably-committed mutation. Implementations must
    /// treat <paramref name="mutation"/> as immutable and should complete
    /// quickly; long-running work belongs on a background queue drained
    /// by an <c>IHostedService</c>.
    /// </summary>
    /// <param name="mutation">The committed mutation metadata.</param>
    /// <param name="cancellationToken">
    /// Cancellation signal propagated from the grain's ambient cancellation
    /// when one is plumbed through. Observers should respect it for any
    /// asynchronous work they start.
    /// </param>
    Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken);
}
