namespace Orleans.Lattice;

/// <summary>
/// Extensibility hook invoked synchronously from inside the Lattice grain
/// write path after a published mutation has been durably persisted and before
/// the grain method returns. Intended for change-feed producers, replication
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
/// <para><b>Tree identity.</b> <see cref="LatticeMutation.TreeId"/> identifies
/// the logical tree through which the write was routed, not the derived
/// physical backing created by a resize, shadow restore, or schema remediation.
/// Subscriptions and enrollment filters therefore keep matching after an
/// alias swap. Writes made directly to physical grains without a logical
/// routing context identify that physical tree instead. The routed logical
/// identity is honoured only when its paired physical target matches the
/// publishing grain; unrelated or stale inherited context falls back to the
/// physical identity.
/// </para>
/// <para><b>Threading and latency.</b> <see cref="OnMutationAsync"/> runs
/// on the originating grain's single-threaded scheduler and is awaited
/// inline before the grain method returns. Every millisecond spent inside
/// the observer is a millisecond added to the caller's write latency and
/// a millisecond during which no other call to that grain can be
/// dispatched. Implementations must return quickly - do not issue
/// synchronous network I/O, database writes, or external HTTP calls
/// directly from the hook. The canonical safe pattern is to enqueue a
/// copy of the <see cref="LatticeMutation"/> onto a
/// <c>System.Threading.Channels.Channel&lt;LatticeMutation&gt;</c> and
/// drain it from a background <c>IHostedService</c>, so the grain write
/// path only pays the cost of a channel write.
/// </para>
/// <para><b>Failure semantics.</b> Exceptions thrown by the observer are
/// caught, logged as a warning, and suppressed - the write has already
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
/// <para><b>Coverage.</b> Observers see foreground point, batch and range
/// writes, including saga prepare-phase writes and cross-tree participant
/// legs; non-migration merge traffic that represents external input, such
/// as tree merges, online snapshot or resize copies into their destinations,
/// merging backup restores, and replicated plain values; forwarded writes
/// re-published on their destination; and replicated range deletes, CRDT
/// deltas and prepared writes. Silent paths include bulk loads, offline
/// snapshot copy, single full-backup restore into a new tree or shadow,
/// internal key moves for leaf splits, shard splits and consolidations, WAL
/// replay and snapshot loads at activation, prepared-write visibility at
/// commit, saga terminal records and compaction records.
/// </para>
/// <para><b>DeleteRange shape.</b> A <see cref="MutationKind.DeleteRange"/>
/// event is published per bounded page per shard, not once per tombstoned key
/// and not once per user call. A single <c>ILattice.DeleteRangeAsync</c>
/// invocation against an N-shard tree can therefore produce several
/// <see cref="MutationKind.DeleteRange"/> mutations per shard. Each event's
/// <see cref="LatticeMutation.Key"/> is the page start, and all events from
/// one user call share a transaction id; consumers that need to group them
/// should use that id rather than the key range. The event timestamp is the
/// producer's issue HLC. Observers that need per-key granularity must scan the
/// range themselves.
/// </para>
/// <para><b>Ordering.</b> Within a single leaf grain, observer invocations
/// for successive mutations on the same key are strictly ordered.
/// Ordering across keys, across leaves, or across trees is <b>not</b>
/// guaranteed - the hook fires on whichever grain committed the write,
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
    /// Invoked for a published durably committed mutation. Implementations must
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
