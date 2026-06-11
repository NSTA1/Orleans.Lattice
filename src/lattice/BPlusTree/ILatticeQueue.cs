namespace Orleans.Lattice;

/// <summary>
/// A typed, cluster-internal, single-cluster FIFO queue. Each logical queue
/// is backed by one coordinator grain over a reserved system tree; entries
/// are appended at the tail and consumed from the head in insertion order.
/// <para>
/// The queue is strictly cluster-internal: it is <b>not</b> a CRDT-replicated
/// primitive and never ships a <c>LatticeMergeMode</c>. Destructive dequeue
/// is fundamentally non-monotonic, so coordination-free cross-cluster FIFO is
/// outside the library's CRDT-merge model.
/// </para>
/// <para>
/// Throughput is bounded by the single coordinator grain (FIFO is the
/// contract, so sharding cannot relieve this). Applications needing higher
/// throughput should fan work across several independently-named queues
/// (partitioned lanes) and hash a producer key to a lane.
/// <see cref="ListAsync(System.Threading.CancellationToken)"/> is an
/// O(shards) fan-out and is intended for diagnostic / control-plane use, not
/// the hot path.
/// </para>
/// </summary>
/// <typeparam name="T">The queued value type.</typeparam>
public interface ILatticeQueue<T>
{
    /// <summary>
    /// Appends <paramref name="item"/> to the tail and returns the monotonic
    /// id assigned to it. When the backing tree's
    /// <see cref="LatticeOptions.QueueCapacity"/> is set and reached, the
    /// oldest entry is evicted first (FIFO eviction).
    /// </summary>
    Task<long> EnqueueAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes and returns the head entry, or <see langword="null"/> when the
    /// queue is empty.
    /// </summary>
    Task<LatticeQueueEntry<T>?> TryDequeueAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the head entry without removing it, or <see langword="null"/>
    /// when the queue is empty.
    /// </summary>
    Task<LatticeQueueEntry<T>?> PeekAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the number of entries currently parked.</summary>
    Task<int> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns an ascending-id snapshot of every parked entry. This is a
    /// fan-out scan across the backing tree's shards and is intended for
    /// diagnostics; prefer <see cref="CountAsync(System.Threading.CancellationToken)"/>
    /// and <see cref="PeekAsync(System.Threading.CancellationToken)"/> on the
    /// hot path.
    /// </summary>
    Task<IReadOnlyList<LatticeQueueEntry<T>>> ListAsync(CancellationToken cancellationToken = default);
}
