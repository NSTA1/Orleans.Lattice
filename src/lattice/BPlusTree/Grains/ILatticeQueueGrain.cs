using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal coordinator grain backing one logical cluster-internal queue.
/// One activation per logical queue (keyed by the queue name) serializes
/// all FIFO operations against a reserved
/// <c>_lattice_queue_{queueName}</c> system tree, which is what makes the
/// single-coordinator grain - rather than sharding - the throughput
/// ceiling: FIFO ordering is the contract, so applications that need more
/// throughput should fan work across several independently-named queues
/// (partitioned lanes) and hash a producer key to a lane.
/// <para>
/// The grain operates on opaque <c>byte[]</c> payloads; the typed client
/// surface <see cref="ILatticeQueue{T}"/> performs the
/// <see cref="ILatticeSerializer{T}"/> round-trip. Because this interface
/// is <see langword="internal"/>, user code in other assemblies cannot
/// resolve it directly and always goes through the typed facade.
/// </para>
/// </summary>
[Alias(TypeAliases.ILatticeQueueGrain)]
[EditorBrowsable(EditorBrowsableState.Never)]
internal interface ILatticeQueueGrain : IGrainWithStringKey
{
    /// <summary>
    /// Appends <paramref name="value"/> and returns the assigned monotonic
    /// id. When the backing tree's <see cref="LatticeOptions.QueueCapacity"/>
    /// is set and reached, the oldest entry is evicted first (FIFO).
    /// </summary>
    Task<long> EnqueueAsync(byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Removes and returns the head entry, or <see langword="null"/> when the queue is empty.</summary>
    Task<LatticeQueueByteEntry?> TryDequeueAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the head entry without removing it, or <see langword="null"/> when empty.</summary>
    Task<LatticeQueueByteEntry?> PeekAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the number of entries currently parked.</summary>
    Task<int> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns every parked entry in ascending-id order. Empty when the queue is empty.</summary>
    Task<IReadOnlyList<LatticeQueueByteEntry>> ListAsync(CancellationToken cancellationToken = default);
}
