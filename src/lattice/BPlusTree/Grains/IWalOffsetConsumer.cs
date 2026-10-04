using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A WAL consumer that reads partitions by offset and keeps its own durable
/// read position, so the WAL GC can ask it, from any silo, how far it has
/// consumed each partition (issue #4579). The replication shipper is one.
/// <para>
/// The GC needs this because the HLC cursor a consumer reports cannot protect
/// it. A WAL partition is not HLC-ordered in offset (a skewed silo clock, a
/// merge that keeps its source stamp), so an entry the consumer has not read
/// can carry an HLC at or below a cursor it has already reported.
/// </para>
/// <para>
/// A consumer registers itself with the tree's
/// <see cref="IWalOffsetConsumerRegistryGrain"/> before it reads that log, and
/// the GC asks every registered consumer on every pass, so the answer is the
/// consumer's own persisted state rather than a process-local report.
/// </para>
/// </summary>
[Alias(TypeAliases.IWalOffsetConsumer)]
internal interface IWalOffsetConsumer : IGrain
{
    /// <summary>
    /// Returns, per partition of <paramref name="walTreeId"/>'s WAL, the lowest
    /// offset this consumer has <b>not</b> durably consumed. The GC must not
    /// trim an entry at or above <c>result[p]</c> in partition <c>p</c> unless
    /// the retention ceiling admits it. A partition at or beyond the result's
    /// length is treated as position 0 (fail closed), so a partition count that
    /// grows under a running consumer is held until the consumer reports it.
    /// A consumer that has registered but not yet read reports 0 for every
    /// partition. Returns <see langword="null"/> when the consumer no longer
    /// reads <paramref name="walTreeId"/>.
    /// </summary>
    /// <param name="walTreeId">The physical tree id whose WAL is asked about.</param>
    /// <remarks>
    /// Interleaved: the answer is read from an immutable snapshot the consumer
    /// replaces only after its position is persisted, so a GC pass never waits
    /// behind a ship round-trip.
    /// </remarks>
    [AlwaysInterleave]
    Task<long[]?> GetDurableReadPositionsAsync(string walTreeId);
}
