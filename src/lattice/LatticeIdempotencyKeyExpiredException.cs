namespace Orleans.Lattice;

/// <summary>
/// Thrown when a write carrying a caller-supplied
/// <see cref="LatticeIdempotencyKey"/> reaches a replicated tree after the key's
/// <see cref="LatticeIdempotencyKey.Timestamp"/> has fallen below the tree's WAL
/// clock floor (issue #4586).
/// <para>
/// A replicated tree's write-ahead log refuses a freshly authored write whose
/// HLC stamp is older than its floor, which trails the wall clock by
/// <see cref="LatticeOptions.ReplicationClockFloorLag"/>. That refusal is what
/// lets a replication receiver decide a causal dependency safely. An ordinary
/// write is re-stamped and retried transparently, but an idempotency key's
/// timestamp is stamped verbatim by contract - so that every retry of the same
/// operation collapses onto one write - and cannot be renewed. The write was
/// not applied.
/// </para>
/// <para>
/// <b>Caller contract.</b> This is a deterministic caller error, not a transient
/// fault: retrying with the same key fails identically. An idempotency key is
/// usable for at least <see cref="LatticeOptions.ReplicationClockFloorLag"/>
/// after it was minted, so mint it (for example with
/// <see cref="LatticeIdempotencyKey.Fresh"/>) when the logical operation starts,
/// keep the client's clock synchronised with the cluster's, and when this is
/// raised read the key's current state before deciding whether to issue the
/// operation again under a fresh key. Trees that are not replicated never raise
/// it.
/// </para>
/// <para>
/// Derives directly from <see cref="Exception"/>, so a same-silo deep copy of a
/// grain result needs no registered copier.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeIdempotencyKeyExpired)]
public sealed class LatticeIdempotencyKeyExpiredException : Exception
{
    /// <summary>
    /// The idempotency key's timestamp that was refused. <see cref="HybridLogicalClock.Zero"/>
    /// on the parameterless and message-only constructors.
    /// </summary>
    [Id(0)]
    public HybridLogicalClock KeyTimestamp { get; }

    /// <summary>
    /// The WAL clock floor the key's timestamp fell below.
    /// <see cref="HybridLogicalClock.Zero"/> on the parameterless and message-only constructors.
    /// </summary>
    [Id(1)]
    public HybridLogicalClock Floor { get; }

    /// <summary>Initialises a new instance with a default message.</summary>
    public LatticeIdempotencyKeyExpiredException()
        : base("The idempotency key is older than the replicated tree's WAL clock floor; the write was not applied.")
    {
    }

    /// <summary>Initialises a new instance with <paramref name="message"/>.</summary>
    /// <param name="message">Caller-facing description of the refusal.</param>
    public LatticeIdempotencyKeyExpiredException(string message)
        : base(message)
    {
    }

    /// <summary>Initialises a new instance with <paramref name="message"/> and <paramref name="innerException"/>.</summary>
    /// <param name="message">Caller-facing description of the refusal.</param>
    /// <param name="innerException">The underlying refusal.</param>
    public LatticeIdempotencyKeyExpiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initialises a new instance for a key whose <paramref name="keyTimestamp"/>
    /// fell below <paramref name="floor"/>.
    /// </summary>
    /// <param name="keyTimestamp">The refused idempotency key timestamp.</param>
    /// <param name="floor">The WAL clock floor it fell below.</param>
    /// <param name="innerException">The underlying refusal, when available.</param>
    public LatticeIdempotencyKeyExpiredException(HybridLogicalClock keyTimestamp, HybridLogicalClock floor, Exception? innerException = null)
        : base(
            $"The idempotency key's timestamp {keyTimestamp} is below the replicated tree's WAL clock floor {floor}: "
            + $"a key is usable for at least {nameof(LatticeOptions)}.{nameof(LatticeOptions.ReplicationClockFloorLag)} after it is minted. "
            + "The write was not applied; read the key's state before re-issuing the operation under a fresh key.",
            innerException)
    {
        KeyTimestamp = keyTimestamp;
        Floor = floor;
    }
}
