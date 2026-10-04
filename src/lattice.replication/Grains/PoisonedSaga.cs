namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A saga this shipper has poisoned for its peer (#4494): a prepare of the saga
/// was parked on the dead-letter queue instead of shipped, so the peer never
/// stages that write. Every later prepare and every terminal of the saga is
/// parked too, so the peer keeps the saga invisible rather than committing it
/// without the lost write.
/// <para>
/// The entry retires only once the saga can produce no further WAL record:
/// the origin registry has been seen to hold the saga's decision and later to
/// hold no row for it (a terminal, including a split sweep's late terminal,
/// needs a recorded decision), and the shipper's durable cursor has passed
/// every partition tail sampled after that observation. A count of the saga's
/// terminals is not a bound: an unstamped or late sweep terminal can follow the
/// stamped ones.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.PoisonedSaga)]
internal sealed class PoisonedSaga
{
    /// <summary>The poisoned saga's transaction id.</summary>
    [Id(0)] public Guid TransactionId { get; set; }

    /// <summary>
    /// Set once the saga is known to be decided: the origin registry reported
    /// its decision, or a terminal of it was parked. Only after that does a
    /// registry with no row for the saga mean the row was purged rather than
    /// not yet written.
    /// </summary>
    [Id(1)] public bool Decided { get; set; }

    /// <summary>
    /// The source-log partition tails sampled after the origin registry was
    /// seen to hold no row for the decided saga, or <see langword="null"/> until
    /// then. The entry retires once every durable partition cursor has reached
    /// its tail here.
    /// </summary>
    [Id(2)] public long[]? RetireAfterTails { get; set; }

    /// <summary>
    /// UTC ticks at which the origin registry was first seen holding no row for
    /// the decided saga, or <c>0</c>. The tails are sampled only once that has
    /// held for the retirement grace, so a split sweep that read the decision
    /// just before the purge has appended its terminal below the sample.
    /// </summary>
    [Id(3)] public long AbsentSinceUtcTicks { get; set; }
}
