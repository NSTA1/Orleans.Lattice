namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Legacy: a saga an earlier build poisoned for its peer (#4494) after parking a
/// prepare of it that could not be encoded. Nothing creates one any more: an
/// encode failure takes the peer off the log instead (#4614), and a shipper
/// that activates with entries in <see cref="ReplicationShipperState.PoisonedSagas"/>
/// takes the peer off the log and forgets them. The type and its alias stay so
/// persisted state still decodes.
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
