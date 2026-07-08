namespace Orleans.Lattice.Replication;

/// <summary>
/// Read-only snapshot of the durable per-tree write-fence and shipping-pause
/// primitive's state (<see cref="Grains.ISagaWriteFenceGrain.GetSnapshotAsync"/>).
/// Exposed for diagnostics and tests so callers can observe the fence phase,
/// the fenced tree group, and the two release points without mutating state.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SagaWriteFenceSnapshot)]
[Immutable]
internal readonly record struct SagaWriteFenceSnapshot
{
    /// <summary>Identifier of the saga that engaged the fence, or empty when none.</summary>
    [Id(0)]
    public string SagaId { get; init; }

    /// <summary>Current lifecycle phase of the fence.</summary>
    [Id(1)]
    public SagaWriteFencePhase Phase { get; init; }

    /// <summary>The fenced tree group hosted on the local cluster.</summary>
    [Id(2)]
    public List<string> Trees { get; init; }

    /// <summary>Absolute UTC tick at which the write fence self-lifts.</summary>
    [Id(3)]
    public long FenceDeadlineTicks { get; init; }

    /// <summary>
    /// <see langword="true"/> once the local write fence has been lifted (the
    /// local flip release point), whether by an explicit unblock or the
    /// self-lifting deadline.
    /// </summary>
    [Id(4)]
    public bool WritesUnblocked { get; init; }

    /// <summary>
    /// <see langword="true"/> once shipping and receiving have resumed after
    /// observed global saga completion (the cross-cluster release point).
    /// </summary>
    [Id(5)]
    public bool ShippingResumed { get; init; }
}
