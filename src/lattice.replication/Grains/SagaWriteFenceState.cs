namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable state for <see cref="ISagaWriteFenceGrain"/>. Persisted so a
/// coordinator crash mid-cutover never strands a tree write-fenced: on
/// reactivation the phase and deadline are recovered and the self-lifting timer
/// is re-armed.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SagaWriteFenceState)]
internal sealed class SagaWriteFenceState
{
    /// <summary>Identifier of the saga that engaged the fence, or <c>null</c> when idle.</summary>
    [Id(0)]
    public string? SagaId { get; set; }

    /// <summary>The group of locally-hosted trees fenced and lifted as one unit.</summary>
    [Id(1)]
    public List<string> Trees { get; set; } = [];

    /// <summary>Current lifecycle phase.</summary>
    [Id(2)]
    public SagaWriteFencePhase Phase { get; set; } = SagaWriteFencePhase.None;

    /// <summary>
    /// Absolute UTC tick at which the write fence self-lifts. Sized for the
    /// bounded cutover window so a stranded coordinator cannot fence writes
    /// forever.
    /// </summary>
    [Id(3)]
    public long FenceDeadlineTicks { get; set; }

    /// <summary>Cluster id whose coordinator's global completion gates shipping resume.</summary>
    [Id(4)]
    public string? CoordinatorClusterId { get; set; }

    /// <summary>
    /// <see langword="true"/> once the local write fence has been lifted (local
    /// flip or self-lifting deadline). Distinct from
    /// <see cref="ShippingResumed"/> - the two release points are never
    /// conflated.
    /// </summary>
    [Id(5)]
    public bool WritesUnblocked { get; set; }

    /// <summary>
    /// <see langword="true"/> once shipping and receiving have resumed after
    /// observed global saga completion.
    /// </summary>
    [Id(6)]
    public bool ShippingResumed { get; set; }

    /// <summary>
    /// Absolute UTC tick at which the write fence was engaged. Used to record the
    /// per-tree write-fence window duration (engage to write-fence lift) as an
    /// observability histogram; persisted so the measurement survives a
    /// reactivation between engage and lift.
    /// </summary>
    [Id(7)]
    public long EngagedAtTicks { get; set; }
}
