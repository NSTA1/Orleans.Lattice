namespace Orleans.Lattice;

/// <summary>
/// Payload delivered to every registered
/// <see cref="IWalSaturationObserver"/> on each per-tree transition of
/// the WAL saturation signal. Carries the tree id, the previous and new
/// states, optional per-partition / per-shard attribution for the
/// underlying signal source, and the wall-clock instant at which the
/// transition was observed by the sampler.
/// <para>
/// Observers may use the attribution slots to graph hotspot partitions
/// or shards; a transition driven by aggregate behaviour (for example
/// dispatch-timeout rate summed across several partitions) leaves both
/// slots <c>null</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalSaturationStateChange)]
[Immutable]
public readonly record struct WalSaturationStateChange
{
    /// <summary>The logical tree id whose saturation state changed.</summary>
    [Id(0)]
    public string TreeId { get; init; }

    /// <summary>The state the tree was in before this transition.</summary>
    [Id(1)]
    public WalSaturationState PreviousState { get; init; }

    /// <summary>The state the tree is now in.</summary>
    [Id(2)]
    public WalSaturationState NewState { get; init; }

    /// <summary>
    /// Optional writer-partition index attributable to this transition
    /// when the source signal is the per-partition admission-semaphore
    /// depth. <c>null</c> when no single partition dominated (for
    /// example a transition driven by dispatch-timeout rate, or by
    /// multiple partitions crossing the threshold in the same sample
    /// window).
    /// </summary>
    [Id(3)]
    public int? AttributedPartition { get; init; }

    /// <summary>
    /// Optional shard index attributable to this transition when the
    /// source signal is recent dispatch-timeout trips. <c>null</c> when
    /// no single shard dominated.
    /// </summary>
    [Id(4)]
    public int? AttributedShard { get; init; }

    /// <summary>
    /// Wall-clock instant at which the sampler observed the transition.
    /// </summary>
    [Id(5)]
    public DateTimeOffset ObservedAt { get; init; }
}
