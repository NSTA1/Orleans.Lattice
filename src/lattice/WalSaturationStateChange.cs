namespace Orleans.Lattice;

/// <summary>
/// Payload delivered to every registered
/// <see cref="IWalSaturationObserver"/> on each per-tree transition of
/// the WAL saturation signal. Carries the tree id, the previous and new
/// states, the partition and shard the sampler associated with the tree in
/// that sample window, the input the transition was attributed to, and the
/// wall-clock instant at which the transition was observed by the sampler.
/// <para>
/// Observers may use the attribution slots to graph hotspot partitions
/// or shards, but the slots are filled independently of
/// <see cref="Cause"/>: they record where admission depth and trips were seen
/// in the sample window, not which input drove the transition, so a slot can
/// be populated on a transition some other input drove, including a
/// transition back to <see cref="WalSaturationState.Healthy"/>. Read them
/// together with <see cref="Cause"/> rather than as the cause.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalSaturationStateChange)]
[Immutable]
public readonly record struct WalSaturationStateChange
{
    /// <summary>The WAL tree id whose saturation state changed.</summary>
    [Id(0)]
    public string TreeId { get; init; }

    /// <summary>The state the tree was in before this transition.</summary>
    [Id(1)]
    public WalSaturationState PreviousState { get; init; }

    /// <summary>The state the tree is now in.</summary>
    [Id(2)]
    public WalSaturationState NewState { get; init; }

    /// <summary>
    /// The writer partition whose admission semaphore had the highest depth
    /// (in-flight appends over its cap) among the tree's partitions when the
    /// sampler read them. <c>null</c> when no partition had an append in flight
    /// against a bounded semaphore at that moment. Filled whatever
    /// <see cref="Cause"/> the transition carries, so it can name a partition on
    /// a transition driven by another input.
    /// </summary>
    [Id(3)]
    public int? AttributedPartition { get; init; }

    /// <summary>
    /// The first shard the sampler found with any dispatch-timeout,
    /// provider-failure, flush-latency or durable pin-write latency trip in the
    /// sample window, whether or not that input crossed its own threshold; when
    /// several shards recorded trips, which one is reported is not specified.
    /// For the first three inputs the index is the tree's WAL partition; for a
    /// durable pin-write trip it is a materialiser pin-store shard (see
    /// <see cref="LatticeOptions.WalMaterialiserPinShards"/>), so the two index
    /// spaces are not interchangeable. <c>null</c> when no shard recorded a
    /// trip. Like <see cref="AttributedPartition"/>, it is filled whatever
    /// <see cref="Cause"/> the transition carries.
    /// </summary>
    [Id(4)]
    public int? AttributedShard { get; init; }

    /// <summary>
    /// Wall-clock instant at which the sampler observed the transition.
    /// </summary>
    [Id(5)]
    public DateTimeOffset ObservedAt { get; init; }

    /// <summary>
    /// Which sampler input the transition was attributed to. Several inputs map
    /// to the same <see cref="WalSaturationState"/>, so the state alone does not
    /// identify the subsystem under pressure; this names it. Best-effort and
    /// single-valued: when several inputs cross in the same window the first
    /// evaluated is reported. <see cref="WalSaturationCause.None"/> on every
    /// transition back to <see cref="WalSaturationState.Healthy"/>, on a
    /// transition to <see cref="WalSaturationState.Throttled"/> that only the
    /// <see cref="LatticeOptions.WalSaturationRecoveryWindow"/> hold produced,
    /// and on any transition published by a host predating cause attribution.
    /// </summary>
    [Id(6)]
    public WalSaturationCause Cause { get; init; }
}
