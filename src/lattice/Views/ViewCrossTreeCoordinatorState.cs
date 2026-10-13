
namespace Orleans.Lattice.Views;

/// <summary>
/// Durable decision state of the view-side cross-tree coordinator. Persisted
/// before every return so the wait set, the recorded per-view slices, and the
/// joint-flip decision survive a crash and a redelivered registration re-heals
/// idempotently (mirroring the receiver-side
/// <c>CrossTreeReceiverState</c> discipline).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewCrossTreeCoordinatorState)]
internal sealed class ViewCrossTreeCoordinatorState
{
    internal const int CurrentRepairProtocolVersion = 1;

    /// <summary>
    /// The frozen view wait set (canonicalised: ordinal-sorted, de-duplicated),
    /// empty until the first registration freezes it.
    /// </summary>
    [Id(0)] public List<string> WaitSet { get; set; } = [];

    /// <summary>
    /// The per-view recorded ready slices, keyed by view name. Once
    /// <see cref="Decided"/> is persisted, their payloads and physical targets
    /// are frozen for every retry of the same joint operation.
    /// </summary>
    [Id(1)] public Dictionary<string, ViewCrossTreeSlice> Slices { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// <c>true</c> once the wait set has completed and the decision (to flip)
    /// has been persisted - the joint-flip intent. The joint cross-tree write is
    /// idempotent, so a crash after this is set but before <see cref="Applied"/>
    /// is re-driven by a redelivered registration.
    /// </summary>
    [Id(2)] public bool Decided { get; set; }

    /// <summary>
    /// <c>true</c> once the joint cross-tree flip has durably committed across
    /// every participant view tree. A redelivered registration after this is set
    /// returns the memoized applied decision without re-issuing the flip.
    /// </summary>
    [Id(3)] public bool Applied { get; set; }

    /// <summary>UTC ticks at which the first registration was recorded.</summary>
    [Id(4)] public long StartedAtTicks { get; set; }

    /// <summary>
    /// <c>true</c> once a participant timed out waiting for the joint flip and
    /// terminally degraded the operation. Set only when <see cref="Applied"/> is
    /// and <see cref="Decided"/> are still <c>false</c> (no joint intent exists);
    /// once set, the coordinator
    /// never issues the joint flip and every registration returns the degraded
    /// decision so every participant flips its own slice locally instead.
    /// </summary>
    [Id(5)] public bool Degraded { get; set; }

    /// <summary>
    /// Fresh participant contributions for the post-publication joint repair.
    /// Independent of <see cref="Slices"/>; frozen when <see cref="RepairDecided"/>
    /// is persisted. All original participants must contribute before publication.
    /// </summary>
    [Id(6)] public Dictionary<string, ViewCrossTreeSlice> RepairSlices { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The durable intent to publish the complete joint repair set.</summary>
    [Id(7)] public bool RepairDecided { get; set; }

    /// <summary>
    /// The repair publication committed. Retried registrations return its
    /// memoized result without writing either the original or repair payload.
    /// </summary>
    [Id(8)] public bool RepairApplied { get; set; }

    /// <summary>
    /// Participants whose repaired source batch has been durably retired from
    /// their checkpoint. Cleanup waits for all original participants.
    /// </summary>
    [Id(9)] public List<string> RepairAcknowledged { get; set; } = [];

    /// <summary>
    /// The admitted repair protocol. Missing in released states (zero): those
    /// operations finish and expire under their original contract, without
    /// certifying fresh repair. New admissions persist the current version.
    /// </summary>
    [Id(10)] public int RepairProtocolVersion { get; set; }
}
