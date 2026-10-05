namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which fix a <see cref="WalDurabilityLifecycleModel"/> run removes. Each guard
/// removes exactly one, and its companion test requires the violation Coyote
/// finds to be reported by the one assertion that fix protects.
/// </summary>
public enum WalDurabilityLifecycleGuard
{
    /// <summary>Every fix in place.</summary>
    None,

    /// <summary>
    /// The pin is resolved against the CURRENT checkpoint (persisted or pending)
    /// rather than the persisted one, as before issue #3476. Must be caught by
    /// <c>[PublishedPinWithinPersistedBelief]</c>.
    /// </summary>
    PinFromPendingCheckpoint,

    /// <summary>
    /// A failed checkpoint persist is not rolled back, so the activation goes on
    /// believing it persisted an advance storage never received, as before issue
    /// #4017. Must be caught by <c>[PersistedBeliefHonest]</c>.
    /// </summary>
    NoRollbackOnFailedPersist,

    /// <summary>
    /// A reader is clamped at the allocator's next offset rather than at the
    /// durable-contiguous watermark. Must be caught by <c>[ShippingNeverSkips]</c>.
    /// </summary>
    ReaderIgnoresWatermark,

    /// <summary>
    /// The GC's offset floor is the HIGHEST published pin rather than the
    /// lowest. Must be caught by <c>[TrimCoveredBySnapshot]</c>.
    /// </summary>
    TrimFloorFromHighestPin,

    /// <summary>
    /// A never-written leaf's release ignores the snapshot coverage it holds and
    /// is published at its persisted checkpoint, as before issue #4456 (and, with
    /// no coverage, before issue #4523). Must be caught by
    /// <c>[ReleaseBackedBySnapshot]</c> at the publication and, with that check
    /// off and one leaf owning nothing, by <c>[RecoveryNeverFallsOffLog]</c>.
    /// </summary>
    NeverWrittenReleaseIgnoresCoverage,

    /// <summary>
    /// An append is acknowledged when its offset is assigned rather than once its
    /// flush has landed (<c>WalShardGrain.AppendAsync</c> completes only after the
    /// flush). Must be caught by <c>[AckedWriteDurable]</c>.
    /// </summary>
    AckBeforeFlush,

    /// <summary>
    /// A cold start (no snapshot to load) resumes reading at the persisted
    /// checkpoint over an empty projection instead of replaying the readable WAL
    /// from its start (<c>LeafReplayStartPolicy</c>'s cold override), so the leaf's
    /// read position passes writes its projection never received. Must be caught
    /// by <c>[ReadPositionHonest]</c>.
    /// </summary>
    ColdStartResumesFromCheckpoint,

    /// <summary>
    /// The replay never reads past the checkpoint the leaf persisted, so a write
    /// acknowledged while its owner was not applying it is never materialised.
    /// Must be caught by <c>[EveryAckedWriteMaterialised]</c>.
    /// </summary>
    ReplayStopsAtPersistedCheckpoint,

    /// <summary>
    /// The read position advances only over entries the leaf owns, as before
    /// issue #2270, so a leaf that owns nothing in the partition never advances,
    /// keeps its block pin and holds the GC. Must be caught by
    /// <c>[ReclamationEventuallyAdvances]</c>, with one leaf owning nothing.
    /// </summary>
    ReadPositionTracksOwnEntries,
}
