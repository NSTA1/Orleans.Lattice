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
}
