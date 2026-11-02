namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The pure decisions of the WAL append fence (epic #5605), kept apart from
/// <see cref="WalShardGrain"/> so each guard is unit-testable and maps one to one
/// onto the guard of the draft model <c>spec/wal/design/WalLiveAdvance.tla</c>
/// that it implements: <see cref="DecideClaimWrite"/> is <c>ClaimWrite</c>,
/// <see cref="IsRaiseAdmitted"/> is <c>ClaimRaise</c>, and
/// <see cref="IsEpochAdmitted"/> is <c>OwnAlloc</c>.
/// </summary>
/// <remarks>
/// An in-memory entry of <see cref="Unloaded"/> means the WAL shard activation has
/// not loaded the entry since it activated. Nothing is ever admitted from an
/// unloaded entry (<c>StaleEpochNeverAppendsUnloadedEntryAccepted</c>); the leaf's
/// re-presented claim loads it.
/// </remarks>
internal static class WalAppendFenceCore
{
    /// <summary>The in-memory value of an entry this activation has not loaded.</summary>
    public const long Unloaded = -1;

    /// <summary>What a claim does with the durable entry row.</summary>
    public enum ClaimWriteDecision
    {
        /// <summary>Refuse the claim: it is not above the entry, or not above the retired floor.</summary>
        Refuse,

        /// <summary>Max-merge the claimed value into the durable row, conditionally on its ETag.</summary>
        Write,

        /// <summary>
        /// The durable row already holds exactly this value: the same claimant is
        /// re-presenting its claim (after a WAL shard restart or move, or a lost
        /// response), so nothing is written and the claim proceeds to its raise,
        /// which loads the entry.
        /// </summary>
        AlreadyDurable,
    }

    /// <summary>
    /// The claim's entry write (step 2). A claim is written only strictly above the
    /// higher of the loaded entry, the durable entry and the retired floor, so the
    /// row never moves down (<c>StaleEpochNeverAppendsClaimEntryOverwrite</c>) and
    /// never lands at or below a retirement (<c>IgnoresRetiredFloor</c>). The one
    /// exception is a value equal to the durable entry and not below the loaded
    /// one: only the claimant that wrote it can hold it, because the leaf's epoch
    /// row is written by a strict conditional write.
    /// </summary>
    /// <param name="value">The claimed epoch or generation.</param>
    /// <param name="loaded">The in-memory entry, or <see cref="Unloaded"/>.</param>
    /// <param name="durable">The durable entry just read; 0 when absent.</param>
    /// <param name="floor">The partition's retired floor; 0 for a routing generation.</param>
    public static ClaimWriteDecision DecideClaimWrite(long value, long loaded, long durable, long floor)
    {
        // RED-PROOF: retired floor ignored at the claim write

        if (value > Math.Max(loaded, durable))
        {
            return ClaimWriteDecision.Write;
        }

        return value == durable && value >= loaded
            ? ClaimWriteDecision.AlreadyDurable
            : ClaimWriteDecision.Refuse;
    }

    /// <summary>
    /// The claim's raise (step 3), under the lock that assigns offsets. Refused if
    /// another claim has written or raised the entry above this value since the
    /// write, or if a retirement since the write has raised the floor to it.
    /// </summary>
    /// <param name="value">The claimed epoch or generation.</param>
    /// <param name="loaded">The in-memory entry, or <see cref="Unloaded"/>.</param>
    /// <param name="durableSeen">The highest durable entry this activation has read or written.</param>
    /// <param name="floor">The partition's retired floor; 0 for a routing generation.</param>
    public static bool IsRaiseAdmitted(long value, long loaded, long durableSeen, long floor) =>
        value >= Math.Max(loaded, durableSeen); // RED-PROOF: floor ignored at the raise

    /// <summary>
    /// A stamped append's epoch check, under the lock that assigns its offset. The
    /// entry must be loaded and the epoch not below it. The caller has already
    /// ruled out an unloaded entry.
    /// </summary>
    /// <remarks>
    /// The partition's retired floor is deliberately not checked here (#5635). The
    /// floor is one scalar per partition, while epochs are per leaf, so checking it
    /// at every append would reject every live leaf whose epoch sits at or below
    /// any other leaf's retired entry: one retirement would supersede, and
    /// deactivate, every such leaf of the partition. The floor protects a leaf
    /// re-created under a retired id, and for that the claim's write and raise
    /// checks suffice (the design's redundancy row for
    /// <c>StaleEpochNeverAppendsAppendIgnoresRetiredFloor</c>): retirement removes
    /// the retired leaf's own entry, so its later appends meet an unloaded entry,
    /// and a claim straddling the retirement is refused at its raise.
    /// </remarks>
    /// <param name="epoch">The stamp's epoch.</param>
    /// <param name="loaded">The loaded in-memory entry (never <see cref="Unloaded"/>).</param>
    public static bool IsEpochAdmitted(long epoch, long loaded) =>
        loaded != Unloaded; // RED-PROOF: stale epoch admitted

    /// <summary>
    /// A stamped append's routing-generation check: the initiator's entry must be
    /// loaded and the generation not below it.
    /// </summary>
    public static bool IsGenerationAdmitted(long generation, long loaded) =>
        loaded != Unloaded && generation >= loaded;

    /// <summary>The value a refused claim must exceed.</summary>
    public static long ClaimThreshold(long loaded, long durable, long floor) =>
        Math.Max(Math.Max(loaded, durable), floor);
}
