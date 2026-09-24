namespace Orleans.Lattice;

/// <summary>
/// Which durable-pin state an <i>absent</i> consumer whose pin the WAL GC has
/// singled out is actually in.
/// <para>
/// <b>Two populations, and the premise that separates them.</b> Members are
/// recorded from two call sites. The <i>blocked</i> arm classifies a consumer
/// that <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/> named, so the
/// pin is known unusable before the read begins. The <i>floor-holder</i> arm
/// (issue #3158) classifies the oldest pins on a tree whose floor reports
/// usable, so the pin is <b>not</b> known to be unusable - it may simply be the
/// oldest healthy one. <see cref="CheckpointedUncovered"/> is sound only under
/// the first premise, which is why
/// <see cref="CheckpointedCoverageUnknown"/> exists (issue #3168).
/// </para>
/// <para>
/// <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/> says a pass was
/// blocked; it cannot say <i>why</i>, because the leaf publishes
/// <see cref="HybridLogicalClock.Zero"/> with offset <c>-1</c> for
/// both routes into the block and the durable pin record therefore preserves
/// no difference between them. The two routes call for opposite responses -
/// one is correct behaviour that must never be "repaired", the other is a
/// genuine coverage hole - so collapsing them leaves both a fix and a decision
/// not to fix equally unfounded (issue #3042).
/// </para>
/// <para>
/// The discriminator is not reachable from the pin. It is the leaf's own
/// durable <see cref="BPlusTree.State.LeafNodeState.ProjectionCheckpointOffsetsByPartition"/>,
/// which the scheduler reads <i>directly from the storage provider</i> without
/// activating the leaf - the population this exists to measure is precisely the
/// population that cannot be activated, so an instrument that needed the leaf
/// live would measure the wrong leaves.
/// </para>
/// <para>
/// Diagnostic only. This value never widens or narrows the trim predicate: a
/// pass reclaims exactly the entries it would have reclaimed before it existed.
/// In particular it must never be turned into a published trim entitlement -
/// see the retention rationale on
/// <c>BPlusLeafGrain.Snapshot.cs</c>, which names that as the
/// silent-data-loss-on-upgrade shape.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(LatticeWalGcScheduler),
    "orleans.lattice.wal.gc.blocking_pin_state",
    LatticeMetrics.TagStatus)]
public enum WalGcBlockingPinState
{
    /// <summary>
    /// The leaf has durably checkpointed this partition (its persisted
    /// per-partition projection checkpoint is <c>&gt;= 0</c>) but published an
    /// unusable pin anyway, because the snapshot coverage it is floored against
    /// is absent.
    /// <para>
    /// This is the <i>repairable</i> state. The checkpoint is real and durable,
    /// so there is a WAL offset the leaf could honestly claim; what is missing
    /// is the coverage half of <c>min(checkpoint, covered)</c>, which is
    /// per-activation in-memory state populated only on snapshot capture or
    /// load. A remedy that restores coverage would clear this block.
    /// </para>
    /// <para>
    /// <b>The "uncovered" half is inferred, never measured, so it is only as
    /// sound as its premise.</b> The classifier reads the checkpoint and stops;
    /// coverage lives in per-activation memory it cannot reach. What licenses
    /// the second half is knowing independently that the pin is unusable:
    /// <c>ResolveDurablePinForPartition</c> publishes
    /// <c>min(checkpoint, covered)</c>, so an unusable pin with a checkpoint
    /// <c>&gt;= 0</c> entails coverage is absent. That premise holds on the
    /// blocked arm by construction and <b>fails</b> on the floor-holder arm,
    /// where a pin is sampled for being oldest rather than for being unusable.
    /// A usable pin sampled there is <see cref="CheckpointedCoverageUnknown"/>,
    /// not this state (issue #3168).
    /// </para>
    /// </summary>
    CheckpointedUncovered = 0,

    /// <summary>
    /// The leaf has never durably checkpointed this partition (no persisted
    /// per-partition checkpoint, or the sentinel <c>-1</c>), yet holds live
    /// data.
    /// <para>
    /// This is the <i>correct</i> state, and it is deliberately retained by
    /// design: there is no WAL offset the leaf could honestly claim, so
    /// publishing anything other than a blocking pin would entitle the GC to
    /// trim entries the leaf has never applied. No repair exists and none
    /// should. A tree blocked only by consumers in this state is not defective -
    /// it is a tree whose WAL genuinely cannot be reclaimed yet, and the
    /// remedy lies in getting the leaf to checkpoint, not in changing the pin.
    /// </para>
    /// </summary>
    NeverCheckpointed = 1,

    /// <summary>
    /// The leaf has no durable state blob at all: the storage provider was
    /// reached and answered, and reported that nothing has ever been persisted
    /// for this grain.
    /// <para>
    /// This is a <i>fourth</i> thing, not a flavour of either state above, and
    /// it is deliberately not folded into <see cref="NeverCheckpointed"/>.
    /// Folding it there would assert "the leaf holds live data it has never
    /// checkpointed" on the strength of an absence - which is precisely the
    /// merge that
    /// <c>BPlusLeafGrain.CursorRegistry.ResolveDurablePinForPartition</c>
    /// already performs and that this enum exists to undo. A block held by
    /// consumers in this state is neither repairable nor correct-by-design, and
    /// should be read as a finding in its own right.
    /// </para>
    /// </summary>
    NoDurableState = 2,

    /// <summary>
    /// The classifier could not answer for this consumer, so no claim is made
    /// about the leaf at all.
    /// <para>
    /// Separated from <see cref="NoDurableState"/> on the same reasoning that
    /// separates that arm from <see cref="NeverCheckpointed"/>: this arm
    /// reports a failure of the <i>instrument</i>, whereas the other three
    /// report a property of the <i>leaf</i>. Collapsing them would let a silo
    /// with no storage provider registered present as an estate of leaves that
    /// have never persisted anything - a defect in the measurement rendered as
    /// a finding about the system.
    /// </para>
    /// <para>
    /// The routes in are a consumer id that does not parse back to a grain id
    /// and partition, no storage provider registered on this silo, and a read
    /// that threw.
    /// </para>
    /// </summary>
    Unreadable = 3,

    /// <summary>
    /// The leaf's durable state blob exists but carries no bound tree id: the
    /// leaf was reclaimed or purged after publishing this pin, leaving a husk
    /// behind. The pin has outlived its publisher and can never be lifted by
    /// the leaf, so it is retired outright rather than driven (issue #3105).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why a husk is proof rather than a guess.</b> A leaf's pin registration
    /// is birth-gated on a persisted tree id, so a durable pin can only exist if
    /// the leaf's state carried one at the moment the pin was written. Reading
    /// that state back and finding no tree id therefore establishes that the
    /// state was cleared <i>after</i> the pin was published - the exact
    /// condition <c>DriveStarvedCheckpointAsync</c> reports as
    /// <c>NotDriven</c>, reached without activating the leaf.
    /// </para>
    /// <para>
    /// <b>Why this arm has to exist separately.</b> Before it did, a husk was
    /// classified by <c>ClassifyCheckpoint</c>, which reads only the persisted
    /// projection checkpoint and never the tree id. A husk retains whatever
    /// checkpoint offset it last wrote, so it classified as
    /// <see cref="CheckpointedUncovered"/> - "repairable" - and an estate of
    /// 9,468 orphaned pins on one tree presented as a coverage problem that a
    /// snapshot would fix. The one diagnostic that could have revealed the
    /// backlog instead concealed it, which is why the misclassification is part
    /// of the defect and not a cosmetic detail of it.
    /// </para>
    /// </remarks>
    Orphaned = 4,

    /// <summary>
    /// The leaf has durably checkpointed this partition, but its pin is
    /// <b>not</b> known to be unusable - so whether that checkpoint is covered
    /// is not determinable from durable state, and this arm deliberately makes
    /// no claim either way.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>What it measures.</b> Exactly what
    /// <see cref="CheckpointedUncovered"/> measures - a persisted per-partition
    /// projection checkpoint <c>&gt;= 0</c> - and nothing more. The two arms
    /// read the same byte. They differ only in whether the premise that
    /// licenses the <i>uncovered</i> conclusion was available, and that premise
    /// is a property of the call site, not of the leaf.
    /// </para>
    /// <para>
    /// <b>Why it had to exist (issue #3168).</b> The floor-holder sample runs
    /// only when the cursor floor reports usable, which is true only when no
    /// dormant pin resolved to the blocking sentinel - so every pin it samples
    /// is, at that moment, usable. Classifying those as
    /// <see cref="CheckpointedUncovered"/> asserted a coverage hole in the one
    /// population structurally guaranteed not to have one. On a live estate
    /// that read all eight partitions of a healthy, fully-covered, merely idle
    /// leaf as repairable, and issue #3164 drove it into the reactivation
    /// remedy every pass; the leaf answered
    /// <c>no_checkpointed_uncovered_partition</c> over four thousand times,
    /// which was the leaf being right.
    /// </para>
    /// <para>
    /// <b>Not a coverage defect, and that is the point.</b> This arm is excluded
    /// from the set #3164 drives <i>for coverage</i>. There is no coverage hole
    /// to repair, so a reactivation undertaken to close one can only cost an
    /// activation and report that it found nothing. A tree sitting on this arm
    /// is not defective in the way #3164 addresses: its WAL floor is held by a
    /// pin that is healthy and simply old, which is a frontier-advance question
    /// rather than a coverage one.
    /// </para>
    /// <para>
    /// <b>That advance question has since been answered (issue #3178), and it
    /// does not reopen this one.</b> The offset half of such a pin is a
    /// scanned-through projection checkpoint, which advances only during replay
    /// - that is, only while the leaf is activated - so a leaf that deactivates
    /// freezes it at its exit position, and on a converged corpus nothing ever
    /// reactivates it. Being a minimum over every leaf, the tree's offset floor
    /// is then held indefinitely by whichever dormant leaf sits lowest, and the
    /// WAL cannot be trimmed by a byte although no leaf is blocked and no pass
    /// fails. The remedy is the same activation, sought for a different reason:
    /// a pin on this arm whose durable offset equals the tree's offset floor is
    /// driven for <i>liveness</i>, not for coverage. It remains true that
    /// nothing here asserts a coverage hole, and a pin on this arm sitting above
    /// the floor is still not driven at all - it is not in the way.
    /// </para>
    /// <para>
    /// <b>Why it is honest rather than merely cautious.</b> Naming it
    /// <c>CheckpointedCovered</c> would be the same error running the other
    /// way. The durable pin store merges frontiers by monotonic max, so a
    /// positive stored frontier is a high-water mark and not a live statement
    /// of usability; coverage still cannot be read without activating the leaf.
    /// This arm claims only what was measured.
    /// </para>
    /// </remarks>
    CheckpointedCoverageUnknown = 5,
}
