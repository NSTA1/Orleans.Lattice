namespace Orleans.Lattice;

/// <summary>
/// Which of the two durable-pin states an <i>absent</i> consumer that blocks a
/// WAL GC pass is actually in.
/// <para>
/// <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/> says a pass was
/// blocked; it cannot say <i>why</i>, because the leaf publishes
/// <see cref="Primitives.HybridLogicalClock.Zero"/> with offset <c>-1</c> for
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
}
