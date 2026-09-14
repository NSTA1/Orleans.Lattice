namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a starvation drive achieved on one leaf (issue #2692 Half B). Returned
/// by <see cref="IBPlusLeafGrain.DriveStarvedCheckpointAsync"/> to the WAL GC
/// blocked-leaf sweep.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this is not a <see cref="bool"/>.</b> The sweep's previous touch
/// (<c>GetTreeIdAsync</c>) returned promptly for an already-active leaf, replayed
/// nothing, and was reported as <c>Completed</c> - a value nothing inspected. The
/// sweep was therefore not failing loudly, it was succeeding vacuously, and no
/// series anywhere could distinguish "drove the leaf and lifted its pin" from
/// "touched the leaf and achieved nothing". Collapsing
/// <see cref="NoAdvance"/> into <see cref="Lifted"/>, or either into a boolean,
/// reproduces exactly that defect one layer down.
/// </para>
/// <para>
/// <b>Why the arity matters.</b> A tree cannot trim while <i>any</i> one of its
/// leaves lacks a per-partition checkpoint, so the condition is existential over
/// leaves. Every tree-level series is a sum or a count over leaves and cannot
/// express an existential: a tree with hundreds of healthy leaves and one starved
/// leaf produces a large, healthy-looking aggregate and blocks on every pass.
/// This value is measured per leaf, which is the only arity at which the
/// condition can be stated at all.
/// </para>
/// <para>
/// <b>Why this shares an instrument and a tag with <c>ReactivationOutcome</c>.</b>
/// Both enums arm the <c>outcome</c> tag of
/// <c>orleans.lattice.wal.gc.blocked_leaf_reactivations</c>, which is legitimate
/// and is anticipated by <see cref="InstrumentedEnumAttribute"/>: the arming
/// relation is one-directional, so every member needs an arm but an arm need not
/// come from any one enum. That instrument's tag already carries four lifecycle
/// arms belonging to no enum at all. The two member sets stay disjoint because
/// every arm here is <c>drove_</c>-prefixed, which is what keeps a drive verdict
/// from being read as a terminal reactivation outcome.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LeafStarvationDriveOutcome)]
[InstrumentedEnum(
    typeof(LatticeWalGcScheduler),
    "orleans.lattice.wal.gc.blocked_leaf_reactivations",
    LatticeMetrics.TagOutcome)]
internal enum LeafStarvationDriveOutcome
{
    /// <summary>
    /// The leaf had no tree id bound, so it does no replay and must not consume
    /// a replay permit. Distinguished from <see cref="NoAdvance"/> because
    /// nothing was driven at all: the sweep reached a grain that cannot be
    /// starved rather than one it failed to repair.
    /// </summary>
    NotDriven = 0,

    /// <summary>
    /// Replay ran and the persisted checkpoint advanced, so the leaf's durable
    /// materialiser pin can resolve to a real offset and its partition's WAL
    /// cursor branch is no longer blocked on its account. The only affirmative
    /// reading in this enum.
    /// </summary>
    Lifted = 1,

    /// <summary>
    /// Replay ran cleanly and the persisted checkpoint did <b>not</b> advance.
    /// <para>
    /// This is a real and expected outcome, not a failure: the checkpoint
    /// advance is clamped behind any unresolved prepared-saga mutation and is
    /// re-asserted monotonic before it is written, so a drive can execute in
    /// full, return cleanly, and lift nothing. Reported separately precisely
    /// because it is indistinguishable from <see cref="Lifted"/> at the call
    /// site, and folding the two together is what made the previous sweep
    /// unreadable.
    /// </para>
    /// </summary>
    NoAdvance = 2,

    /// <summary>
    /// The drive was refused, or abandoned, because the process was under heap
    /// pressure - the verdict <c>BPlusLeafGrain.IsReadMemoryPressure</c>
    /// recognises.
    /// <para>
    /// Kept apart from <see cref="NoAdvance"/> because the two call for opposite
    /// responses. A leaf that drove and lifted nothing has been reached and is
    /// blocked on something structural; a leaf refused for memory pressure was
    /// never given the chance, so retrying it once pressure lifts is the correct
    /// remedy rather than a wasted touch. Folding them together would report a
    /// resource stall as a permanent structural block.
    /// </para>
    /// </summary>
    MemoryRefused = 3,

    /// <summary>
    /// A drive was already in flight on this activation, so this call did
    /// nothing rather than stacking a second concurrent whole-window replay on
    /// the same leaf. The sweep retries after its cooldown.
    /// </summary>
    AlreadyDriving = 4,
}
