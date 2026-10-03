namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free rules an online resize applies to the old physical
/// copy it fences: which calls the fenced copy still admits, and whether a
/// failed alias flip lifts the fence again. Extracted so the decisions
/// <c>ShardRootGrain</c> (<c>AdmitsBoundSagaWhileFenced</c>) and
/// <c>TreeResizeGrain</c> (<c>LiftFenceUnlessSwappedAsync</c>) execute are the
/// artifact the shard-ownership specification (<c>spec/shard-ownership/</c>) and
/// its Coyote model drive, with no possibility of drift.
/// <para>
/// <b>Fence before flip (#4362).</b> The resize moves every old shard into
/// <c>ShadowForwardPhase.Rejecting</c> before the alias names the resized copy,
/// so a router whose cached pair still names the old copy is refused rather than
/// served the rounds the old copy held at the flip.
/// </para>
/// <para>
/// <b>The bound saga is admitted (#4369).</b> An atomic-write saga bound to the
/// fenced copy is still served there - its prepared batch carries the binding,
/// and its terminals are addressed to the copy directly rather than through the
/// logical alias - and the copy mirrors it to the resized copy. Refusing it would
/// leave the batch on some of the old copy's shards only, which an undo would
/// re-expose torn.
/// </para>
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no wall-clock, and no Orleans types,
/// and allocates nothing. Physical tree ids compare ordinally, as they do
/// everywhere else.
/// </para>
/// </summary>
internal static class ResizeFence
{
    /// <summary>
    /// Whether a shard of a physical copy admits the current call although an
    /// online resize has fenced the copy.
    /// </summary>
    /// <param name="rejecting">Whether the shard is in <c>ShadowForwardPhase.Rejecting</c>. An unfenced shard is not asked.</param>
    /// <param name="directTerminal">
    /// Whether the call is a saga terminal addressed to this copy directly, with no
    /// routed-logical stamp. Such a terminal is always the saga's own.
    /// </param>
    /// <param name="preparedScope">Whether the call runs under an atomic-write saga's prepared scope.</param>
    /// <param name="boundPhysicalTreeId">The physical copy the saga's binding names, or <see langword="null"/> for none.</param>
    /// <param name="physicalTreeId">The physical copy this shard belongs to.</param>
    public static bool AdmitsBoundSaga(
        bool rejecting,
        bool directTerminal,
        bool preparedScope,
        string? boundPhysicalTreeId,
        string physicalTreeId) =>
        rejecting
        && (directTerminal
            || (preparedScope && string.Equals(boundPhysicalTreeId, physicalTreeId, StringComparison.Ordinal)));

    /// <summary>
    /// Whether the resize lifts the old copy's fence after an alias flip that
    /// failed or was refused. A flip that failed in transport may still have
    /// landed, so the registry's answer decides: when the logical tree resolves
    /// to the resized copy the fence is exactly what must stay.
    /// </summary>
    /// <param name="resolvedPhysicalTreeId">The physical copy the logical tree resolves to after the failure.</param>
    /// <param name="resizedPhysicalTreeId">The resized copy the flip would have moved the alias to.</param>
    public static bool LiftsFenceAfterFailedFlip(string? resolvedPhysicalTreeId, string? resizedPhysicalTreeId) =>
        !string.Equals(resolvedPhysicalTreeId, resizedPhysicalTreeId, StringComparison.Ordinal);
}
