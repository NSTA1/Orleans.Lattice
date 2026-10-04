namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free rules that hold an atomic-write saga's batch on the
/// physical copy it commits on (#4357, #4358, #4369). Extracted so the decisions
/// <c>LatticeGrain.SetManyAsyncCore</c> (the routing tier) and
/// <c>AtomicWriteGrain</c> (<c>RebindAcrossAliasSwapAsync</c> and the execute
/// phase's refusal handling) execute are the artifact the shard-ownership
/// specification (<c>spec/shard-ownership/</c>) and its Coyote model drive, with
/// no possibility of drift.
/// <para>
/// A saga binds to the physical copy its prepares go to
/// (<c>AtomicWriteState.BoundPhysicalTreeId</c>). Its prepared batch travels
/// through stateless routing activations, each caching a (physical copy, map)
/// pair that can predate an alias swap, so the routing tier refuses to place a
/// bound batch on any other copy. Before the decision the saga re-checks where
/// the tree resolves and either commits, stays bound to a copy that mirrors into
/// the new one, or re-binds and re-dispatches.
/// </para>
/// <para>
/// <b>Known gap (#4454).</b> <see cref="RebindsAfterRefusal"/> encodes the
/// execute phase's mid-dispatch re-bind as production has it: it does not apply
/// the mirror check <see cref="BeforeDecision"/> applies, so a batch partly
/// prepared on a copy that mirrors into the new one is re-dispatched there and
/// its earlier prepares are left behind.
/// </para>
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no wall-clock, and no Orleans types,
/// and allocates nothing. Physical tree ids compare ordinally.
/// </para>
/// </summary>
internal static class SagaCopyBinding
{
    /// <summary>
    /// Whether the routing tier may place a dispatch on
    /// <paramref name="resolvedPhysicalTreeId"/>: always for an unbound dispatch,
    /// and for a bound one only when the copy is the bound copy.
    /// </summary>
    /// <param name="boundPhysicalTreeId">The saga's bound copy, or <see langword="null"/> when the dispatch carries no binding.</param>
    /// <param name="resolvedPhysicalTreeId">The physical copy the routing pair names.</param>
    public static bool AdmitsDispatch(string? boundPhysicalTreeId, string resolvedPhysicalTreeId) =>
        boundPhysicalTreeId is null
        || string.Equals(resolvedPhysicalTreeId, boundPhysicalTreeId, StringComparison.Ordinal);

    /// <summary>
    /// What a saga whose batch is fully dispatched does immediately before its
    /// commit decision.
    /// </summary>
    /// <param name="boundPhysicalTreeId">The copy the batch is bound to.</param>
    /// <param name="resolvedPhysicalTreeId">The copy the logical tree resolves to now.</param>
    /// <param name="boundMirrorDestination">
    /// The copy the bound copy mirrors its mutations into, or <see langword="null"/>
    /// when it mirrors nowhere or the answer is unknown. Asked only when the tree
    /// has moved; an unknown answer re-binds, as it would without a mirror.
    /// </param>
    public static SagaCopyBindingVerdict BeforeDecision(
        string boundPhysicalTreeId,
        string resolvedPhysicalTreeId,
        string? boundMirrorDestination)
    {
        if (string.Equals(resolvedPhysicalTreeId, boundPhysicalTreeId, StringComparison.Ordinal))
        {
            return SagaCopyBindingVerdict.Commit;
        }

        return string.Equals(boundMirrorDestination, resolvedPhysicalTreeId, StringComparison.Ordinal)
            ? SagaCopyBindingVerdict.StayBound
            : SagaCopyBindingVerdict.Rebind;
    }

    /// <summary>
    /// Whether a dispatch the routing tier refused because the tree moved makes
    /// the saga re-bind: the refusal must name the saga's own bound copy. See the
    /// known gap in the type summary (#4454).
    /// </summary>
    /// <param name="boundPhysicalTreeId">The saga's bound copy, or <see langword="null"/> when it is unbound.</param>
    /// <param name="refusedPhysicalTreeId">The stale copy the routing tier's refusal names.</param>
    public static bool RebindsAfterRefusal(string? boundPhysicalTreeId, string? refusedPhysicalTreeId) =>
        boundPhysicalTreeId is not null
        && string.Equals(refusedPhysicalTreeId, boundPhysicalTreeId, StringComparison.Ordinal);
}
