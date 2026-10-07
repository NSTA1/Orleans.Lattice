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
/// A bound copy that mirrors into the copy the tree now resolves to - the source
/// of an online resize whose destination the tree flipped to - is still the copy
/// the batch belongs on, at every step: the routing tier places a bound dispatch
/// there (<see cref="DispatchCopy"/>), a refused dispatch stays bound
/// (<see cref="AfterRefusal"/>), and so does the pre-decision check
/// (<see cref="BeforeDecision"/>). The source takes the batch through its fence
/// and mirrors it, so the batch commits whole on both copies; re-binding part
/// way would leave the prepares already taken on the source, where a resize undo
/// re-exposes them (#4369, #4454).
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
    /// The copy the routing tier places a dispatch on once it has resolved the
    /// tree afresh, or <see langword="null"/> to refuse it: the resolved copy for
    /// an unbound dispatch or one bound to it, the bound copy when that copy
    /// mirrors into the resolved one (#4454), and nothing otherwise.
    /// </summary>
    /// <param name="boundPhysicalTreeId">The saga's bound copy, or <see langword="null"/> when the dispatch carries no binding.</param>
    /// <param name="resolvedPhysicalTreeId">The physical copy the tree resolves to.</param>
    /// <param name="boundMirrorDestination">
    /// The copy the bound copy mirrors its mutations into, or <see langword="null"/>
    /// when it mirrors nowhere or the answer is unknown; an unknown answer refuses,
    /// as it would without a mirror.
    /// </param>
    public static string? DispatchCopy(
        string? boundPhysicalTreeId,
        string resolvedPhysicalTreeId,
        string? boundMirrorDestination)
    {
        if (AdmitsDispatch(boundPhysicalTreeId, resolvedPhysicalTreeId))
        {
            return resolvedPhysicalTreeId;
        }

        return string.Equals(boundMirrorDestination, resolvedPhysicalTreeId, StringComparison.Ordinal)
            ? boundPhysicalTreeId
            : null;
    }

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
    /// Whether a dispatch the routing tier refused because the tree moved
    /// concerns the saga's binding at all: the refusal must name the saga's own
    /// bound copy. What the saga then does is <see cref="AfterRefusal"/>.
    /// </summary>
    /// <param name="boundPhysicalTreeId">The saga's bound copy, or <see langword="null"/> when it is unbound.</param>
    /// <param name="refusedPhysicalTreeId">The stale copy the routing tier's refusal names.</param>
    public static bool RebindsAfterRefusal(string? boundPhysicalTreeId, string? refusedPhysicalTreeId) =>
        boundPhysicalTreeId is not null
        && string.Equals(refusedPhysicalTreeId, boundPhysicalTreeId, StringComparison.Ordinal);

    /// <summary>
    /// What a saga whose dispatch the routing tier refused, naming its bound copy,
    /// does once it has resolved the tree afresh: the same rule as
    /// <see cref="BeforeDecision"/>, applied part way through the batch.
    /// <see cref="SagaCopyBindingVerdict.Commit"/> means the tree still resolves to
    /// the bound copy, so the refusal is an ordinary failure to retry;
    /// <see cref="SagaCopyBindingVerdict.StayBound"/> means the bound copy mirrors
    /// into the resolved one, so the saga stays bound and dispatches again, which
    /// the routing tier now places on the bound copy (<see cref="DispatchCopy"/>);
    /// <see cref="SagaCopyBindingVerdict.Rebind"/> means re-bind and re-dispatch.
    /// Re-binding part way when the bound copy mirrors would leave the prepares
    /// already taken on it, where a resize undo re-exposes them (#4454).
    /// </summary>
    /// <param name="boundPhysicalTreeId">The copy the batch is bound to.</param>
    /// <param name="resolvedPhysicalTreeId">The copy the logical tree resolves to now.</param>
    /// <param name="boundMirrorDestination">As for <see cref="BeforeDecision"/>.</param>
    public static SagaCopyBindingVerdict AfterRefusal(
        string boundPhysicalTreeId,
        string resolvedPhysicalTreeId,
        string? boundMirrorDestination) =>
        BeforeDecision(boundPhysicalTreeId, resolvedPhysicalTreeId, boundMirrorDestination);
}
