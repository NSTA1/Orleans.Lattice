namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How a read resolves a key that carries a prepared (pending-tx) mutation,
/// once the owning saga's recorded outcome and the leaf's local orphan-guard
/// state are known. The three cases are mutually exclusive and total.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="AtomicVisibilityGate"/> and
/// <see cref="TxDecisionView"/>, is the <b>dependency-free correctness core</b>
/// of the consensus-free atomic-commit read gate. It is the exact artifact the
/// production leaf grain executes on every read that touches a pending key, and
/// it is also the artifact the Coyote concurrency model drives under systematic
/// schedule exploration - so the property proven by the model is a property of
/// the code that actually runs, not of a parallel mimic that can drift.
/// </remarks>
internal enum PendingReadOutcome
{
    /// <summary>
    /// The saga committed and this is not an already-terminal orphan bucket, so
    /// the prepared (post-saga) value is the reader's answer.
    /// </summary>
    SurfacePrepared,

    /// <summary>
    /// The saga committed and this is not an already-terminal orphan bucket, but
    /// the prepared value is a tombstone or has expired, so the key is absent to
    /// the reader (it does <b>not</b> fall through to the pre-saga value).
    /// <para>
    /// Also the outcome for a saga whose status is
    /// <see cref="TxStatus.Indeterminate"/>: the registry cannot say whether the
    /// saga committed, so the reader is shown neither candidate value. Absence is
    /// the only answer that asserts nothing, and it is the reason this case
    /// exists separately from <see cref="FallThroughToPreSaga"/> rather than
    /// being folded into it.
    /// </para>
    /// </summary>
    Hidden,

    /// <summary>
    /// The saga is in-flight or aborted, or this is an already-terminal orphan
    /// bucket, so the prepared value is invisible and the reader serves the
    /// pre-saga (previously-committed) value from the entry cache.
    /// </summary>
    FallThroughToPreSaga,
}

/// <summary>
/// The pure per-key decision rule of the atomic-commit read gate: given a saga's
/// recorded <see cref="TxStatus"/>, whether the leaf has already applied that
/// saga's terminal (the orphan guard), and whether the prepared value is
/// hidden by a tombstone or TTL expiry, decide how a reader resolves the key.
/// <para>
/// Extracted so the production read path (<c>BPlusLeafGrain</c>'s single-key and
/// scan reads) and the Coyote atomic-visibility model share one rule with no
/// possibility of drift. The rule is intentionally trivial and total; the
/// correctness weight of the gate lives in <see cref="TxDecisionView"/> - the
/// discipline of resolving <i>every</i> key of a fan-out against a
/// <b>single</b> registry view - which is what makes a saga all-or-nothing
/// visible.
/// </para>
/// </summary>
internal static class AtomicVisibilityGate
{
    /// <summary>
    /// Resolves how a reader treats a key that carries a prepared mutation under
    /// a saga whose recorded outcome is <paramref name="status"/>.
    /// </summary>
    /// <param name="status">
    /// The saga's outcome as recorded by the per-tree transaction registry,
    /// resolved against a single consistent snapshot (see
    /// <see cref="TxDecisionView"/>).
    /// </param>
    /// <param name="alreadyTerminal">
    /// <see langword="true"/> when this leaf has already applied the saga's
    /// terminal, so a surviving pending bucket is a late-arriving shadow-forward
    /// orphan that must not shadow the authoritative projected value.
    /// </param>
    /// <param name="preparedHiddenByTombstoneOrExpiry">
    /// <see langword="true"/> when the prepared value is a tombstone or has
    /// expired as of the read's wall-clock moment.
    /// </param>
    /// <remarks>
    /// <para>
    /// <b>Why <see cref="TxStatus.Indeterminate"/> hides rather than falls
    /// through.</b> Falling through to the pre-saga value is not a neutral
    /// default - it is a positive assertion that the saga did not commit. When
    /// the registry has told us it cannot determine the outcome, making that
    /// assertion on its behalf is how an acknowledged, committed write comes to
    /// be served at its pre-saga value: the reader sees a stale value with no
    /// error, no log, and no metric, and (under the aged-out-decision trigger)
    /// keeps seeing it. Hiding the key asserts nothing. It is a strictly weaker
    /// answer, it is never wrong about a value, and it self-corrects the moment
    /// the outcome becomes determinable again.
    /// </para>
    /// <para>
    /// The cost is real and is accepted deliberately: a key whose saga in fact
    /// <i>aborted</i> reads as absent for the duration of the indeterminacy,
    /// where it would previously have read at its pre-saga value. That trades a
    /// temporary, self-announcing absence for a silent, possibly permanent
    /// stale read, on a surface whose whole purpose is all-or-nothing
    /// visibility. Absence is also already in this rule's vocabulary, so
    /// nothing downstream has to learn a new outcome.
    /// </para>
    /// <para>
    /// <b>Old nodes.</b> An older build that receives the unknown enum value
    /// takes this method's <c>status == Committed</c> test as false and returns
    /// <see cref="PendingReadOutcome.FallThroughToPreSaga"/> - the pre-widening
    /// behaviour, which is the correct degradation for a mixed-version cluster.
    /// </para>
    /// </remarks>
    public static PendingReadOutcome ResolveKey(
        TxStatus status,
        bool alreadyTerminal,
        bool preparedHiddenByTombstoneOrExpiry)
    {
        if (status == TxStatus.Indeterminate)
        {
            // Checked ahead of the Committed test so the orphan guard cannot
            // reroute it: alreadyTerminal means this leaf already applied the
            // saga's terminal, which is a claim about THIS leaf's projection,
            // not about the saga's outcome. Under an indeterminate outcome we
            // have no basis to prefer the projected value over the prepared one,
            // and serving either would assert what the registry declined to.
            return PendingReadOutcome.Hidden;
        }

        if (status == TxStatus.Committed && !alreadyTerminal)
        {
            return preparedHiddenByTombstoneOrExpiry
                ? PendingReadOutcome.Hidden
                : PendingReadOutcome.SurfacePrepared;
        }

        return PendingReadOutcome.FallThroughToPreSaga;
    }
}

/// <summary>
/// An immutable capture of the per-tree transaction registry's recorded
/// decisions, used to resolve a multi-key read atomically. Resolving every key
/// of a fan-out against a <b>single</b> view is the linearization point that
/// makes a saga all-or-nothing visible: the registry's
/// <see cref="TxStatus.InFlight"/> to <see cref="TxStatus.Committed"/> transition
/// cannot fall mid-fan-out and let one key surface its prepared value while a
/// sibling key still hides it (a split view). This is the exact invariant the
/// reshard atomic-visibility bug turned on.
/// </summary>
/// <remarks>
/// A txid absent from the view resolves to <see cref="TxStatus.InFlight"/> -
/// the strict-isolation default, consistent with "no decision recorded as of
/// this view's moment". The struct holds a reference to the caller's decision
/// map without copying; callers must not mutate that map after handing it in.
/// <para>
/// That default is only sound because the registry's snapshot APIs no longer
/// use omission to mean two different things. A decision whose retention window
/// has elapsed is present in the map as <see cref="TxStatus.Indeterminate"/>
/// rather than dropped from it, so absence here now means what it says.
/// Previously an aged-out <see cref="TxStatus.Committed"/> decision was omitted
/// and therefore arrived at this method indistinguishable from a saga that had
/// never been decided at all - and on a snapshot exported to a bootstrapping
/// cluster there was no later message able to correct it.
/// </para>
/// </remarks>
internal readonly struct TxDecisionView
{
    private readonly IReadOnlyDictionary<Guid, TxStatus>? _decisions;

    /// <summary>
    /// Creates a view over an already-captured set of registry decisions.
    /// </summary>
    public TxDecisionView(IReadOnlyDictionary<Guid, TxStatus>? decisions) => _decisions = decisions;

    /// <summary>
    /// Resolves the recorded outcome for <paramref name="txid"/>, returning
    /// <see cref="TxStatus.InFlight"/> when the view has no decision for it.
    /// </summary>
    public TxStatus Resolve(Guid txid) =>
        _decisions is not null && _decisions.TryGetValue(txid, out var status)
            ? status
            : TxStatus.InFlight;
}
