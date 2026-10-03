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

    /// <summary>
    /// Chooses which of several prepared mutations covering the <b>same key</b> on
    /// one leaf decides how a reader resolves that key, so that
    /// <see cref="ResolveKey"/> is fed the bucket that actually determines the
    /// key's visible value. Returns the index of the deciding candidate, or
    /// <c>-1</c> when <paramref name="candidates"/> is empty.
    /// </summary>
    /// <param name="candidates">
    /// Every prepared mutation covering the key, each with its saga's recorded
    /// outcome (resolved against the read's single registry view), the orphan
    /// guard, whether the leaf's committed row already supersedes it, and the
    /// prepared value's HLC timestamp.
    /// </param>
    /// <returns>
    /// The index of the candidate to resolve the key against through
    /// <see cref="ResolveKey"/>, or <c>-1</c> when no candidate decides, in which
    /// case the reader serves the leaf's committed row exactly as if the key had
    /// no prepare at all.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Why more than one bucket can cover a key.</b> A saga whose prepares
    /// landed but whose decision has not been recorded keeps its bucket for as
    /// long as it stays undecided - most visibly when a silo restart parks the
    /// saga (the caller sees <see cref="LatticeShuttingDownException"/>) and the
    /// leaf's reactivation replays the prepare from the write-ahead log. Every
    /// later saga that writes the same key then prepares a second bucket beside
    /// it. A shard split's retroactive sweep can likewise leave an orphan beside a
    /// newer prepare.
    /// </para>
    /// <para>
    /// <b>The rule.</b> The key's visible value is what the saga terminals will
    /// leave on this leaf, so only a bucket whose terminal would actually change
    /// the row can decide. That is an <see cref="TxStatus.Indeterminate"/> saga
    /// (which hides the key - the strictly weaker answer wins, because the
    /// registry cannot say whether that saga's value is the one the key settles
    /// on), or a <see cref="TxStatus.Committed"/> saga whose terminal has not been
    /// applied here (which surfaces its prepared value, newest HLC first when
    /// several have committed) - in both cases only when the leaf's committed row
    /// does not already supersede the prepare, because the commit drain skips a
    /// prepare that a newer, non-migrated row dominates (the orphan-drain guard
    /// in <c>BPlusLeafGrain.ApplyTxCommit</c>), so such a bucket never lands.
    /// <see cref="TxStatus.InFlight"/> and <see cref="TxStatus.Aborted"/> buckets,
    /// and already-terminal orphans, are invisible to readers whatever their age,
    /// so they never decide.
    /// </para>
    /// <para>
    /// <b>The defect it closes.</b> The multi-key read paths used to keep whichever
    /// bucket dictionary enumeration reached first. A long-undecided saga's bucket
    /// then shadowed a newer, already-committed saga's bucket on some leaves: those
    /// leaves fell through to the pre-saga value while sibling leaves surfaced the
    /// committed one, so a reader saw the committed batch on some keys and not on
    /// others - a torn read that repeated for every later batch until the stuck
    /// saga resolved. Picking the newest bucket alone (as the single-key paths did)
    /// is not sufficient either: a newer in-flight bucket then shadows an older
    /// bucket whose saga has committed but has not yet drained here, which tears
    /// that older saga in the same way.
    /// </para>
    /// </remarks>
    public static int SelectDecidingPrepare(ReadOnlySpan<PreparedCandidate> candidates)
    {
        var newestCommitted = -1;
        var newestIndeterminate = -1;
        for (var i = 0; i < candidates.Length; i++)
        {
            var candidate = candidates[i];
            if (candidate.SupersededByRow)
                continue;

            if (candidate.Status == TxStatus.Indeterminate)
            {
                if (newestIndeterminate < 0 || candidate.Timestamp.CompareTo(candidates[newestIndeterminate].Timestamp) > 0)
                    newestIndeterminate = i;
            }
            else if (candidate.Status == TxStatus.Committed && !candidate.AlreadyTerminal)
            {
                if (newestCommitted < 0 || candidate.Timestamp.CompareTo(candidates[newestCommitted].Timestamp) > 0)
                    newestCommitted = i;
            }
        }

        return newestIndeterminate >= 0 ? newestIndeterminate : newestCommitted;
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
