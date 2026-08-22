namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The undo token returned by a <see cref="TxRegistryDecisionCore"/> mutation.
/// It captures exactly what the mutation changed so a caller whose durable write
/// (e.g. <c>WriteStateAsync</c>) fails can restore the in-memory decision map and
/// revision counter to their pre-mutation values, keeping the in-memory view in
/// lockstep with the persisted one.
/// </summary>
/// <param name="Txid">The saga the mutation targeted.</param>
/// <param name="HadPrevious">
/// <see langword="true"/> when the map already held a decision for
/// <paramref name="Txid"/> before the mutation, so a rollback must restore
/// <paramref name="PreviousStatus"/> rather than remove the entry.
/// </param>
/// <param name="PreviousStatus">
/// The decision the map held for <paramref name="Txid"/> before the mutation,
/// meaningful only when <paramref name="HadPrevious"/> is <see langword="true"/>.
/// </param>
/// <param name="PreviousRevision">
/// The revision counter value before the mutation, restored on rollback.
/// </param>
/// <param name="Bumped">
/// <see langword="true"/> when the mutation actually changed the map and
/// therefore advanced the revision. A no-op mutation (idempotent re-record, or a
/// remove of an absent txid) reports <see langword="false"/>, and
/// <see cref="TxRegistryDecisionCore.Rollback(in TxDecisionMutation)"/> is then a
/// no-op.
/// </param>
internal readonly record struct TxDecisionMutation(
    Guid Txid,
    bool HadPrevious,
    TxStatus PreviousStatus,
    long PreviousRevision,
    bool Bumped);

/// <summary>
/// An immutable pairing of a captured decision map and the revision counter value
/// that produced it. Because <see cref="TxRegistryDecisionCore.Snapshot"/> reads
/// both in one synchronous step, a later observation of the same
/// <see cref="Revision"/> is proof that the <see cref="Decisions"/> map did not
/// mutate in the intervening window - the invariant the reader-side stability
/// probe (<see cref="ReaderStabilityGate.IsRevisionStable(long, long)"/>) relies
/// on.
/// </summary>
/// <param name="Decisions">
/// A defensive copy of the recorded decisions at capture time. Callers may read
/// it freely; the core never mutates a copy it has handed out.
/// </param>
/// <param name="Revision">The revision counter value at capture time.</param>
internal readonly record struct TxDecisionSnapshot(
    IReadOnlyDictionary<Guid, TxStatus> Decisions,
    long Revision);

/// <summary>
/// The pure, dependency-free model of the per-tree transaction registry's
/// recorded decision map paired with a monotonic revision counter. It is the
/// recording-side companion to <see cref="AtomicVisibilityGate"/> /
/// <see cref="TxDecisionView"/> (the per-key read gate) and
/// <see cref="ReaderStabilityGate"/> (the reader-side stability rule): the
/// registry grain records decisions through it, and the Coyote atomic-commit
/// model drives the same core, so the monotonic-revision invariant proven by the
/// model is a property of the code that actually runs.
/// <para>
/// The core owns no <c>Task</c>/<c>await</c> and no wall-clock. It wraps a caller
/// supplied decision dictionary <b>by reference</b> so a mutation is applied to
/// the same map the grain persists, and tracks the revision as its own field
/// (seeded from, and mirrored back to, the grain's persisted counter). The three
/// operations named in the design are <see cref="Apply(Guid, TxStatus)"/>
/// (apply(decision) -&gt; revision++), <see cref="Snapshot"/> (snapshot() -&gt;
/// (map, revision)), and <see cref="Revision"/> (revisionOf() -&gt; revision).
/// </para>
/// </summary>
internal sealed class TxRegistryDecisionCore
{
    private readonly IDictionary<Guid, TxStatus> _decisions;

    /// <summary>
    /// Wraps <paramref name="decisions"/> (by reference, not copied) and seeds the
    /// revision counter from <paramref name="revision"/>. Mutations are applied to
    /// the supplied dictionary in place, so a caller that persists that same
    /// dictionary sees the core's changes without any further copy.
    /// </summary>
    public TxRegistryDecisionCore(IDictionary<Guid, TxStatus> decisions, long revision)
    {
        ArgumentNullException.ThrowIfNull(decisions);
        _decisions = decisions;
        Revision = revision;
    }

    /// <summary>
    /// The current revision counter (revisionOf()). Advances by one on every
    /// mutation that changes the decision map, and never otherwise.
    /// </summary>
    public long Revision { get; private set; }

    /// <summary>The number of decisions currently recorded.</summary>
    public int Count => _decisions.Count;

    /// <summary>
    /// Resolves the recorded outcome for <paramref name="txid"/>, returning
    /// <see cref="TxStatus.InFlight"/> when no decision is recorded - the
    /// strict-isolation default, consistent with <see cref="TxDecisionView"/>.
    /// </summary>
    public TxStatus Resolve(Guid txid) =>
        _decisions.TryGetValue(txid, out var status) ? status : TxStatus.InFlight;

    /// <summary>
    /// Attempts to read the recorded outcome for <paramref name="txid"/>, reporting
    /// whether a decision is present rather than collapsing an absent entry to the
    /// <see cref="TxStatus.InFlight"/> default.
    /// </summary>
    public bool TryResolve(Guid txid, out TxStatus status) =>
        _decisions.TryGetValue(txid, out status);

    /// <summary>
    /// Records <paramref name="verdict"/> as the outcome for <paramref name="txid"/>,
    /// advancing the revision by one iff the map's mapping for
    /// <paramref name="txid"/> actually changed (a re-record of the identical
    /// outcome is an idempotent no-op that leaves the revision untouched). Returns
    /// a <see cref="TxDecisionMutation"/> undo token for
    /// <see cref="Rollback(in TxDecisionMutation)"/> on a failed durable write.
    /// </summary>
    public TxDecisionMutation Apply(Guid txid, TxStatus verdict)
    {
        var hadPrevious = _decisions.TryGetValue(txid, out var previous);
        var previousRevision = Revision;
        if (hadPrevious && previous == verdict)
        {
            return new TxDecisionMutation(txid, hadPrevious, previous, previousRevision, Bumped: false);
        }

        _decisions[txid] = verdict;
        Revision = previousRevision + 1;
        return new TxDecisionMutation(txid, hadPrevious, previous, previousRevision, Bumped: true);
    }

    /// <summary>
    /// Removes the recorded outcome for <paramref name="txid"/>, advancing the
    /// revision by one iff a decision was actually present (removing an absent
    /// txid is a no-op that leaves the revision untouched). Returns a
    /// <see cref="TxDecisionMutation"/> undo token.
    /// </summary>
    public TxDecisionMutation Remove(Guid txid)
    {
        var hadPrevious = _decisions.TryGetValue(txid, out var previous);
        var previousRevision = Revision;
        if (!hadPrevious)
        {
            return new TxDecisionMutation(txid, hadPrevious, previous, previousRevision, Bumped: false);
        }

        _decisions.Remove(txid);
        Revision = previousRevision + 1;
        return new TxDecisionMutation(txid, hadPrevious, previous, previousRevision, Bumped: true);
    }

    /// <summary>
    /// Advances the revision by one unconditionally, for a batch mutation the
    /// caller has already applied to the decision map directly (for example a
    /// prune pass that physically drops several tombstoned decisions and must
    /// advance the reader-visible revision exactly once for the whole batch).
    /// Returns the prior revision so the caller can
    /// <see cref="RollbackRevision(long)"/> on a failed durable write.
    /// </summary>
    public long AdvanceRevision()
    {
        var previousRevision = Revision;
        Revision = previousRevision + 1;
        return previousRevision;
    }

    /// <summary>
    /// Undoes an <see cref="Apply(Guid, TxStatus)"/> or <see cref="Remove(Guid)"/>
    /// after a failed durable write, restoring both the map entry and the revision
    /// to their pre-mutation values. A token whose
    /// <see cref="TxDecisionMutation.Bumped"/> is <see langword="false"/> changed
    /// nothing, so the rollback is a no-op.
    /// </summary>
    public void Rollback(in TxDecisionMutation mutation)
    {
        if (!mutation.Bumped)
        {
            return;
        }

        if (mutation.HadPrevious)
        {
            _decisions[mutation.Txid] = mutation.PreviousStatus;
        }
        else
        {
            _decisions.Remove(mutation.Txid);
        }

        Revision = mutation.PreviousRevision;
    }

    /// <summary>
    /// Restores the revision counter to <paramref name="previousRevision"/> after
    /// a failed durable write on a batch mutation whose map changes the caller
    /// restores itself (the companion to <see cref="AdvanceRevision"/>).
    /// </summary>
    public void RollbackRevision(long previousRevision) => Revision = previousRevision;

    /// <summary>
    /// Captures a defensive copy of the decision map paired with the current
    /// revision (snapshot() -&gt; (map, revision)). When
    /// <paramref name="include"/> is supplied, only txids for which it returns
    /// <see langword="true"/> are copied (used by the grain to drop
    /// tombstone-expired decisions from the observable snapshot); when it is
    /// <see langword="null"/> the whole map is copied. The revision is read in the
    /// same synchronous call, so it is guaranteed to be the revision that produced
    /// the returned map.
    /// </summary>
    public TxDecisionSnapshot Snapshot(Func<Guid, bool>? include = null)
    {
        var copy = new Dictionary<Guid, TxStatus>(_decisions.Count);
        foreach (var (txid, status) in _decisions)
        {
            if (include is null || include(txid))
            {
                copy[txid] = status;
            }
        }

        return new TxDecisionSnapshot(copy, Revision);
    }
}
