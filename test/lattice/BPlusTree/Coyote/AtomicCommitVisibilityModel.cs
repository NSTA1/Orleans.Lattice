using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// The reader design a <see cref="AtomicCommitVisibilityModel"/> run exercises.
/// </summary>
public enum AtomicCommitReaderMode
{
    /// <summary>
    /// The fix: the reader resolves every key against a single captured registry
    /// decision snapshot (map + revision) and then double-checks the monotonic
    /// revision via <see cref="ReaderStabilityGate.IsRevisionStable(long, long)"/>,
    /// retrying under a fresh snapshot when a commit landed during the fan-out.
    /// This design admits no certified split view.
    /// </summary>
    SharedSnapshotWithRevisionProbe,

    /// <summary>
    /// The guard: the reader still resolves every key against one captured
    /// snapshot but omits the revision re-check, certifying whatever it observed.
    /// A commit landing mid-fan-out (drained on some leaves, stale-InFlight on the
    /// captured snapshot for the rest) then produces a certified torn read.
    /// </summary>
    SharedSnapshotWithoutRevisionProbe,

    /// <summary>
    /// The guard preserving the original #1584 regression: the reader reads the
    /// registry <b>live</b> once per key (no shared snapshot), so a commit falling
    /// between two per-key reads makes one key surface its prepared value while
    /// the sibling still hides it - a split view.
    /// </summary>
    LivePerKeyRead,

    /// <summary>
    /// The fix under registry call failure injection (issue #3641): the snap1
    /// fetch, the revision probe, and the disambiguation snapshot can each fail.
    /// A failed snap1 fans out under the "snapshot unavailable" ambient, where a
    /// key that still carries a prepare fails the attempt closed instead of
    /// resolving; an unverifiable post-fan-out check re-runs the fan-out that way
    /// once. The accept/retry rule is the production
    /// <see cref="ReaderStabilityGate.Decide"/>. This design admits no certified
    /// split view.
    /// </summary>
    SharedSnapshotUnderRegistryFailures,

    /// <summary>
    /// The mutation that restores the pre-#3641 fail-open under the same
    /// failure injection: a failed snap1 leaves each leaf resolving its prepare
    /// live at its own moment, and a failed probe or disambiguation snapshot is
    /// treated as stable. Coyote must find a certified torn read.
    /// </summary>
    SharedSnapshotUnderRegistryFailuresFailOpen,
}

/// <summary>
/// A Coyote concurrency model of an N-key atomic-visibility read resolved against
/// a versioned registry view, driving the <b>production</b> cores under systematic
/// schedule exploration: the recording side is a real
/// <see cref="TxRegistryDecisionCore"/>, the per-key visibility decision is the
/// real <see cref="AtomicVisibilityGate.ResolveKey"/> rule read through a real
/// <see cref="TxDecisionView"/>, and the reader-side stability probe is the real
/// <see cref="ReaderStabilityGate.IsRevisionStable(long, long)"/>. Because the
/// model executes the same code Orleans runs, a violation Coyote finds is a
/// violation of the shipping read path.
/// <para>
/// The scenario generalizes the reshard split-view race (#1584) to a reader
/// fanning out to <c>keyCount</c> keys that all carry a prepared mutation from a
/// single saga. Two independent monotonic transitions are explored: the registry
/// <b>commit</b> (InFlight -&gt; Committed, one revision bump) and, per key, an
/// irreversible per-leaf <b>drain</b> that flips that leaf's prepared entry into
/// the runtime cache so the leaf serves the post-saga value regardless of any
/// reader snapshot. A drain can only follow the commit, and the commit is the
/// only event that advances the registry revision - so any reader that observes a
/// drained leaf while holding a stale InFlight snapshot is, by construction,
/// holding a stale revision, which is exactly what the probe rejects.
/// </para>
/// <para>
/// The safety property is all-or-nothing: every key of the fan-out is observed
/// with the saga's post value, or every key with its pre value. A certified split
/// is a torn read.
/// </para>
/// </summary>
public sealed class AtomicCommitVisibilityModel : ICoyoteModel
{
    private readonly int _keyCount;
    private readonly AtomicCommitReaderMode _mode;

    /// <summary>
    /// Creates the model for a <paramref name="keyCount"/>-key fan-out under the
    /// chosen reader <paramref name="mode"/>.
    /// </summary>
    public AtomicCommitVisibilityModel(int keyCount, AtomicCommitReaderMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(keyCount, 2);
        _keyCount = keyCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // The real recording-side core holds the single saga's decision and the
        // monotonic revision counter the reader-side probe checks against.
        var core = new TxRegistryDecisionCore(new Dictionary<Guid, TxStatus>(), 0L);
        var txid = Guid.NewGuid();

        // Per-leaf drain state. A drained leaf serves the post-saga value from
        // its runtime cache regardless of any reader snapshot; it can only drain
        // after the saga has committed.
        var drained = new bool[_keyCount];

        void MaybeCommit()
        {
            if (core.Resolve(txid) != TxStatus.Committed && runtime.RandomBoolean())
            {
                core.Apply(txid, TxStatus.Committed);
            }
        }

        void MaybeDrain(int leaf)
        {
            if (core.Resolve(txid) == TxStatus.Committed && !drained[leaf] && runtime.RandomBoolean())
            {
                drained[leaf] = true;
            }
        }

        if (_mode == AtomicCommitReaderMode.LivePerKeyRead)
        {
            RunLivePerKeyRead(txid, core, MaybeCommit);
            return;
        }

        if (_mode is AtomicCommitReaderMode.SharedSnapshotUnderRegistryFailures
            or AtomicCommitReaderMode.SharedSnapshotUnderRegistryFailuresFailOpen)
        {
            RunUnderRegistryFailures(runtime, txid, core, drained, MaybeCommit, MaybeDrain);
            return;
        }

        RunSharedSnapshot(txid, core, drained, MaybeCommit, MaybeDrain);
    }

    /// <summary>
    /// Bound on reader attempts under failure injection, standing in for
    /// <see cref="LatticeOptions.MaxScanRetries"/> so exploration terminates.
    /// Exhaustion is the typed exception: nothing is certified.
    /// </summary>
    private const int MaxReaderAttempts = 3;

    /// <summary>
    /// The multi-key reader with registry call failures injected on snap1, the
    /// revision probe, and the disambiguation snapshot (issue #3641), mirroring
    /// the production <c>LatticeGrain</c> read loop: fail-closed in
    /// <see cref="AtomicCommitReaderMode.SharedSnapshotUnderRegistryFailures"/>,
    /// the restored fail-open in
    /// <see cref="AtomicCommitReaderMode.SharedSnapshotUnderRegistryFailuresFailOpen"/>.
    /// </summary>
    private void RunUnderRegistryFailures(
        ICoyoteRuntime runtime,
        Guid txid,
        TxRegistryDecisionCore core,
        bool[] drained,
        Action maybeCommit,
        Action<int> maybeDrain)
    {
        var failOpen = _mode == AtomicCommitReaderMode.SharedSnapshotUnderRegistryFailuresFailOpen;

        for (var attempt = 0; attempt < MaxReaderAttempts; attempt++)
        {
            // snap1 fetch, which may fail in transport.
            TxDecisionSnapshot? snap1 = runtime.RandomBoolean() ? null : core.Snapshot();
            var strict = snap1 is null && !failOpen;

            for (var pass = 0; pass < 2; pass++)
            {
                var observedPost = new bool[_keyCount];
                var reachedUnresolvablePrepare = false;
                for (var i = 0; i < _keyCount; i++)
                {
                    maybeCommit();
                    maybeDrain(i);
                    if (drained[i])
                    {
                        // A drained leaf holds no prepare and serves post-saga.
                        observedPost[i] = true;
                        continue;
                    }

                    if (strict)
                    {
                        // The "snapshot unavailable" ambient: the leaf reaches a
                        // prepared key and throws rather than resolve it.
                        reachedUnresolvablePrepare = true;
                        break;
                    }

                    // Under snap1, or - fail-open with no snap1 - each leaf
                    // resolving its own prepare live at its own moment.
                    var view = snap1 is { } s
                        ? new TxDecisionView(s.Decisions)
                        : new TxDecisionView(core.Snapshot().Decisions);
                    observedPost[i] = ObserveKey(i, view, txid, drained);
                }

                if (reachedUnresolvablePrepare)
                {
                    break;
                }

                ReaderStabilityVerdict verdict;
                if (strict)
                {
                    verdict = ReaderStabilityVerdict.Unverifiable;
                }
                else if (snap1 is not { } captured)
                {
                    // Fail-open only: no snap1 to verify against; the legacy
                    // code certified this attempt.
                    verdict = ReaderStabilityVerdict.Stable;
                }
                else
                {
                    verdict = ClassifyAfterFanOut(runtime, core, captured);
                    if (failOpen && verdict == ReaderStabilityVerdict.Unverifiable)
                    {
                        verdict = ReaderStabilityVerdict.Stable;
                    }
                }

                if (ReaderStabilityGate.Decide(verdict, resolvedPreparedKey: !strict) == ReaderAttemptDecision.Accept)
                {
                    AssertAllOrNothing(observedPost);
                    return;
                }

                if (verdict == ReaderStabilityVerdict.Unverifiable && !strict)
                {
                    strict = true;
                    continue;
                }

                break;
            }
        }

        // Retry budget exhausted: the read throws the typed exception and
        // certifies nothing.
    }

    /// <summary>
    /// The post-fan-out stability check with transport failure injected on the
    /// revision probe and on the disambiguation snapshot; a failure of either is
    /// <see cref="ReaderStabilityVerdict.Unverifiable"/>, exactly as
    /// <c>LatticeGrain.ClassifySnap2Async</c> reports it.
    /// </summary>
    private static ReaderStabilityVerdict ClassifyAfterFanOut(
        ICoyoteRuntime runtime,
        TxRegistryDecisionCore core,
        TxDecisionSnapshot snap1)
    {
        if (runtime.RandomBoolean())
        {
            return ReaderStabilityVerdict.Unverifiable;
        }

        if (ReaderStabilityGate.IsRevisionStable(snap1.Revision, core.Revision))
        {
            return ReaderStabilityVerdict.Stable;
        }

        if (runtime.RandomBoolean())
        {
            return ReaderStabilityVerdict.Unverifiable;
        }

        return ReaderStabilityGate.ClassifySnapshot(
            new Dictionary<Guid, TxStatus>(snap1.Decisions),
            new Dictionary<Guid, TxStatus>(core.Snapshot().Decisions));
    }

    /// <summary>
    /// The shared-snapshot reader (with or without the revision probe). Captures a
    /// single registry snapshot, resolves every key against it while the commit
    /// and per-leaf drains are interleaved by the scheduler, then either certifies
    /// the observation directly or, under the probe, retries on a revision change.
    /// </summary>
    private void RunSharedSnapshot(
        Guid txid,
        TxRegistryDecisionCore core,
        bool[] drained,
        Action maybeCommit,
        Action<int> maybeDrain)
    {
        var withProbe = _mode == AtomicCommitReaderMode.SharedSnapshotWithRevisionProbe;

        while (true)
        {
            // Capture the registry decision snapshot: the map paired with the
            // revision that produced it, exactly as SnapshotWithRevisionAsync does.
            var snapshot = core.Snapshot();
            var view = new TxDecisionView(snapshot.Decisions);

            // Resolve every key against the single captured view. The commit and
            // the per-leaf drains are interleaved between key resolutions so the
            // fan-out can straddle a mid-commit drain.
            var observedPost = new bool[_keyCount];
            for (var i = 0; i < _keyCount; i++)
            {
                maybeCommit();
                maybeDrain(i);
                observedPost[i] = ObserveKey(i, view, txid, drained);
            }

            if (withProbe && !ReaderStabilityGate.IsRevisionStable(snapshot.Revision, core.Revision))
            {
                // A decision changed during the fan-out: the snapshot the read
                // resolved against is no longer authoritative. Retry under a
                // fresh snapshot rather than certifying a possibly-torn read.
                continue;
            }

            AssertAllOrNothing(observedPost);
            return;
        }
    }

    /// <summary>
    /// The live-per-key reader (the original #1584 regression): each key reads the
    /// registry decision live, so a commit falling between two reads tears the view.
    /// </summary>
    private void RunLivePerKeyRead(Guid txid, TxRegistryDecisionCore core, Action maybeCommit)
    {
        // The commit may already have landed before the reader begins.
        maybeCommit();

        var observedPost = new bool[_keyCount];
        for (var i = 0; i < _keyCount; i++)
        {
            // Live read: no shared snapshot, so a value observed here can be
            // superseded before the next key is read.
            var liveStatus = core.Resolve(txid);
            observedPost[i] = AtomicVisibilityGate.ResolveKey(
                liveStatus, alreadyTerminal: false, preparedHiddenByTombstoneOrExpiry: false)
                == PendingReadOutcome.SurfacePrepared;
            maybeCommit();
        }

        AssertAllOrNothing(observedPost);
    }

    /// <summary>
    /// Resolves whether key <paramref name="leaf"/> is observed with the saga's
    /// post value. A drained leaf serves the post value from its runtime cache
    /// regardless of the reader's snapshot; an undrained leaf consults the
    /// captured registry <paramref name="view"/> through the production gate.
    /// </summary>
    private static bool ObserveKey(int leaf, TxDecisionView view, Guid txid, bool[] drained)
    {
        if (drained[leaf])
        {
            return true;
        }

        var status = view.Resolve(txid);
        return AtomicVisibilityGate.ResolveKey(
            status, alreadyTerminal: false, preparedHiddenByTombstoneOrExpiry: false)
            == PendingReadOutcome.SurfacePrepared;
    }

    private static void AssertAllOrNothing(bool[] observedPost)
    {
        var first = observedPost[0];
        for (var i = 1; i < observedPost.Length; i++)
        {
            Specification.Assert(
                observedPost[i] == first,
                $"atomic-visibility split across {observedPost.Length} keys: key0Post={first}, " +
                $"key{i}Post={observedPost[i]} (a reader observed one key of the saga but not another)");
        }
    }
}
