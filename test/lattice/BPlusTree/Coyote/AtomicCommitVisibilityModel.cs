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

        RunSharedSnapshot(txid, core, drained, MaybeCommit, MaybeDrain);
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
