using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="AtomicVisibilityGate"/> and
/// <see cref="TxDecisionView"/> - the shared correctness core the production leaf
/// read path and the Coyote atomic-visibility model both execute. These pin the
/// exact three-outcome truth table so a change to the rule is caught here (and by
/// the Coyote model) rather than only by a slow reshard chaos run.
/// </summary>
[TestFixture]
public sealed class AtomicVisibilityGateTests
{
    [Test]
    public void Committed_not_orphan_live_surfaces_prepared()
    {
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.Committed, alreadyTerminal: false, preparedHiddenByTombstoneOrExpiry: false),
            Is.EqualTo(PendingReadOutcome.SurfacePrepared));
    }

    [Test]
    public void Committed_not_orphan_tombstone_or_expired_hides_key()
    {
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.Committed, alreadyTerminal: false, preparedHiddenByTombstoneOrExpiry: true),
            Is.EqualTo(PendingReadOutcome.Hidden));
    }

    [Test]
    public void Committed_but_already_terminal_orphan_falls_through([Values] bool preparedHidden)
    {
        // An already-applied terminal makes a surviving pending bucket a late
        // shadow-forward orphan: it must never shadow the projected value,
        // regardless of whether the orphan's prepared value is a tombstone.
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.Committed, alreadyTerminal: true, preparedHidden),
            Is.EqualTo(PendingReadOutcome.FallThroughToPreSaga));
    }

    [Test]
    public void InFlight_always_falls_through([Values] bool alreadyTerminal, [Values] bool preparedHidden)
    {
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.InFlight, alreadyTerminal, preparedHidden),
            Is.EqualTo(PendingReadOutcome.FallThroughToPreSaga));
    }

    [Test]
    public void Aborted_always_falls_through([Values] bool alreadyTerminal, [Values] bool preparedHidden)
    {
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.Aborted, alreadyTerminal, preparedHidden),
            Is.EqualTo(PendingReadOutcome.FallThroughToPreSaga));
    }

    [Test]
    public void Indeterminate_hides_key_when_no_terminal_was_applied_here([Values] bool preparedHidden)
    {
        // An indeterminate reading is a refusal to answer, not an answer. The
        // gate must hide rather than fall through: falling through would publish
        // the pre-saga value, which is an affirmative claim that the saga did not
        // commit - the one thing an indeterminate reading says nobody knows.
        // Hiding is the only outcome that is never wrong for a committed saga
        // and never wrong for an aborted one.
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.Indeterminate, alreadyTerminal: false, preparedHidden),
            Is.EqualTo(PendingReadOutcome.Hidden));
    }

    [Test]
    public void Already_terminal_orphan_falls_through_under_an_indeterminate_outcome([Values] bool preparedHidden)
    {
        // Issue #4428. The orphan guard is tested ahead of the Indeterminate arm:
        // this leaf already applied the saga's terminal, so its row is the outcome,
        // materialised, and serving it asserts nothing the leaf does not hold.
        // Hiding it kept a committed value unreadable for as long as the registry
        // row stayed masked, which nothing guarantees will end.
        Assert.That(
            AtomicVisibilityGate.ResolveKey(TxStatus.Indeterminate, alreadyTerminal: true, preparedHidden),
            Is.EqualTo(PendingReadOutcome.FallThroughToPreSaga));
    }

    [Test]
    public void DecisionView_resolves_present_indeterminate_txid_without_collapsing_it()
    {
        // The view must carry Indeterminate through verbatim. Collapsing it onto
        // InFlight here would restore the ambiguity the status exists to remove,
        // one layer below the gate.
        var txid = Guid.NewGuid();
        var view = new TxDecisionView(new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Indeterminate });
        Assert.That(view.Resolve(txid), Is.EqualTo(TxStatus.Indeterminate));
    }

    [Test]
    public void DecisionView_resolves_present_txid_to_recorded_status()
    {
        var txid = Guid.NewGuid();
        var view = new TxDecisionView(new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed });
        Assert.That(view.Resolve(txid), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void DecisionView_resolves_absent_txid_to_inflight()
    {
        var view = new TxDecisionView(new Dictionary<Guid, TxStatus>());
        Assert.That(view.Resolve(Guid.NewGuid()), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public void DecisionView_over_null_map_resolves_to_inflight()
    {
        var view = new TxDecisionView(null);
        Assert.That(view.Resolve(Guid.NewGuid()), Is.EqualTo(TxStatus.InFlight));
    }

    private static HybridLogicalClock At(long ticks) => new() { WallClockTicks = ticks };

    private static PreparedCandidate Candidate(
        TxStatus status, long ticks, bool alreadyTerminal = false, bool supersededByRow = false) =>
        new(status, alreadyTerminal, supersededByRow, At(ticks));

    [Test]
    public void SelectDecidingPrepare_returns_minus_one_for_no_candidates()
    {
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare([]), Is.EqualTo(-1));
    }

    [Test]
    public void SelectDecidingPrepare_older_in_flight_never_shadows_a_newer_committed_prepare()
    {
        // The silo-restart tear: a saga parked by the restart keeps its prepare
        // (InFlight) while the next saga prepares and commits the same key. The
        // committed saga decides, whatever order the buckets are enumerated in.
        PreparedCandidate[] olderFirst = [Candidate(TxStatus.InFlight, 1), Candidate(TxStatus.Committed, 2)];
        PreparedCandidate[] newerFirst = [Candidate(TxStatus.Committed, 2), Candidate(TxStatus.InFlight, 1)];

        Assert.Multiple(() =>
        {
            Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(olderFirst), Is.EqualTo(1));
            Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(newerFirst), Is.EqualTo(0));
        });
    }

    [Test]
    public void SelectDecidingPrepare_newer_in_flight_never_shadows_an_older_committed_prepare()
    {
        // Picking the newest bucket alone is not enough: the older saga has
        // committed but not yet drained here, so it decides the key's value.
        PreparedCandidate[] candidates = [Candidate(TxStatus.Committed, 1), Candidate(TxStatus.InFlight, 2)];
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(candidates), Is.EqualTo(0));
    }

    [Test]
    public void SelectDecidingPrepare_prefers_the_newest_of_several_committed_prepares()
    {
        PreparedCandidate[] candidates =
        [
            Candidate(TxStatus.Committed, 1),
            Candidate(TxStatus.Committed, 3),
            Candidate(TxStatus.Committed, 2),
        ];
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(candidates), Is.EqualTo(1));
    }

    [Test]
    public void SelectDecidingPrepare_an_indeterminate_prepare_decides_over_a_committed_one()
    {
        // The strictly weaker answer wins: the registry cannot say whether the
        // indeterminate saga's value is the one the key settles on.
        PreparedCandidate[] candidates = [Candidate(TxStatus.Committed, 2), Candidate(TxStatus.Indeterminate, 1)];
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(candidates), Is.EqualTo(1));
    }

    [Test]
    public void SelectDecidingPrepare_returns_minus_one_when_only_invisible_prepares_cover_the_key([Values] bool aborted)
    {
        var status = aborted ? TxStatus.Aborted : TxStatus.InFlight;
        PreparedCandidate[] candidates = [Candidate(status, 1), Candidate(TxStatus.InFlight, 2)];
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(candidates), Is.EqualTo(-1));
    }

    [Test]
    public void SelectDecidingPrepare_skips_an_already_terminal_committed_orphan()
    {
        PreparedCandidate[] candidates =
        [
            Candidate(TxStatus.Committed, 2, alreadyTerminal: true),
            Candidate(TxStatus.InFlight, 1),
        ];
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(candidates), Is.EqualTo(-1));
    }

    [Test]
    public void SelectDecidingPrepare_skips_an_already_terminal_indeterminate_orphan()
    {
        // Issue #4428: an orphan cannot change the row whatever its saga's
        // status, so it must not hide a committed prepare beside it either.
        PreparedCandidate[] alone = [Candidate(TxStatus.Indeterminate, 2, alreadyTerminal: true)];
        PreparedCandidate[] besideCommitted =
        [
            Candidate(TxStatus.Indeterminate, 2, alreadyTerminal: true),
            Candidate(TxStatus.Committed, 1),
        ];

        Assert.Multiple(() =>
        {
            Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(alone), Is.EqualTo(-1));
            Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(besideCommitted), Is.EqualTo(1));
        });
    }

    [Test]
    public void SelectDecidingPrepare_skips_prepares_the_committed_row_already_supersedes([Values] bool indeterminate)
    {
        var status = indeterminate ? TxStatus.Indeterminate : TxStatus.Committed;
        // The commit drain skips a prepare a newer non-migrated row dominates, so
        // such a bucket can never become the key's value and must not decide.
        PreparedCandidate[] candidates =
        [
            Candidate(status, 1, supersededByRow: true),
            Candidate(TxStatus.InFlight, 2),
        ];
        Assert.That(AtomicVisibilityGate.SelectDecidingPrepare(candidates), Is.EqualTo(-1));
    }
}
