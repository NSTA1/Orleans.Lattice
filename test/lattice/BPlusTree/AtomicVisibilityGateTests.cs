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
}
