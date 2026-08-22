using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="ReaderStabilityGate"/> - the
/// reader-side stability rule the production <c>LatticeGrain</c> multi-shard read
/// retry and the Coyote atomic-commit model both execute. These pin the cheap
/// revision probe and the asymmetric snapshot-disambiguation rule so a
/// regression is caught here rather than only by a slow integration run.
/// </summary>
[TestFixture]
public sealed class ReaderStabilityGateTests
{
    [Test]
    public void IsRevisionStable_equal_revisions_is_stable()
    {
        Assert.That(ReaderStabilityGate.IsRevisionStable(4, 4), Is.True);
    }

    [Test]
    public void IsRevisionStable_advanced_revision_is_unstable()
    {
        Assert.That(ReaderStabilityGate.IsRevisionStable(4, 5), Is.False);
    }

    [Test]
    public void IsRevisionStable_lower_observed_revision_is_unstable()
    {
        // A defensive case: any inequality means the captured snapshot is no
        // longer authoritative, so the read must not be certified.
        Assert.That(ReaderStabilityGate.IsRevisionStable(5, 4), Is.False);
    }

    [Test]
    public void IsSnapshotStable_null_snap2_is_stable()
    {
        var snap1 = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, null), Is.True);
    }

    [Test]
    public void IsSnapshotStable_empty_snap2_is_stable()
    {
        var snap1 = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        var snap2 = new Dictionary<Guid, TxStatus>();

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.True);
    }

    [Test]
    public void IsSnapshotStable_new_committed_in_snap2_is_unstable()
    {
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus>();
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.False);
    }

    [Test]
    public void IsSnapshotStable_in_flight_to_committed_transition_is_unstable()
    {
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight };
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.False);
    }

    [Test]
    public void IsSnapshotStable_already_committed_in_both_is_stable()
    {
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.True);
    }

    [Test]
    public void IsSnapshotStable_new_aborted_in_snap2_is_stable()
    {
        // An Aborted transition removes pending entries everywhere and never
        // surfaces a value, so it cannot tear a read - it must not invalidate.
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus>();
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Aborted };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.True);
    }

    [Test]
    public void IsSnapshotStable_forget_between_snapshots_is_stable()
    {
        // snap1 has the decision, snap2 has forgotten it: a forget implies every
        // leaf already drained the terminal, so the read stays consistent.
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        var snap2 = new Dictionary<Guid, TxStatus>();

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.True);
    }

    [Test]
    public void IsSnapshotStable_committed_in_snap2_with_null_snap1_is_unstable()
    {
        var txid = Guid.NewGuid();
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(null, snap2), Is.False);
    }

    [Test]
    public void IsSnapshotStable_mixed_transitions_isolates_the_committed_one()
    {
        var committedTx = Guid.NewGuid();
        var abortedTx = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus>();
        var snap2 = new Dictionary<Guid, TxStatus>
        {
            [committedTx] = TxStatus.Committed,
            [abortedTx] = TxStatus.Aborted,
        };

        Assert.That(ReaderStabilityGate.IsSnapshotStable(snap1, snap2), Is.False);
    }
}
