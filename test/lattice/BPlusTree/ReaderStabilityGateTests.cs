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
    public void ClassifySnapshot_null_snap2_is_unverifiable_not_stable()
    {
        var snap1 = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };

        // Issue #3641: an unreachable disambiguation snapshot used to read as
        // stable, which let a registry blip certify a torn read.
        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, null), Is.EqualTo(ReaderStabilityVerdict.Unverifiable));
    }

    [Test]
    public void ClassifySnapshot_empty_snap2_is_stable()
    {
        var snap1 = new Dictionary<Guid, TxStatus> { [Guid.NewGuid()] = TxStatus.Committed };
        var snap2 = new Dictionary<Guid, TxStatus>();

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Stable));
    }

    [Test]
    public void ClassifySnapshot_new_committed_in_snap2_is_unstable()
    {
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus>();
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Unstable));
    }

    [Test]
    public void ClassifySnapshot_in_flight_to_committed_transition_is_unstable()
    {
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight };
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Unstable));
    }

    [Test]
    public void ClassifySnapshot_already_committed_in_both_is_stable()
    {
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Stable));
    }

    [Test]
    public void ClassifySnapshot_new_aborted_in_snap2_is_stable()
    {
        // An Aborted transition removes pending entries everywhere and never
        // surfaces a value, so it cannot tear a read - it must not invalidate.
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus>();
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Aborted };

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Stable));
    }

    [Test]
    public void ClassifySnapshot_forget_between_snapshots_is_stable()
    {
        // snap1 has the decision, snap2 has forgotten it: a forget implies every
        // leaf already drained the terminal, so the read stays consistent.
        var txid = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        var snap2 = new Dictionary<Guid, TxStatus>();

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Stable));
    }

    [Test]
    public void ClassifySnapshot_committed_in_snap2_with_null_snap1_is_unstable()
    {
        var txid = Guid.NewGuid();
        var snap2 = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        Assert.That(ReaderStabilityGate.ClassifySnapshot(null, snap2), Is.EqualTo(ReaderStabilityVerdict.Unstable));
    }

    [Test]
    public void ClassifySnapshot_mixed_transitions_isolates_the_committed_one()
    {
        var committedTx = Guid.NewGuid();
        var abortedTx = Guid.NewGuid();
        var snap1 = new Dictionary<Guid, TxStatus>();
        var snap2 = new Dictionary<Guid, TxStatus>
        {
            [committedTx] = TxStatus.Committed,
            [abortedTx] = TxStatus.Aborted,
        };

        Assert.That(ReaderStabilityGate.ClassifySnapshot(snap1, snap2), Is.EqualTo(ReaderStabilityVerdict.Unstable));
    }

    // ---- Decide: combining the verdict with the prepared-key signal (#3641) ----

    [TestCase(false)]
    [TestCase(true)]
    public void Decide_stable_is_accepted_whether_or_not_a_prepared_key_was_resolved(bool resolvedPreparedKey)
    {
        Assert.That(ReaderStabilityGate.Decide(ReaderStabilityVerdict.Stable, resolvedPreparedKey),
            Is.EqualTo(ReaderAttemptDecision.Accept));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Decide_unstable_is_retried_whether_or_not_a_prepared_key_was_resolved(bool resolvedPreparedKey)
    {
        Assert.That(ReaderStabilityGate.Decide(ReaderStabilityVerdict.Unstable, resolvedPreparedKey),
            Is.EqualTo(ReaderAttemptDecision.Retry));
    }

    [Test]
    public void Decide_unverifiable_without_a_resolved_prepared_key_is_accepted()
    {
        // No key's value depended on a saga decision, so the read cannot be torn.
        Assert.That(ReaderStabilityGate.Decide(ReaderStabilityVerdict.Unverifiable, resolvedPreparedKey: false),
            Is.EqualTo(ReaderAttemptDecision.Accept));
    }

    [Test]
    public void Decide_unverifiable_with_a_resolved_prepared_key_is_retried()
    {
        // The fail-open this replaces: accepting here certifies a possibly torn read.
        Assert.That(ReaderStabilityGate.Decide(ReaderStabilityVerdict.Unverifiable, resolvedPreparedKey: true),
            Is.EqualTo(ReaderAttemptDecision.Retry));
    }
}