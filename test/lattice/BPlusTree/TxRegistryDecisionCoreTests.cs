using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="TxRegistryDecisionCore"/> -
/// the recording-side core the production tx-registry grain and the Coyote
/// atomic-commit model both execute. These pin the monotonic-revision contract
/// (bump iff the decision map actually changed; exactly one bump per mutation;
/// clean rollback of both map and revision on a failed durable write) so a
/// regression is caught here rather than only by a slow integration run.
/// </summary>
[TestFixture]
public sealed class TxRegistryDecisionCoreTests
{
    private static TxRegistryDecisionCore NewCore(long revision = 0) =>
        new(new Dictionary<Guid, TxStatus>(), revision);

    [Test]
    public void Ctor_null_decisions_throws()
    {
        Assert.That(
            () => new TxRegistryDecisionCore(null!, 0),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_seeds_revision_and_count_from_supplied_map()
    {
        var txid = Guid.NewGuid();
        var map = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };

        var core = new TxRegistryDecisionCore(map, 7);

        Assert.That(core.Revision, Is.EqualTo(7));
        Assert.That(core.Count, Is.EqualTo(1));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Fresh_core_has_revision_zero_and_no_decisions()
    {
        var core = NewCore();

        Assert.That(core.Revision, Is.EqualTo(0));
        Assert.That(core.Count, Is.EqualTo(0));
    }

    [Test]
    public void Resolve_absent_txid_returns_in_flight_default()
    {
        var core = NewCore();

        Assert.That(core.Resolve(Guid.NewGuid()), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public void TryResolve_absent_txid_reports_false()
    {
        var core = NewCore();

        var present = core.TryResolve(Guid.NewGuid(), out var status);

        Assert.That(present, Is.False);
        Assert.That(status, Is.EqualTo(default(TxStatus)));
    }

    [Test]
    public void TryResolve_present_txid_reports_true_and_status()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Aborted);

        var present = core.TryResolve(txid, out var status);

        Assert.That(present, Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public void Apply_new_decision_bumps_revision_by_one_and_records_it()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();

        var mutation = core.Apply(txid, TxStatus.Committed);

        Assert.That(core.Revision, Is.EqualTo(1));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Committed));
        Assert.That(mutation.Bumped, Is.True);
        Assert.That(mutation.HadPrevious, Is.False);
        Assert.That(mutation.PreviousRevision, Is.EqualTo(0));
    }

    [Test]
    public void Apply_same_outcome_again_is_idempotent_and_does_not_bump()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);

        var mutation = core.Apply(txid, TxStatus.Committed);

        Assert.That(core.Revision, Is.EqualTo(1));
        Assert.That(mutation.Bumped, Is.False);
        Assert.That(mutation.HadPrevious, Is.True);
        Assert.That(mutation.PreviousStatus, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Apply_conflicting_outcome_overwrites_and_bumps()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);

        var mutation = core.Apply(txid, TxStatus.Aborted);

        Assert.That(core.Revision, Is.EqualTo(2));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Aborted));
        Assert.That(mutation.Bumped, Is.True);
        Assert.That(mutation.HadPrevious, Is.True);
        Assert.That(mutation.PreviousStatus, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Sequential_distinct_applies_are_strictly_monotonic()
    {
        var core = NewCore();

        core.Apply(Guid.NewGuid(), TxStatus.Committed);
        core.Apply(Guid.NewGuid(), TxStatus.Aborted);
        core.Apply(Guid.NewGuid(), TxStatus.Committed);

        Assert.That(core.Revision, Is.EqualTo(3));
        Assert.That(core.Count, Is.EqualTo(3));
    }

    [Test]
    public void Remove_present_decision_bumps_and_drops_entry()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);

        var mutation = core.Remove(txid);

        Assert.That(core.Revision, Is.EqualTo(2));
        Assert.That(core.Count, Is.EqualTo(0));
        Assert.That(mutation.Bumped, Is.True);
        Assert.That(mutation.HadPrevious, Is.True);
        Assert.That(mutation.PreviousStatus, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Remove_absent_decision_does_not_bump()
    {
        var core = NewCore();

        var mutation = core.Remove(Guid.NewGuid());

        Assert.That(core.Revision, Is.EqualTo(0));
        Assert.That(mutation.Bumped, Is.False);
        Assert.That(mutation.HadPrevious, Is.False);
    }

    [Test]
    public void AdvanceRevision_bumps_unconditionally_and_returns_prior()
    {
        var core = NewCore(revision: 5);

        var prior = core.AdvanceRevision();

        Assert.That(prior, Is.EqualTo(5));
        Assert.That(core.Revision, Is.EqualTo(6));
    }

    [Test]
    public void Rollback_of_new_apply_removes_entry_and_restores_revision()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        var mutation = core.Apply(txid, TxStatus.Committed);

        core.Rollback(mutation);

        Assert.That(core.Revision, Is.EqualTo(0));
        Assert.That(core.Count, Is.EqualTo(0));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public void Rollback_of_overwrite_restores_previous_status_and_revision()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);
        var mutation = core.Apply(txid, TxStatus.Aborted);

        core.Rollback(mutation);

        Assert.That(core.Revision, Is.EqualTo(1));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Rollback_of_remove_restores_entry_and_revision()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);
        var mutation = core.Remove(txid);

        core.Rollback(mutation);

        Assert.That(core.Revision, Is.EqualTo(1));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Rollback_of_non_bumping_mutation_is_a_no_op()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);
        var idempotent = core.Apply(txid, TxStatus.Committed);

        core.Rollback(idempotent);

        Assert.That(core.Revision, Is.EqualTo(1));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void RollbackRevision_restores_counter_without_touching_map()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);
        var prior = core.AdvanceRevision();

        core.RollbackRevision(prior);

        Assert.That(core.Revision, Is.EqualTo(1));
        Assert.That(core.Resolve(txid), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public void Snapshot_captures_map_copy_paired_with_current_revision()
    {
        var core = NewCore();
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        core.Apply(a, TxStatus.Committed);
        core.Apply(b, TxStatus.Aborted);

        var snapshot = core.Snapshot();

        Assert.That(snapshot.Revision, Is.EqualTo(2));
        Assert.That(snapshot.Decisions, Has.Count.EqualTo(2));
        Assert.That(snapshot.Decisions[a], Is.EqualTo(TxStatus.Committed));
        Assert.That(snapshot.Decisions[b], Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public void Snapshot_is_a_defensive_copy_unaffected_by_later_mutation()
    {
        var core = NewCore();
        var txid = Guid.NewGuid();
        core.Apply(txid, TxStatus.Committed);

        var snapshot = core.Snapshot();
        core.Apply(Guid.NewGuid(), TxStatus.Aborted);

        Assert.That(snapshot.Decisions, Has.Count.EqualTo(1));
        Assert.That(snapshot.Revision, Is.EqualTo(1));
    }

    [Test]
    public void Snapshot_include_predicate_filters_excluded_txids()
    {
        var core = NewCore();
        var kept = Guid.NewGuid();
        var dropped = Guid.NewGuid();
        core.Apply(kept, TxStatus.Committed);
        core.Apply(dropped, TxStatus.Committed);

        var snapshot = core.Snapshot(txid => txid == kept);

        Assert.That(snapshot.Decisions, Has.Count.EqualTo(1));
        Assert.That(snapshot.Decisions.ContainsKey(kept), Is.True);
        Assert.That(snapshot.Decisions.ContainsKey(dropped), Is.False);
        Assert.That(snapshot.Revision, Is.EqualTo(2));
    }

    [Test]
    public void Apply_mutates_the_wrapped_map_by_reference()
    {
        var backing = new Dictionary<Guid, TxStatus>();
        var core = new TxRegistryDecisionCore(backing, 0);
        var txid = Guid.NewGuid();

        core.Apply(txid, TxStatus.Committed);

        Assert.That(backing.ContainsKey(txid), Is.True);
        Assert.That(backing[txid], Is.EqualTo(TxStatus.Committed));
    }
}
