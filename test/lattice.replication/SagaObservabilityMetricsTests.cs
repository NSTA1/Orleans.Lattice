using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for the coordinated cross-cluster restore saga observability
/// instruments added to <see cref="LatticeReplicationMetrics"/>: their canonical
/// names, units, tag keys, tag values, and that each fires with the expected
/// value and tags through a <see cref="MeterCollector{T}"/>.
/// </summary>
[TestFixture]
public class SagaObservabilityMetricsTests
{
    [Test]
    public void Saga_instrument_names_and_units_are_canonical()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.SagaPhaseDuration.Name,
                Is.EqualTo("orleans.lattice.replication.saga.phase.duration"));
            Assert.That(LatticeReplicationMetrics.SagaPhaseDuration.Unit, Is.EqualTo("ms"));
            Assert.That(LatticeReplicationMetrics.SagaPhaseDurationName,
                Is.EqualTo("orleans.lattice.replication.saga.phase.duration"));

            Assert.That(LatticeReplicationMetrics.SagaFenceDuration.Name,
                Is.EqualTo("orleans.lattice.replication.saga.fence.duration"));
            Assert.That(LatticeReplicationMetrics.SagaFenceDuration.Unit, Is.EqualTo("ms"));
            Assert.That(LatticeReplicationMetrics.SagaFenceDurationName,
                Is.EqualTo("orleans.lattice.replication.saga.fence.duration"));

            Assert.That(LatticeReplicationMetrics.SagaParticipantVotes.Name,
                Is.EqualTo("orleans.lattice.replication.saga.participant.votes"));
            Assert.That(LatticeReplicationMetrics.SagaParticipantVotes.Unit, Is.EqualTo("{vote}"));
            Assert.That(LatticeReplicationMetrics.SagaParticipantVotesName,
                Is.EqualTo("orleans.lattice.replication.saga.participant.votes"));

            Assert.That(LatticeReplicationMetrics.SagaParticipantCommits.Name,
                Is.EqualTo("orleans.lattice.replication.saga.participant.commits"));
            Assert.That(LatticeReplicationMetrics.SagaParticipantCommits.Unit, Is.EqualTo("{commit}"));
            Assert.That(LatticeReplicationMetrics.SagaParticipantCommitsName,
                Is.EqualTo("orleans.lattice.replication.saga.participant.commits"));

            Assert.That(LatticeReplicationMetrics.SagaParticipantAborts.Name,
                Is.EqualTo("orleans.lattice.replication.saga.participant.aborts"));
            Assert.That(LatticeReplicationMetrics.SagaParticipantAborts.Unit, Is.EqualTo("{abort}"));
            Assert.That(LatticeReplicationMetrics.SagaParticipantAbortsName,
                Is.EqualTo("orleans.lattice.replication.saga.participant.aborts"));

            Assert.That(LatticeReplicationMetrics.SagaCompensations.Name,
                Is.EqualTo("orleans.lattice.replication.saga.compensations"));
            Assert.That(LatticeReplicationMetrics.SagaCompensations.Unit, Is.EqualTo("{compensation}"));
            Assert.That(LatticeReplicationMetrics.SagaCompensationsName,
                Is.EqualTo("orleans.lattice.replication.saga.compensations"));
        });
    }

    [Test]
    public void Saga_tag_keys_and_values_use_canonical_names()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.TagPhase, Is.EqualTo("phase"));
            Assert.That(LatticeReplicationMetrics.TagCause, Is.EqualTo("cause"));

            Assert.That(LatticeReplicationMetrics.SagaPhasePrepare, Is.EqualTo("prepare"));
            Assert.That(LatticeReplicationMetrics.SagaPhaseCommit, Is.EqualTo("commit"));
            Assert.That(LatticeReplicationMetrics.SagaPhaseAbort, Is.EqualTo("abort"));

            Assert.That(LatticeReplicationMetrics.SagaCauseVoteAbort, Is.EqualTo("vote-abort"));
            Assert.That(LatticeReplicationMetrics.SagaCauseCoordinatorLoss, Is.EqualTo("coordinator-loss"));

            Assert.That(LatticeReplicationMetrics.SagaReasonCommit, Is.EqualTo("commit"));
            Assert.That(LatticeReplicationMetrics.SagaReasonEngineUnavailable, Is.EqualTo("engine-unavailable"));
            Assert.That(LatticeReplicationMetrics.SagaReasonInfeasible, Is.EqualTo("infeasible"));
            Assert.That(LatticeReplicationMetrics.SagaReasonPrecondition, Is.EqualTo("precondition"));
            Assert.That(LatticeReplicationMetrics.SagaReasonBuildFailed, Is.EqualTo("build-failed"));
            Assert.That(LatticeReplicationMetrics.SagaReasonSingle, Is.EqualTo("single"));
            Assert.That(LatticeReplicationMetrics.SagaReasonSet, Is.EqualTo("set"));
        });
    }

    [Test]
    public void Saga_phase_duration_histogram_records_with_phase_tag()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.SagaPhaseDurationName);

        LatticeReplicationMetrics.SagaPhaseDuration.Record(42.5,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagPhase, LatticeReplicationMetrics.SagaPhasePrepare));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(42.5));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "phase" && (string?)t.Value == "prepare"));
        });
    }

    [Test]
    public void Saga_fence_duration_histogram_records_with_tree_tag()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.SagaFenceDurationName);

        LatticeReplicationMetrics.SagaFenceDuration.Record(7.0,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, "orders"));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(7.0));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == "orders"));
        });
    }

    [Test]
    public void Saga_participant_vote_counter_records_with_reason_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.SagaParticipantVotesName);

        LatticeReplicationMetrics.SagaParticipantVotes.Add(1,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.SagaReasonInfeasible));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "infeasible"));
        });
    }

    [Test]
    public void Saga_participant_commit_counter_records_with_reason_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.SagaParticipantCommitsName);

        LatticeReplicationMetrics.SagaParticipantCommits.Add(1,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.SagaReasonSingle));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "single"));
        });
    }

    [Test]
    public void Saga_participant_abort_counter_records_with_reason_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.SagaParticipantAbortsName);

        LatticeReplicationMetrics.SagaParticipantAborts.Add(1,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.SagaReasonSet));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == "set"));
        });
    }

    [Test]
    public void Saga_compensation_counter_records_with_cause_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.SagaCompensationsName);

        LatticeReplicationMetrics.SagaCompensations.Add(1,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagCause, LatticeReplicationMetrics.SagaCauseCoordinatorLoss));

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "cause" && (string?)t.Value == "coordinator-loss"));
        });
    }
}
