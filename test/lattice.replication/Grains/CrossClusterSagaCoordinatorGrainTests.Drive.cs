using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The start / drive / status contract a caller uses to follow a saga without
/// holding one grain call open for the whole saga (issue #5506).
/// </summary>
public partial class CrossClusterSagaCoordinatorGrainTests
{
    private Task<CrossClusterSagaStatus> Begin(CrossClusterSagaCoordinatorGrain grain, params string[] clusters) =>
        grain.BeginAsync([.. clusters], TargetTree, ManifestId, CoordinatorCluster);

    [Test]
    public async Task BeginAsync_records_the_saga_without_preparing_anyone()
    {
        var channel = FinalizingChannel();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var (grain, state, _, _) = CreateGrain(channel: channel);

        var status = await Begin(grain, "site-a");

        Assert.Multiple(() =>
        {
            Assert.That(status.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing));
            Assert.That(status.Outcome, Is.Null);
            Assert.That(status.IsDriving, Is.False);
            Assert.That(status.IsComplete, Is.False);
            Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing), "the record is persisted");
        });
        await channel.DidNotReceive().PrepareAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task DriveAsync_runs_a_begun_saga_to_its_committed_completion()
    {
        var channel = FinalizingChannel();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        StubPrepare(channel, "site-b", SagaVote.Commit);
        var (grain, _, _, _) = CreateGrain(channel: channel);
        await Begin(grain, "site-a", "site-b");

        await grain.DriveAsync();
        var status = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.IsComplete, Is.True);
            Assert.That(status.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
            Assert.That(status.IsDriving, Is.False);
            Assert.That(status.RequiresRepair, Is.False);
        });
        await channel.Received(1).CommitAsync("site-a", Arg.Any<SagaControlRequest>());
        await channel.Received(1).CommitAsync("site-b", Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task GetStatusAsync_reports_a_drive_that_is_still_preparing()
    {
        var channel = FinalizingChannel();
        var vote = new TaskCompletionSource<SagaControlResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.PrepareAsync("site-a", Arg.Any<SagaControlRequest>()).Returns(vote.Task);
        var (grain, _, _, _) = CreateGrain(channel: channel);
        await Begin(grain, "site-a");

        var drive = grain.DriveAsync();
        var during = await grain.GetStatusAsync();
        vote.SetResult(Vote(SagaVote.Commit));
        await drive.WaitAsync(TimeSpan.FromSeconds(30));
        var after = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(during.IsDriving, Is.True, "a drive held in prepare is reported as running");
            Assert.That(during.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing));
            Assert.That(during.Outcome, Is.Null);
            Assert.That(after.IsDriving, Is.False);
            Assert.That(after.IsComplete, Is.True);
            Assert.That(after.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        });
    }

    [Test]
    public async Task DriveAsync_records_a_fault_and_leaves_the_saga_for_the_next_drive()
    {
        var channel = FinalizingChannel();
        channel.PrepareAsync("site-a", Arg.Any<SagaControlRequest>())
            .ThrowsAsync(new InvalidOperationException("peer transport fault"));
        var (grain, state, _, _) = CreateGrain(channel: channel);
        await Begin(grain, "site-a");

        await grain.DriveAsync();
        var stopped = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(stopped.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing), "a faulted drive leaves the saga in place");
            Assert.That(stopped.IsDriving, Is.False);
            Assert.That(stopped.Detail, Does.Contain("peer transport fault"));
            Assert.That(state.State.Outcome, Is.Null, "no decision was recorded");
        });

        StubPrepare(channel, "site-a", SagaVote.Commit);
        await grain.DriveAsync();
        var resumed = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.IsComplete, Is.True);
            Assert.That(resumed.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
            Assert.That(resumed.Detail, Is.Null, "a clean drive clears the earlier fault");
        });
    }

    [Test]
    public async Task GetStatusAsync_reports_a_saga_a_participant_refused_as_needing_repair()
    {
        var channel = FinalizingChannel();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        channel.CommitAsync("site-a", Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Applied(SagaPhase.Aborted)));
        var (grain, _, _, _) = CreateGrain(channel: channel);
        await Begin(grain, "site-a");

        await grain.DriveAsync();
        var status = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.RequiresRepair, Is.True);
            Assert.That(status.IsComplete, Is.False, "a refused saga is never reported complete");
            Assert.That(status.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
            Assert.That(status.Detail, Does.Contain("operator repair"));
        });
    }

    [Test]
    public async Task BeginAsync_on_a_completed_saga_reports_its_memoized_outcome()
    {
        var channel = FinalizingChannel();
        StubPrepare(channel, "site-a", SagaVote.Abort);
        var (grain, _, _, _) = CreateGrain(channel: channel);
        await Run(grain, "site-a");

        var status = await Begin(grain, "site-a");

        Assert.Multiple(() =>
        {
            Assert.That(status.IsComplete, Is.True);
            Assert.That(status.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));
        });
        await channel.Received(1).PrepareAsync("site-a", Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task BeginAsync_with_no_participants_completes_as_a_vacuous_commit()
    {
        var (grain, _, _, _) = CreateGrain();

        var status = await Begin(grain);

        Assert.Multiple(() =>
        {
            Assert.That(status.IsComplete, Is.True);
            Assert.That(status.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        });
    }

    [Test]
    public async Task BeginAsync_reattaching_with_different_arguments_throws()
    {
        var (grain, _, _, _) = CreateGrain();
        await Begin(grain, "site-a");

        Assert.That(async () => await Begin(grain, "site-a", "site-b"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void The_follow_contract_never_queues_behind_or_waits_on_a_running_drive()
    {
        var contract = typeof(ICrossClusterSagaCoordinatorGrain);

        Assert.Multiple(() =>
        {
            Assert.That(contract.GetMethod(nameof(ICrossClusterSagaCoordinatorGrain.BeginAsync))!
                .IsDefined(typeof(Orleans.Concurrency.AlwaysInterleaveAttribute), inherit: false), Is.True,
                "a re-attach must answer while a drive holds the coordinator");
            Assert.That(contract.GetMethod(nameof(ICrossClusterSagaCoordinatorGrain.GetStatusAsync))!
                .IsDefined(typeof(Orleans.Concurrency.AlwaysInterleaveAttribute), inherit: false), Is.True,
                "a status read must answer while a drive holds the coordinator");
            Assert.That(contract.GetMethod(nameof(ICrossClusterSagaCoordinatorGrain.DriveAsync))!
                .IsDefined(typeof(Orleans.Concurrency.OneWayAttribute), inherit: false), Is.True,
                "no caller may wait on a drive, whose length the response timeout does not bound");
        });
    }

    [Test]
    public async Task DriveAsync_on_a_never_started_saga_does_nothing()
    {
        var channel = FinalizingChannel();
        var (grain, state, _, _) = CreateGrain(channel: channel);

        await grain.DriveAsync();

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.NotStarted));
        await channel.DidNotReceive().PrepareAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }
}
