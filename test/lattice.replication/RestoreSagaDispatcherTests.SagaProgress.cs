using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The dispatcher follows a coordinated restore's saga to its end without holding
/// any one coordinator call open for its length (issue #5506): it begins the saga,
/// asks the coordinator to drive it, and reads its status until it completes.
/// </summary>
public partial class RestoreSagaDispatcherTests
{
    private static LatticeRestoreRequest ReplicatedRequest() =>
        new(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover);

    [Test]
    public async Task TryDispatchAsync_follows_a_saga_that_outlasts_one_call_to_its_committed_outcome()
    {
        var h = CreateHarness();
        var driving = SagaCoordinatorStub.Preparing with { IsDriving = true };
        h.Coordinator.GetStatusAsync().Returns(
            Task.FromResult(driving),
            Task.FromResult(driving),
            Task.FromResult(SagaCoordinatorStub.Completed(CrossClusterSagaOutcome.Committed)));

        var result = await h.Dispatcher.TryDispatchAsync(ReplicatedRequest());

        Assert.That(result, Is.Not.Null, "a committed saga is a successful restore");
        Assert.That(result!.TargetTreeId, Is.EqualTo(TargetTree));
        await h.Coordinator.Received(1).DriveAsync();
        await h.Coordinator.Received(3).GetStatusAsync();
        await h.Coordinator.DidNotReceive().RunAsync(
            Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public async Task TryDispatchAsync_drives_again_a_saga_whose_drive_stopped_short()
    {
        var h = CreateHarness();
        var stopped = SagaCoordinatorStub.Preparing with { Detail = "peer transport fault" };
        h.Coordinator.GetStatusAsync().Returns(
            Task.FromResult(stopped),
            Task.FromResult(SagaCoordinatorStub.Completed(CrossClusterSagaOutcome.Committed)));

        var result = await h.Dispatcher.TryDispatchAsync(ReplicatedRequest());

        Assert.That(result, Is.Not.Null, "the saga committed on its second drive");
        await h.Coordinator.Received(2).DriveAsync();
    }

    [Test]
    public void TryDispatchAsync_reports_the_abort_of_a_saga_that_aborted_after_several_polls()
    {
        var h = CreateHarness();
        var driving = SagaCoordinatorStub.Preparing with { IsDriving = true };
        h.Coordinator.GetStatusAsync().Returns(
            Task.FromResult(driving),
            Task.FromResult(SagaCoordinatorStub.Completed(CrossClusterSagaOutcome.Aborted)));

        Assert.That(async () => await h.Dispatcher.TryDispatchAsync(ReplicatedRequest()),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public void TryDispatchAsync_a_saga_a_participant_refused_reports_that_it_needs_repair()
    {
        var h = CreateHarness();
        const string split = "the saga is split across clusters and needs operator repair.";
        h.Coordinator.GetStatusAsync().Returns(Task.FromResult(
            new CrossClusterSagaStatus(
                CrossClusterSagaPhase.Committed, CrossClusterSagaOutcome.Committed,
                IsDriving: false, RequiresRepair: true, Detail: split)));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await h.Dispatcher.TryDispatchAsync(ReplicatedRequest()));

        Assert.That(ex!.Message, Is.EqualTo(split));
    }

    [Test]
    public async Task TryDispatchAsync_reattaches_to_a_completed_saga_without_driving_it()
    {
        var h = CreateHarness();
        h.Coordinator.BeginAsync(
                Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>())
            .Returns(Task.FromResult(SagaCoordinatorStub.Completed(CrossClusterSagaOutcome.Committed)));

        var result = await h.Dispatcher.TryDispatchAsync(ReplicatedRequest());

        Assert.That(result, Is.Not.Null);
        await h.Coordinator.DidNotReceive().DriveAsync();
        await h.Coordinator.DidNotReceive().GetStatusAsync();
    }

    [Test]
    public void TryDispatchAsync_cancellation_stops_waiting_on_a_running_saga()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        h.Coordinator.GetStatusAsync().Returns(_ =>
        {
            cts.Cancel();
            return Task.FromResult(SagaCoordinatorStub.Preparing with { IsDriving = true });
        });

        Assert.That(async () => await h.Dispatcher.TryDispatchAsync(ReplicatedRequest(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
