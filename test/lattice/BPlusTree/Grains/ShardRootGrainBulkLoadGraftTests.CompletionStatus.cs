using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5812: a caller whose bulk load outran the response timeout reads the load's
/// status from the shard root instead of re-issuing it behind the first. The read
/// interleaves with the running load and reports it running, then completed.
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    [Test]
    public async Task A_bulk_load_reads_running_while_it_runs_and_completed_once_it_has()
    {
        var rig = CreateHintRig();
        var headRead = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(_ => headRead.Task);

        Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.NotRunning),
            "precondition: nothing has run");

        var load = rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));
        Assert.Multiple(async () =>
        {
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Running),
                "the load is suspended reading its birth heads");
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("other"), Is.EqualTo(BulkOperationStatus.NotRunning),
                "another operation id is not the running load");
        });

        headRead.SetResult(BirthHead);
        await load;

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Completed));
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("other"), Is.EqualTo(BulkOperationStatus.NotRunning));
        });
    }

    [Test]
    public async Task A_bulk_load_reads_running_until_its_switch_tail_completes_not_from_its_publish()
    {
        // Review S1 of #5815: the load records itself complete when it publishes its
        // root, before its switch's tail has relocated the replaced root and closed by
        // fence. A caller reading Completed there could proceed or cut over mid-tail.
        var rig = CreateHintRig();
        var close = new TaskCompletionSource<WalAppendFenceClaimResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Wal.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ => close.Task);

        var load = rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));
        Assert.Multiple(async () =>
        {
            Assert.That(rig.H.State.State.LastCompletedBulkOperationId, Is.EqualTo("load"),
                "precondition: the load has published and recorded itself complete");
            Assert.That(load.IsCompleted, Is.False, "precondition: its switch's close is still in flight");
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Running),
                "the load is running until its tail completes");
        });

        close.SetResult(WalAppendFenceClaimResult.Accept(CloseHead));
        await load;
        Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Completed));
    }

    [Test]
    public async Task A_bulk_load_whose_switch_close_faults_after_it_published_reads_not_running_and_its_reissue_closes_the_switch()
    {
        // Review S1 of #5815, second round: the load persisted its completion when it
        // published, so a tail that then faulted read as Completed and nobody re-issued it.
        var timers = TimerRegistry();
        var rig = CreateHintRig(timers: timers);
        FaultFirstClose(rig);

        Assert.ThrowsAsync<InvalidOperationException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));
        Assert.Multiple(async () =>
        {
            Assert.That(rig.H.State.State.LastCompletedBulkOperationId, Is.EqualTo("load"),
                "precondition: the load published and recorded itself complete");
            Assert.That((await ShardMarkerAsync(rig)).Open, Does.ContainKey("bulk:load"),
                "precondition: its switch is still open");
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.NotRunning),
                "a load whose tail faulted is re-issued");
            Assert.That(BulkIdleClosesArmed(timers), Is.EqualTo(1), "the faulted tail arms the idle close");
        });

        await rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(async () =>
        {
            Assert.That(marker.Open, Does.Not.ContainKey("bulk:load"), "the re-issue closes the switch");
            Assert.That(marker.Closes.Select(c => c.Token), Does.Contain("bulk:load"));
            Assert.That(marker.BulkBirthHints, Does.Not.ContainKey("load"));
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Completed));
        });
    }

    [Test]
    public async Task A_new_activation_of_a_shard_whose_bulk_switch_was_left_open_closes_it_by_its_idle_close()
    {
        // Review S1 of #5815, second round: the faulted load is remembered in activation
        // memory only, so the activation after it closes the switch itself.
        var rig = CreateHintRig();
        FaultFirstClose(rig);
        Assert.ThrowsAsync<InvalidOperationException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));
        Assert.That((await ShardMarkerAsync(rig)).Open, Does.ContainKey("bulk:load"), "precondition: the switch is left open");

        var timers = TimerRegistry();
        var next = CreateHintRig(timers: timers);
        next.H.State.State = rig.H.State.State;
        next.H.Grain.UseHandOffStorageForTesting(rig.Storage);
        await ((IGrainBase)next.H.Grain).OnActivateAsync(CancellationToken.None);

        Assert.That(BulkIdleClosesArmed(timers), Is.EqualTo(1), "the activation arms the idle close");
        await CapturedBulkIdleClose(timers)(CancellationToken.None);

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Does.Not.ContainKey("bulk:load"), "the idle close closes the left-open switch");
            Assert.That(marker.Closes.Select(c => c.Token), Does.Contain("bulk:load"));
        });
    }

    [Test]
    public async Task An_activation_of_a_shard_that_never_bulk_loaded_arms_no_idle_close()
    {
        var timers = TimerRegistry();
        var rig = CreateHintRig(timers: timers);

        await ((IGrainBase)rig.H.Grain).OnActivateAsync(CancellationToken.None);

        Assert.That(BulkIdleClosesArmed(timers), Is.Zero, "the healthy path pays nothing");
    }

    private static void FaultFirstClose(HintRig rig)
    {
        var claims = 0;
        rig.Wal.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ => Interlocked.Increment(ref claims) == 1
                ? Task.FromException<WalAppendFenceClaimResult>(new InvalidOperationException("the close's claim failed"))
                : Task.FromResult(WalAppendFenceClaimResult.Accept(CloseHead)));
    }

    private static ITimerRegistry TimerRegistry()
    {
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(_ => Substitute.For<IGrainTimer>());
        return timers;
    }

    private static IEnumerable<NSubstitute.Core.ICall> BulkIdleCloseRegistrations(ITimerRegistry timers) =>
        timers.ReceivedCalls().Where(c =>
            c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)
            && c.GetArguments()[3] is GrainTimerCreationOptions { DueTime: var due } && due == ShardRootGrain.BulkSwitchIdleClose);

    private static int BulkIdleClosesArmed(ITimerRegistry timers) => BulkIdleCloseRegistrations(timers).Count();

    private static Func<CancellationToken, Task> CapturedBulkIdleClose(ITimerRegistry timers) =>
        (Func<CancellationToken, Task>)BulkIdleCloseRegistrations(timers).Last().GetArguments()[2]!;

    [Test]
    public async Task A_bulk_load_that_faults_reads_not_running()
    {
        var rig = CreateHintRig();
        rig.Coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new InvalidOperationException("head read failed")));

        Assert.ThrowsAsync<InvalidOperationException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));

        Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.NotRunning),
            "a faulted load is issued again");
    }
}
