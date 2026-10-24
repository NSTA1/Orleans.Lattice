using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task Coverage_timer_dispatch_releases_the_turn_while_storage_is_pending()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var head = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? drive = null;
        try
        {
            var timer = Substitute.For<IGrainTimer>();
            var registry = Substitute.For<ITimerRegistry>();
            registry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>()).Returns(timer);
            var wal = new GrowingWal();
            var (grain, state, _, _) = CreateGrainWithMaterialiser(wal.Coordinator,
                treeId: UniqueStarvationDriveTree(), persistedCheckpoint: -1,
                starvationDriveBudget: TimeSpan.FromSeconds(10),
                maxConcurrentReplays: TimerAdmissionCeiling, timerRegistry: registry);
            await ActivateAsync(grain);
            SeedRow(grain);
            wal.GrowTo(3);
            registry.ClearReceivedCalls();

            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            var registrations = registry.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)).ToArray();
            Specification.Assert(registrations.Length == 1, "[Dispatch] one deferred drive, not an inline await");
            var arguments = registrations[0].GetArguments();
            var options = (GrainTimerCreationOptions)arguments[3]!;
            Specification.Assert(options.Interleave && options.DueTime == TimeSpan.Zero
                && options.Period == Timeout.InfiniteTimeSpan, "[NoAwaitedDrive] one-shot interleaving turn");
            Specification.Assert(state.State.ProjectionCheckpointOffset == -1, "[Dispatch] storage has not run inline");

            await grain.GetVisibleRawEntriesAsync(["foreground"]);
            wal.Coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(head.Task);
            drive = ((Func<CancellationToken, Task>)arguments[2]!)(CancellationToken.None);
            Specification.Assert(!drive.IsCompleted, "[Complete] storage is deliberately still pending");
            var foreground = grain.GetVisibleRawEntriesAsync(["foreground"]);
            Specification.Assert(foreground.IsCompletedSuccessfully,
                "[EveryCallServed/TurnServedWithinBound] foreground local work need not await the drive");
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            Specification.Assert(registry.ReceivedCalls().Count(c =>
                c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)) == 1,
                "[ExclusiveTurn] a pending drive cannot dispatch another");

            head.TrySetResult(3);
            await drive;
            Specification.Assert(!grain.InterleavedTimerDrivePendingForTest,
                "[CallReturn/Complete] completion clears the outstanding dispatch");
            timer.Received(1).Dispose();
        }
        finally
        {
            head.TrySetResult(3);
            if (drive is not null)
                await drive;
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [Category("Coyote")]
    [NonParallelizable]
    public void Coverage_timer_choices_preserve_single_dispatch_and_foreground_service()
        => CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new TurnOccupancyModel(), iterations: 100);

    private sealed class TurnOccupancyModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var registry = Substitute.For<ITimerRegistry>();
            registry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>()).Returns(Substitute.For<IGrainTimer>());
            var (grain, _, _, _) = CreateGrainWithMaterialiser(
                Substitute.For<ILeafReplayCoordinatorGrain>(), treeId: null, timerRegistry: registry);
            for (var generation = 0; generation < 2; generation++)
            {
                for (var step = 0; step < 4; step++)
                {
                    var dispatch = grain.DispatchTimerDriveAsync(1, "coyote");
                    Specification.Assert(dispatch.IsCompletedSuccessfully, "[NoAwaitedDrive] dispatch returns locally");
                    var registrations = registry.ReceivedCalls()
                        .Where(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer)).ToArray();
                    Specification.Assert(registrations.Length == generation + 1,
                        "[ExclusiveTurn] at most one outstanding dispatched drive");
                    var arguments = registrations[^1].GetArguments();
                    Specification.Assert(((GrainTimerCreationOptions)arguments[3]!).Interleave,
                        "[NoAwaitedDrive] the deferred turn must interleave");
                    var foreground = grain.GetVisibleRawEntriesAsync(["foreground"]);
                    Specification.Assert(foreground.IsCompletedSuccessfully, "[EveryCallServed] foreground service is local");
                    if (runtime.RandomBoolean() || step == 3)
                    {
                        var finish = ((Func<CancellationToken, Task>)arguments[2]!)(CancellationToken.None);
                        Specification.Assert(finish.IsCompletedSuccessfully && !grain.InterleavedTimerDrivePendingForTest,
                            "[Complete] an empty drive returns its dispatch ownership");
                        break;
                    }
                }
            }
        }
    }
}
