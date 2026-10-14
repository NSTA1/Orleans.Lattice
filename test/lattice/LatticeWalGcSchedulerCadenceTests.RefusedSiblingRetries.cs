using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    [NonParallelizable]
    public async Task A_pass_with_only_admission_refusals_touches_each_leaf_once_and_consumes_no_attempts()
    {
        const int Population = 4;
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(LivenessLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, LivenessConsumerId(i), UsablePin, FloorOffset + (i * 10));
        }

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));
        var drives = new List<GrainId>();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(call =>
        {
            var id = call.ArgAt<GrainId>(0);
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.BankDurablePinAsync().Returns(Task.CompletedTask);
            leaf.DriveStarvedCheckpointAsync().Returns(_ =>
            {
                drives.Add(id);
                return Task.FromResult(LeafStarvationDriveOutcome.AdmissionRefused);
            });
            return leaf;
        });
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        try
        {
            var time = new VirtualTimeProvider();
            var scheduler = CreateScheduler(
                factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage);
            using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);
            await StartAndRunFirstPassAsync(scheduler, time);
            try
            {
                var ticks = 0;
                while (drives.Count == 0)
                {
                    await TickAsync(time);
                    Assert.That(++ticks, Is.LessThan(100), "the scheduler never reached its floor-holder drive.");
                }

                Assert.Multiple(() =>
                {
                    Assert.That(drives, Is.EquivalentTo(Enumerable.Range(0, Population).Select(LivenessLeafGrainId)),
                        "refused siblings cannot release replay capacity, so each selected leaf runs only once.");
                    Assert.That(Outcomes(recorder, "admission_refused"), Is.EqualTo(Population));
                    Assert.That(Outcomes(recorder, "attempted"), Is.Zero);
                });

                await AdvanceAtLeastAsync(time, TimeSpan.FromHours(2));
                Assert.Multiple(() =>
                {
                    Assert.That(drives.Count, Is.GreaterThan(Population), "cross-pass admission retries must remain enabled.");
                    Assert.That(Outcomes(recorder, "attempted"), Is.Zero);
                    Assert.That(Outcomes(recorder, "abandoned"), Is.Zero,
                        "continued admission refusals never consume MaxReactivationAttempts.");
                });
            }
            finally
            {
                await scheduler.StopAsync(CancellationToken.None);
            }
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
