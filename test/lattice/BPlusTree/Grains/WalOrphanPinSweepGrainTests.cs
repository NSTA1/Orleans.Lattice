using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="WalOrphanPinSweepGrain"/>, the tree-keyed coordinator
/// that runs the WAL GC orphan-pin sweep once per interval for the whole cluster
/// instead of once per silo (issue #5097).
/// </summary>
[TestFixture]
public class WalOrphanPinSweepGrainTests
{
    private const string TreeId = "tree-a";

    private sealed class CountingRunner : IWalOrphanPinSweepRunner
    {
        public List<(string TreeId, bool Classify)> Calls { get; } = [];

        public Exception? Throw { get; set; }

        public Task<WalOrphanPinSweepOutcome?> RunAsync(
            string treeId,
            bool classifyFloorHolders,
            CancellationToken cancellationToken)
        {
            Calls.Add((treeId, classifyFloorHolders));
            if (Throw is not null)
            {
                throw Throw;
            }

            return Task.FromResult<WalOrphanPinSweepOutcome?>(new WalOrphanPinSweepOutcome
            {
                Classified = classifyFloorHolders,
                RepairConsumerIds = classifyFloorHolders ? ["c1"] : null,
                RequireOffsetAdvance = classifyFloorHolders ? [] : null,
                PinPopulation = 3,
            });
        }
    }

    private static (WalOrphanPinSweepGrain Grain, CountingRunner Runner, ManualTimeProvider Clock) Create()
    {
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-orphan-pin-sweep", TreeId));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var runner = new CountingRunner();
        var clock = new ManualTimeProvider();
        var grain = new WalOrphanPinSweepGrain(
            context,
            Substitute.For<IReminderRegistry>(),
            runner,
            NullLogger<WalOrphanPinSweepGrain>.Instance)
        {
            TimeProvider = clock,
        };
        return (grain, runner, clock);
    }

    [Test]
    public async Task RequestSweepAsync_returns_null_before_the_first_sweep()
    {
        var (grain, runner, _) = Create();

        Assert.That(await grain.RequestSweepAsync(classifyFloorHolders: true), Is.Null);
        Assert.That(runner.Calls, Is.Empty);
    }

    [Test]
    public async Task ProcessNextPhaseAsync_without_demand_does_not_sweep()
    {
        var (grain, runner, _) = Create();

        await grain.ProcessNextPhaseAsync();

        Assert.That(runner.Calls, Is.Empty);
    }

    [Test]
    public async Task Demand_from_many_callers_is_coalesced_into_one_sweep()
    {
        var (grain, runner, _) = Create();

        await grain.RequestSweepAsync(classifyFloorHolders: false);
        await grain.RequestSweepAsync(classifyFloorHolders: true);
        await grain.RequestSweepAsync(classifyFloorHolders: false);
        await grain.ProcessNextPhaseAsync();

        Assert.That(runner.Calls, Is.EqualTo(new[] { (TreeId, true) }));
        var outcome = await grain.RequestSweepAsync(classifyFloorHolders: false);
        Assert.That(outcome, Is.Not.Null);
        Assert.That(outcome!.Classified, Is.True);
        Assert.That(outcome.RepairConsumerIds, Is.EqualTo(new[] { "c1" }));
        Assert.That(outcome.Generation, Is.EqualTo(1));
        Assert.That(outcome.Epoch, Is.Not.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task A_second_sweep_waits_for_the_orphan_sweep_interval()
    {
        var (grain, runner, clock) = Create();
        await grain.RequestSweepAsync(classifyFloorHolders: true);
        await grain.ProcessNextPhaseAsync();

        await grain.RequestSweepAsync(classifyFloorHolders: true);
        clock.Advance(LatticeWalGcScheduler.OrphanSweepInterval - TimeSpan.FromSeconds(1));
        await grain.ProcessNextPhaseAsync();
        Assert.That(runner.Calls, Has.Count.EqualTo(1));

        clock.Advance(TimeSpan.FromSeconds(1));
        await grain.ProcessNextPhaseAsync();
        Assert.That(runner.Calls, Has.Count.EqualTo(2));

        var outcome = await grain.RequestSweepAsync(classifyFloorHolders: false);
        Assert.That(outcome!.Generation, Is.EqualTo(2));
    }

    [Test]
    public async Task A_failed_sweep_keeps_its_demand_owed()
    {
        var (grain, runner, clock) = Create();
        runner.Throw = new InvalidOperationException("storage down");
        await grain.RequestSweepAsync(classifyFloorHolders: true);

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextPhaseAsync());

        runner.Throw = null;
        clock.Advance(LatticeWalGcScheduler.OrphanSweepInterval);
        await grain.ProcessNextPhaseAsync();

        Assert.That(runner.Calls, Has.Count.EqualTo(2));
        Assert.That(runner.Calls[1].Classify, Is.True);
        Assert.That((await grain.RequestSweepAsync(false))!.Generation, Is.EqualTo(1));
    }
}
