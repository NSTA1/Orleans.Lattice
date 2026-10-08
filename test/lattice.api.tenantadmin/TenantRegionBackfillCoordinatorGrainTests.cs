using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Tenancy;
using Orleans.Runtime;
using Orleans.Timers;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

[TestFixture]
public sealed class TenantRegionBackfillCoordinatorGrainTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public async Task EnsureRunningAsync_persists_work_and_source_before_arming_the_recoverable_pump()
    {
        var harness = Create(TenantRegionStatus.Provisioning);
        var calls = new List<string>();
        harness.State.WriteStateAsync().Returns(_ =>
        {
            calls.Add("persist");
            return Task.CompletedTask;
        });
        harness.Reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                calls.Add("reminder");
                return Task.FromResult(Substitute.For<IGrainReminder>());
            });

        await harness.Grain.EnsureRunningAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.True);
            Assert.That(harness.State.State.SourceClusterId, Is.EqualTo("east"));
            Assert.That(calls, Is.EqualTo(new[] { "persist", "reminder" }));
        });
    }

    [Test]
    public async Task Activation_with_persisted_work_rearms_the_phase_timer_after_restart()
    {
        var harness = Create(TenantRegionStatus.Backfilling, new()
        {
            InProgress = true,
            SourceClusterId = "east",
        });

        await ((IGrainBase)harness.Grain).OnActivateAsync(CancellationToken.None);

        harness.Timers.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
        Assert.That(harness.State.State.InProgress, Is.True);
    }

    [Test]
    public async Task EnsureRunningAsync_reselects_a_source_after_the_pinned_region_leaves_online()
    {
        var harness = Create(TenantRegionStatus.Backfilling, new()
        {
            InProgress = true,
            SourceClusterId = "east",
        });
        var record = harness.Registry.Peek(Acme.Value)!;
        record.SetRegionStatus("east", TenantRegionStatus.Offline, new() { WallClockTicks = 6 }, "seed");
        record.AuthorizeRegion("north", new() { WallClockTicks = 7 }, "seed");
        record.SetRegionStatus("north", TenantRegionStatus.Online, new() { WallClockTicks = 8 }, "seed");

        await harness.Grain.EnsureRunningAsync();

        Assert.That(harness.State.State.SourceClusterId, Is.EqualTo("north"));
        await harness.State.Received(1).WriteStateAsync();
    }

    private static Harness Create(
        TenantRegionStatus targetStatus,
        TenantRegionBackfillCoordinatorState? persistedState = null)
    {
        static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

        var registry = new FakeTenantRegistry();
        var record = TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Stamp(1),
            "seed");
        record.AuthorizeRegion("east", Stamp(2), "seed");
        record.SetRegionStatus("east", TenantRegionStatus.Online, Stamp(3), "seed");
        record.AuthorizeRegion("west", Stamp(4), "seed");
        record.SetRegionStatus("west", targetStatus, Stamp(5), "seed");
        registry.Seed(record);

        var timers = Substitute.For<ITimerRegistry>();
        var timer = Substitute.For<IGrainTimer>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(timer);
        var services = new ServiceCollection();
        services.AddSingleton(timers);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tenant-region-backfill", Acme.Value));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var state = Substitute.For<IPersistentState<TenantRegionBackfillCoordinatorState>>();
        state.State.Returns(persistedState ?? new TenantRegionBackfillCoordinatorState());
        var reminders = Substitute.For<IReminderRegistry>();
        var clusterOptions = Options.Create(new ClusterOptions { ClusterId = "west" });
        var service = new TenantRegionBackfillService(
            registry,
            new TenantRegionLifecycleDriver(registry, clusterOptions),
            Substitute.For<IGrainFactory>(),
            clusterOptions,
            NullLogger<TenantRegionBackfillService>.Instance);
        var grain = new TenantRegionBackfillCoordinatorGrain(
            context,
            reminders,
            NullLogger<TenantRegionBackfillCoordinatorGrain>.Instance,
            state,
            registry,
            service,
            clusterOptions);

        return new Harness(grain, state, reminders, timers, registry);
    }

    private sealed record Harness(
        TenantRegionBackfillCoordinatorGrain Grain,
        IPersistentState<TenantRegionBackfillCoordinatorState> State,
        IReminderRegistry Reminders,
        ITimerRegistry Timers,
        FakeTenantRegistry Registry);
}
