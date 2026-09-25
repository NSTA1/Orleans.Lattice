using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for issue #3558: a view created, or a maintainer started,
/// inside Orleans' asynchronous reminder-service startup window must not fail
/// view creation just because the keepalive-reminder registration in
/// <see cref="ViewMaintainerGrain.EnsureActiveAsync"/> raced the reminder service
/// into life. The registration now waits the transient "Reminder Service is still
/// initializing" condition out with <see cref="ReminderServiceReadiness"/>, the
/// same remedy #2086 (coordinator) and #2579 (tree deletion) applied. Any other
/// fault, and a transient that never clears within the retry budget, still
/// surface with their original shape.
/// <para>
/// The grain is driven directly (the construction pattern established by
/// <see cref="ViewMaintainerDecommissionTests"/>) down the ShipView-consumer
/// suppression path, which is the shortest route through
/// <see cref="ViewMaintainerGrain.EnsureActiveAsync"/> that runs to completion
/// with substituted collaborators. Reaching that path's cursor-pin release is the
/// observable proof that activation proceeded past the keepalive registration.
/// </para>
/// </summary>
[TestFixture]
public sealed class ViewMaintainerKeepaliveReadinessTests
{
    private const string ViewName = "orders-view";
    private const string KeepaliveReminderName = "view-maintainer-keepalive";

    // A system-prefixed source short-circuits the physical-id resolve, so the
    // suppression path needs no registry-read collaborator.
    private const string SourceTreeId = LatticeConstants.SystemTreePrefix + "orders";

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        IReminderRegistry Reminders,
        IWalCursorRegistry Cursors);

    private static Harness Create(IReadOnlyList<TimeSpan> backoff)
    {
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(ViewName).Returns(
            new ViewRegistration(ViewName, SourceTreeId, Substitute.For<ILatticeViewProjection>()));

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));

        // ShipView with both trees replicated and a different producer cluster:
        // this cluster is a consumer, so EnsureActiveAsync suppresses the
        // maintainer and returns once the source pin is released.
        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(ViewName).Returns(new LatticeViewOptions
        {
            ReplicationMode = LatticeViewReplicationMode.ShipView,
            ShipViewProducerClusterId = "producer",
        });
        var replication = Substitute.For<ILatticeReplicationContext>();
        replication.IsReplicationEnabled.Returns(true);
        replication.LocalReplicaId.Returns("consumer");
        replication.ResolveMergeMode(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var reminders = Substitute.For<IReminderRegistry>();
        var cursors = Substitute.For<IWalCursorRegistry>();

        var grain = new ViewMaintainerGrain(
            context,
            Substitute.For<IGrainFactory>(),
            reminders,
            NullLogger<ViewMaintainerGrain>.Instance,
            catalog,
            commitLogReader: null!,
            subscriber: null!,
            cursorRegistry: cursors,
            optionsResolver: null!,
            viewOptions: viewOptions,
            latticeOptions: null!,
            replicationContext: replication,
            saturationSignal: null,
            historyRowCodec: null!,
            new FakePersistentState<ViewCheckpointState>())
        {
            KeepaliveRegistrationBackoff = backoff,
        };

        return new Harness(grain, reminders, cursors);
    }

    // The exact shape Orleans' LocalReminderService raises (see the CI trace on
    // #3558): an OrleansException with an inner TimeoutException.
    private static OrleansException StillInitializing()
        => new(
            ReminderServiceReadiness.StillInitializingMarker + " and it is taking a long time. Please retry again later.",
            new TimeoutException("The operation has timed out."));

    private static void OnRegister(IReminderRegistry reminders, Func<int, Task<IGrainReminder>> behaviour)
    {
        var attempts = 0;
        reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ => behaviour(++attempts));
    }

    private static Task ReceivedPinReleases(IWalCursorRegistry cursors, int count)
        => cursors.Received(count).UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());

    [Test]
    public void KeepaliveRegistrationBackoff_defaults_to_the_shared_readiness_budget()
    {
        var fresh = new ViewMaintainerGrain(
            Substitute.For<IGrainContext>(), null!, null!, NullLogger<ViewMaintainerGrain>.Instance,
            null!, null!, null!, null!, null!, null!, null!, null!, null, null!,
            new FakePersistentState<ViewCheckpointState>());

        Assert.That(fresh.KeepaliveRegistrationBackoff,
            Is.SameAs(ReminderServiceReadiness.DefaultRegistrationBackoff),
            "Production must use the shared bounded budget, not a private copy.");
    }

    [Test]
    public async Task EnsureActiveAsync_registers_the_keepalive_reminder_exactly_once_when_the_service_is_ready()
    {
        var h = Create([TimeSpan.FromSeconds(30)]);
        OnRegister(h.Reminders, _ => Task.FromResult(Substitute.For<IGrainReminder>()));

        await h.Grain.EnsureActiveAsync();

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            GrainId.Create("viewmaintainer", ViewName),
            KeepaliveReminderName,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));
        await ReceivedPinReleases(h.Cursors, 1);
    }

    [Test]
    public async Task EnsureActiveAsync_waits_out_a_transient_reminder_service_initialisation()
    {
        // The regression: before the fix a single "still initializing" transient
        // propagated straight out of EnsureActiveAsync and failed view creation.
        var h = Create([TimeSpan.Zero, TimeSpan.Zero]);
        var attempts = 0;
        OnRegister(h.Reminders, attempt =>
        {
            attempts = attempt;
            return attempt == 1
                ? throw StillInitializing()
                : Task.FromResult(Substitute.For<IGrainReminder>());
        });

        await h.Grain.EnsureActiveAsync();

        Assert.That(attempts, Is.EqualTo(2),
            "The keepalive registration must be retried past the startup-window transient and land exactly once more.");
        await ReceivedPinReleases(h.Cursors, 1);
    }

    [Test]
    public async Task EnsureActiveAsync_rethrows_when_the_reminder_service_never_finishes_initialising()
    {
        // A persistent transient is not swallowed: once the budget is spent it
        // surfaces with its original shape so a genuinely stuck reminder service is
        // not mistaken for a healthy start.
        var h = Create([TimeSpan.Zero, TimeSpan.Zero]);
        var attempts = 0;
        OnRegister(h.Reminders, attempt =>
        {
            attempts = attempt;
            throw StillInitializing();
        });

        Assert.That(
            async () => await h.Grain.EnsureActiveAsync(),
            Throws.TypeOf<OrleansException>()
                .With.Message.Contains(ReminderServiceReadiness.StillInitializingMarker)
                .And.InnerException.TypeOf<TimeoutException>());
        Assert.That(attempts, Is.EqualTo(3), "Every attempt in the budget must be spent before giving up.");
        await ReceivedPinReleases(h.Cursors, 0);
    }

    [Test]
    public async Task EnsureActiveAsync_propagates_an_unrelated_registration_fault_immediately()
    {
        // Only the "still initializing" transient is waited out. A 30 s backoff
        // would time the test out if an unrelated fault consumed a retry slot.
        var h = Create([TimeSpan.FromSeconds(30)]);
        var attempts = 0;
        OnRegister(h.Reminders, attempt =>
        {
            attempts = attempt;
            throw new InvalidOperationException("reminder table unreachable");
        });

        Assert.That(
            async () => await h.Grain.EnsureActiveAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("reminder table unreachable"));
        Assert.That(attempts, Is.EqualTo(1));
        await ReceivedPinReleases(h.Cursors, 0);
    }

    [Test]
    public async Task EnsureActiveAsync_honours_cancellation_during_the_readiness_backoff()
    {
        // The caller's token is forwarded to the wait-out, so a cancelled activation
        // stops waiting instead of sitting out the whole budget.
        var h = Create([TimeSpan.FromSeconds(30)]);
        OnRegister(h.Reminders, _ => throw StillInitializing());
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Assert.That(
            async () => await h.Grain.EnsureActiveAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        await ReceivedPinReleases(h.Cursors, 0);
    }
}
