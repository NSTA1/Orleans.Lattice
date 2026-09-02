using System.Reflection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for <see cref="RepoContextSelfIndexGrain"/>'s durability and teardown
/// contract: the keep-alive reminder that re-activates the grain after a host
/// restart, the reminder beat that re-arms the scan timer, the teardown that stops a
/// removed repository scanning forever, and the defensive spoke gates.
/// <para>
/// Every collaborator here is a backstop rather than a critical path, so the
/// invariant under test is mostly negative: a reminder-service hiccup, a scheduling
/// failure, or an unregistration failure must degrade the backstop without failing
/// onboarding or killing the activation. These paths only run when their
/// collaborator throws, which is why they need a substituted registry to fault on
/// demand.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainLifecycleTests
{
    [Test]
    public async Task Onboarding_survives_a_reminder_registry_that_cannot_register_the_keepalive()
    {
        var harness = new SelfIndexGrainHarness();
        harness.Reminders
            .RegisterOrUpdateReminder(Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Throws(new InvalidOperationException("reminder service unavailable"));

        var progress = await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        await harness.Runner.Received(1).StartIndexAsync(Arg.Any<RepoIndexJobRequest>());
        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Running),
                "A reminder hiccup must not fail onboarding; only the restart-reactivation backstop degrades.");
            Assert.That(harness.TimerCallback, Is.Not.Null,
                "The timer still drives the scan for the life of this activation.");
        });
    }

    [Test]
    public async Task Onboarding_survives_an_approximate_index_scheduler_that_cannot_arm()
    {
        var harness = new SelfIndexGrainHarness
        {
            // A bound embedder takes the scheduler past its no-op guard so the arming
            // call is really attempted, and can therefore really fail.
            Embedder = Substitute.For<IEmbeddingProvider>(),
        };
        harness.Embedder!.Space.Returns(new EmbeddingSpace("test-model", 8, false));
        harness.GrainFactory
            .GetGrain<IRepoContextAnnIndexBuildGrain>(Arg.Any<string>())
            .Throws(new InvalidOperationException("build coordinator unavailable"));

        var progress = await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.Running),
                "Indexing must not fail because a derived index could not be scheduled.");
            Assert.That(harness.State.WriteCount, Is.EqualTo(1),
                "Onboarding still persists its reconcile deadline after the non-fatal scheduling failure.");
        });
    }

    [Test]
    public async Task Onboarding_arms_the_approximate_index_build_coordinator_when_one_can_be_scheduled()
    {
        var harness = new SelfIndexGrainHarness
        {
            Embedder = Substitute.For<IEmbeddingProvider>(),
        };
        harness.Embedder!.Space.Returns(new EmbeddingSpace("test-model", 8, false));
        var build = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        harness.GrainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(Arg.Any<string>()).Returns(build);

        await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        await build.Received(1).EnsureBuildingAsync(Arg.Any<EmbeddingSpaceTag>());
    }

    [Test]
    public async Task The_keepalive_reminder_beat_re_arms_the_scan_timer_after_a_reactivation()
    {
        var harness = new SelfIndexGrainHarness();
        var grain = harness.CreateGrain();

        // A grain reactivated by its keep-alive has no timer until the beat arrives:
        // ReceiveReminder is the only thing that re-arms it without a client call.
        Assert.That(harness.TimerCallback, Is.Null, "Nothing armed the timer before the reminder fired.");

        await grain.ReceiveReminder("repo-context-self-index-keepalive", new TickStatus());

        Assert.That(harness.TimerCallback, Is.Not.Null,
            "The keep-alive beat re-arms the scan timer, so the sweep resumes after a host restart.");
    }

    [Test]
    public async Task An_unrecognised_reminder_name_arms_nothing()
    {
        var harness = new SelfIndexGrainHarness();
        var grain = harness.CreateGrain();

        await grain.ReceiveReminder("some-other-reminder", new TickStatus());

        Assert.That(harness.TimerCallback, Is.Null,
            "Only this grain's own keep-alive re-arms the timer; a foreign reminder is ignored.");
    }

    [Test]
    public async Task Arming_the_timer_twice_reuses_the_first_registration()
    {
        var harness = new SelfIndexGrainHarness();
        var grain = harness.CreateGrain();

        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());
        await grain.ReceiveReminder("repo-context-self-index-keepalive", new TickStatus());

        harness.TimerRegistry.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
    }

    [Test]
    public async Task StopAsync_disposes_the_timer_unregisters_the_keepalive_and_clears_the_checkpoint()
    {
        var harness = new SelfIndexGrainHarness();
        var reminder = Substitute.For<IGrainReminder>();
        harness.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>()).Returns(Task.FromResult<IGrainReminder?>(reminder));

        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());
        harness.State.State.ResumeKey = "some/mid-scan/key";

        await grain.StopAsync();

        harness.Timer.Received(1).Dispose();
        await harness.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.ClearCount, Is.EqualTo(1), "A removed repository's checkpoint is cleared.");
            Assert.That(harness.State.State.ResumeKey, Is.Null,
                "The in-memory checkpoint is reset too, so a re-activation does not resume a dead scan.");
        });
    }

    [Test]
    public async Task StopAsync_with_no_registered_keepalive_unregisters_nothing()
    {
        var harness = new SelfIndexGrainHarness();
        harness.Reminders
            .GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await harness.CreateGrain().StopAsync();

        await harness.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        Assert.That(harness.State.ClearCount, Is.EqualTo(1),
            "Teardown still clears the checkpoint even when there was no reminder to remove.");
    }

    [Test]
    public async Task StopAsync_survives_a_reminder_registry_that_cannot_unregister()
    {
        var harness = new SelfIndexGrainHarness();
        harness.Reminders
            .GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Throws(new InvalidOperationException("reminder service unavailable"));

        var grain = harness.CreateGrain();

        Assert.That(
            async () => await grain.StopAsync(),
            Throws.Nothing,
            "A failed unregistration is logged and absorbed; teardown must still clear the durable state.");
        Assert.That(harness.State.ClearCount, Is.EqualTo(1));
    }

    [Test]
    public async Task A_spoke_never_arms_the_scan_timer_even_on_a_stray_keepalive_beat()
    {
        var harness = new SelfIndexGrainHarness(RepoContextIndexingRole.Spoke);
        var grain = harness.CreateGrain();

        await grain.ReceiveReminder("repo-context-self-index-keepalive", new TickStatus());

        Assert.That(harness.TimerCallback, Is.Null,
            "The defensive role gate keeps a spoke from ever running a reconcile, prune, or gap scan.");
    }

    [Test]
    public async Task A_spoke_tick_that_somehow_fires_mutates_no_index_state()
    {
        // The tick body carries a defensive role gate for the one case the timer
        // guard cannot cover: a timer armed while this cluster was a hub, still
        // firing after the cluster was demoted to a spoke. Production reaches it by
        // a role change outliving an activation; a test reproduces it by arming as a
        // hub and then flipping the same options instance the grain holds, which is
        // the only way to observe the guard without altering production code.
        var options = new RepoContextIndexingOptions { Role = RepoContextIndexingRole.Hub };
        var harness = new SelfIndexGrainHarness(options: options);
        harness.SeedFile("src/gap.cs");

        var grain = harness.CreateGrain();
        await grain.EnsureRunningAsync(SelfIndexGrainHarness.Request());
        Assert.That(harness.TimerCallback, Is.Not.Null, "The hub armed a timer that now outlives its role.");

        DemoteToSpoke(options);
        harness.Job.ClearReceivedCalls();
        var writesBefore = harness.State.WriteCount;

        await harness.TickAsync();

        await harness.Job.DidNotReceive().EnsureIndexedAsync();
        await harness.Job.DidNotReceive().GetProgressAsync();
        Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore),
            "A surviving timer on a demoted cluster reconciles, prunes, and writes nothing: "
            + "the 'a spoke mutates no index state' invariant holds even across a role change.");
    }

    /// <summary>
    /// Flips an existing options instance to the spoke role. <c>Role</c> is
    /// init-only, so a role change that outlives an activation can only be
    /// reproduced by writing its backing field directly.
    /// </summary>
    /// <param name="options">The options instance the grain under test already holds.</param>
    private static void DemoteToSpoke(RepoContextIndexingOptions options)
    {
        var field = typeof(RepoContextIndexingOptions).GetField(
            $"<{nameof(RepoContextIndexingOptions.Role)}>k__BackingField",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "The compiler-generated backing field for the init-only role must exist.");
        field!.SetValue(options, RepoContextIndexingRole.Spoke);
        Assert.That(options.IndexingEnabled, Is.False, "The options now report a spoke.");
    }

    [Test]
    public async Task A_spoke_returns_the_inert_snapshot_without_arming_a_timer()
    {
        var harness = new SelfIndexGrainHarness(RepoContextIndexingRole.Spoke);

        var progress = await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        Assert.Multiple(() =>
        {
            Assert.That(progress.Status, Is.EqualTo(RepoIndexStatus.None));
            Assert.That(progress.Phase, Is.EqualTo(RepoIndexPhase.Pending));
            Assert.That(progress.RepoId, Is.EqualTo(SelfIndexGrainHarness.RepoId));
            Assert.That(harness.TimerCallback, Is.Null, "A spoke arms no scan timer.");
        });
    }
}
