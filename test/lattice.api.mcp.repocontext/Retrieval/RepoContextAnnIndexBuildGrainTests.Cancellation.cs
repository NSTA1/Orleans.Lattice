using NSubstitute;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the build coordinator forwarding its phase tick's cancellation
/// token into the bounded build step (#3130 item 2).
/// <para>
/// <b>The defect.</b> The step was invoked as
/// <c>BuildStepAsync(..., CancellationToken.None)</c>, so nothing could abandon
/// it once started. A single step is budgeted in wall-clock and has been measured
/// at twenty-three minutes on a large corpus; because a grain activation is
/// single-threaded, that step held the activation's turn for its whole duration
/// and no deactivation, timer disposal, or silo stand-down could interrupt it.
/// The coordinator base class was already handed a token by the Orleans grain
/// timer and simply dropped it on the floor.
/// </para>
/// <para>
/// <b>Why the pair below is the test and neither half is.</b> "A cancelled tick
/// takes no step" is satisfied by a coordinator that takes no step under any
/// circumstances - a broken build passes it trivially. Its control, "a live tick
/// does take a step", is satisfied by the defective code, since
/// <see cref="CancellationToken.None"/> also never cancels. Only together do they
/// pin the behaviour to the token: restore <c>CancellationToken.None</c> at the
/// call site and the cancelled case goes red while the control stays green,
/// which is exactly the discrimination the fix is about.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainTests
{
    /// <summary>
    /// Recovers the real grain-timer callback the coordinator base class armed.
    /// Driving THIS is what makes the test about production behaviour: calling
    /// <c>ProcessNextPhaseAsync</c> directly - as the convergence fixtures above
    /// do, quite correctly, because they are about convergence - bypasses the
    /// publication of the tick's token entirely, so a test written that way would
    /// pass against the defect.
    /// </summary>
    private static Func<CancellationToken, Task> CapturedTick(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    private static int VectorsIndexed(Activation activation) =>
        activation.Registry.TryGetProgress(RepoId, Space, out var progress)
            ? progress.VectorsIndexed
            : 0;

    /// <summary>
    /// Drives live ticks until the build has banked at least one vector, so a
    /// later assertion about a step being taken or skipped is made from inside
    /// the ingest phase rather than from the training phases that precede it.
    /// Returns the count banked. A build that never reaches ingest fails here
    /// rather than silently making every later comparison a zero-to-zero one.
    /// </summary>
    private static async Task<int> PumpIntoIngestAsync(
        Activation activation, Func<CancellationToken, Task> tick, CancellationToken token)
    {
        for (var i = 0; i < MaxTicks; i++)
        {
            await tick(token);
            var indexed = VectorsIndexed(activation);
            if (indexed > 0)
            {
                return indexed;
            }
        }

        Assert.Fail("the build never reached the ingest phase, so nothing below would be measurable");
        return 0;
    }

    [Test]
    public async Task A_tick_driven_with_a_live_token_advances_the_build()
    {
        // THE CONTROL. Without it the cancelled case below is vacuous: a
        // coordinator that never steps at all would satisfy "took no step"
        // perfectly. This asserts the rig really does build when nothing is
        // asking it to stop, so the difference the next test measures can only
        // have come from the token.
        var durable = new Durable();
        durable.SeedRing(64);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);

        var tick = CapturedTick(process.Timers);

        using var live = new CancellationTokenSource();
        var indexed = await PumpIntoIngestAsync(process, tick, live.Token);
        var before = indexed;
        await tick(live.Token);

        Assert.That(VectorsIndexed(process), Is.GreaterThan(before),
            "a tick with a live token must do real work, or the cancelled case proves nothing");
    }

    [Test]
    public async Task A_tick_driven_with_an_already_cancelled_token_takes_no_build_step()
    {
        // THE HEADLINE. The token reaches the bounded step, so a build that has
        // been asked to stop stops instead of holding the activation's turn for
        // the rest of its wall-clock budget. Against the previous
        // CancellationToken.None this tick would advance exactly as the control
        // above does, and this assertion goes red.
        var durable = new Durable();
        durable.SeedRing(64);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);

        var tick = CapturedTick(process.Timers);

        using var live = new CancellationTokenSource();
        var before = await PumpIntoIngestAsync(process, tick, live.Token);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await tick(cancelled.Token);

        Assert.That(VectorsIndexed(process), Is.EqualTo(before),
            "a tick whose token is already cancelled must not start a build step");
    }

    [Test]
    public async Task A_cancelled_tick_does_not_surface_as_a_fault()
    {
        // A TEARDOWN IS NOT A FAILURE. The coordinator's filtered catch is what
        // keeps a clean stand-down from being counted and logged as a discarded
        // tick - which, after three of them, escalates to an error claiming the
        // phase machine has stopped advancing. Widen that filter to a blanket
        // OperationCanceledException and this still passes; remove the arm
        // entirely and the exception escapes here.
        var durable = new Durable();
        durable.SeedRing(64);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);

        var tick = CapturedTick(process.Timers);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        Assert.DoesNotThrowAsync(async () => await tick(cancelled.Token),
            "an orderly teardown must not throw out of the phase pump");
    }

    [Test]
    public async Task A_build_cancelled_mid_flight_resumes_on_the_next_live_tick()
    {
        // RESUMABILITY UNDER CANCELLATION. Abandoning a step is only safe if what
        // it had banked survives and the next tick picks up from there; a step
        // that cancelled by discarding its slice would make the cancellation a
        // correctness regression rather than a liveness fix. Nothing is banked
        // until the step returns, so a cancelled step costs at most the slice it
        // was in, never the build.
        var durable = new Durable();
        durable.SeedRing(64);

        using var process = durable.Start();
        await process.Grain.EnsureBuildingAsync(Space);

        var tick = CapturedTick(process.Timers);

        using var live = new CancellationTokenSource();
        var banked = await PumpIntoIngestAsync(process, tick, live.Token);
        Assert.That(banked, Is.GreaterThan(0),
            "the fixture must bank something first, or 'it survived' is unfalsifiable");

        using (var cancelled = new CancellationTokenSource())
        {
            await cancelled.CancelAsync();
            await tick(cancelled.Token);
        }

        Assert.That(VectorsIndexed(process), Is.EqualTo(banked),
            "a cancelled tick must not unwind work the build had already banked");

        await tick(live.Token);

        Assert.That(VectorsIndexed(process), Is.GreaterThan(banked),
            "the build must carry on from where it was, not restart or stall");
    }
}
