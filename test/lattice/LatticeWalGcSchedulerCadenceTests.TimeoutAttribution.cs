using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the attribution carried on the scheduler's two abandonment log
/// lines: which timeout the line is actually reporting.
/// <para>
/// Both bounded awaits are wrapped by <c>Task.WaitAsync</c>, which throws
/// <see cref="TimeoutException"/> when the budget expires and propagates a
/// <see cref="TimeoutException"/> raised inside the operation unchanged. An
/// Orleans response timeout is a <see cref="TimeoutException"/>. The two
/// therefore arrive at the same catch, with the same type, carrying nothing to
/// separate them - while the log line asserted the first of them outright.
/// </para>
/// <para>
/// That is not a hypothetical. A pre-registered diagnostic whose decision rule
/// turned on the elapsed read <c>00:01:00</c> off three of these lines and
/// recorded "our bound fired"; the true elapsed was about thirty seconds and the
/// result was withdrawn. The line reads identically whether the operation took a
/// minute or three seconds, because the value printed was the declared constant.
/// </para>
/// <para>
/// So these fixtures assert the pairing rather than the prose: the measured
/// elapsed is present alongside the budget, and the attribution agrees with the
/// relationship between them. Asserting the relationship rather than either
/// number keeps the fixtures independent of the budget constants, which are
/// private and may be retuned without touching the property under test.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>The attribution naming the scheduler's own budget.</summary>
    private const string ThisBound = LatticeWalGcScheduler.TimeoutSourceThisBound;

    /// <summary>The attribution naming a timeout raised inside the awaited operation.</summary>
    private const string InsideOperation = LatticeWalGcScheduler.TimeoutSourceInsideOperation;

    /// <summary>
    /// The abandonment warning for the registry enumeration, identified by a
    /// fragment that survives prose edits but not the removal of the measured
    /// elapsed this fixture exists to protect.
    /// </summary>
    private static RecordedLogEntry EnumerationAbandonment(RecordingLoggerFactory logs) =>
        SoleWarningContaining(logs, "abandoned the registry enumeration");

    /// <summary>
    /// The abandonment warning for a single tree's collect.
    /// <para>
    /// Deliberately located on a fragment that the superseded constant-only form
    /// also carried. A locator matching only the new wording would make this
    /// fixture fail, on a regression, with "no such line" - which reads as a
    /// renamed message. Matching both makes it fail on the absent elapsed, which
    /// is the property actually lost.
    /// </para>
    /// </summary>
    private static RecordedLogEntry CollectAbandonment(RecordingLoggerFactory logs) =>
        SoleWarningContaining(logs, "WAL GC pass for tree");

    /// <summary>
    /// The one warning carrying <paramref name="fragment"/>, asserting that it is
    /// unique.
    /// <para>
    /// Uniqueness is asserted rather than assumed because the failure this whole
    /// fixture guards against is a reader drawing a conclusion from a line that
    /// did not say what they thought. A first-match helper would let a second,
    /// differently-attributed abandonment sit unexamined behind the one asserted
    /// on - reintroducing, inside the test, the exact ambiguity the change
    /// removes from the log.
    /// </para>
    /// </summary>
    private static RecordedLogEntry SoleWarningContaining(RecordingLoggerFactory logs, string fragment)
    {
        var matching = logs.Warnings
            .Where(e => e.Message.Contains(fragment, StringComparison.Ordinal))
            .ToArray();

        Assert.That(matching, Has.Length.EqualTo(1),
            $"expected exactly one warning containing '{fragment}'.");

        return matching[0];
    }

    /// <summary>
    /// Asserts that the entry carries a measured elapsed and a budget, and that
    /// its attribution agrees with the relationship between them.
    /// </summary>
    /// <param name="entry">The abandonment warning.</param>
    /// <param name="expected">The attribution the entry must carry.</param>
    private static void AssertAttribution(RecordedLogEntry entry, string expected)
    {
        var elapsed = entry.Value("Elapsed");
        var budget = entry.Value("Budget");

        Assert.Multiple(() =>
        {
            // The presence of the measurement at all is half the property. A
            // line carrying only the constant is the defect, and it passes any
            // assertion made solely on the attribution string.
            Assert.That(elapsed, Is.InstanceOf<TimeSpan>(),
                "the line must carry the MEASURED elapsed, not only the declared budget - a reader "
                + "cannot tell a bound that fired from one that did not without it.");
            Assert.That(budget, Is.InstanceOf<TimeSpan>(),
                "and the budget it is measured against, or the elapsed has nothing to be read relative to.");
            Assert.That(entry.Value("TimeoutSource"), Is.EqualTo(expected));
        });

        // The attribution must follow from the numbers on the same line rather
        // than merely appearing beside them, or the line becomes self-consistent
        // prose that can still be wrong.
        var measured = (TimeSpan)elapsed!;
        var against = (TimeSpan)budget!;

        if (string.Equals(expected, ThisBound, StringComparison.Ordinal))
        {
            Assert.That(measured, Is.GreaterThanOrEqualTo(against),
                "an attribution naming this bound must be backed by an elapsed that reached the budget.");
        }
        else
        {
            Assert.That(measured, Is.LessThan(against),
                "an attribution naming the operation must be backed by an elapsed short of the budget - "
                + "that shortfall is the whole proof, because WaitAsync cannot raise its own timeout early.");
        }
    }

    // ------------------------------------------------------- the enumeration

    [Test]
    public async Task An_enumeration_abandoned_by_its_own_budget_is_attributed_to_this_bound()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var never = new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var (scheduler, logs) = SchedulerWithLogs(FactoryAwaiting(never.Task, called), IdleGc(), time);
        await StartArmedAsync(scheduler, time);

        var budgetArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(called.Task);
        await Parked(budgetArmed);

        var cadenceArmed = time.NextTimerAsync();
        time.Advance(TimeSpan.FromMinutes(1));
        await Parked(cadenceArmed);

        AssertAttribution(EnumerationAbandonment(logs), ThisBound);

        never.TrySetResult([]);
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task An_enumeration_timing_out_inside_the_registry_call_is_not_attributed_to_this_bound()
    {
        // The fixture the withdrawn result needed and did not have. The registry
        // raises the TimeoutException itself, promptly - the shape of an Orleans
        // response timeout at the default thirty seconds, well inside a one
        // minute budget - and the clock is deliberately NOT advanced, so the
        // elapsed cannot be confused with a budget that expired.
        //
        // Before this change the line here was byte-identical to the one above,
        // which is precisely how a thirty-second upstream timeout was read as a
        // sixty-second bound firing.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync()
            .Returns<Task<IReadOnlyList<string>>>(_ => throw new TimeoutException("response did not arrive"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var (scheduler, logs) = SchedulerWithLogs(factory, IdleGc(), time);
        await StartArmedAsync(scheduler, time);

        var cadenceArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(cadenceArmed);

        AssertAttribution(EnumerationAbandonment(logs), InsideOperation);

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ------------------------------------------------------------ the collect

    [Test]
    public async Task A_collect_abandoned_by_its_own_budget_is_attributed_to_this_bound()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        const string Stuck = "walgc-attribution-stuck";
        var time = new VirtualTimeProvider();
        var never = new TaskCompletionSource<LatticeWalGcReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var (scheduler, logs) = SchedulerWithLogs(
            FactoryWithTrees(Stuck), GcParkedOn(never.Task, Stuck, called), time);
        await StartArmedAsync(scheduler, time);

        var budgetArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(called.Task);
        await Parked(budgetArmed);

        var cadenceArmed = time.NextTimerAsync();
        time.Advance(TimeSpan.FromMinutes(10));
        await Parked(cadenceArmed);

        AssertAttribution(CollectAbandonment(logs), ThisBound);

        never.TrySetResult(Report(entriesTrimmed: 0));
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task A_collect_timing_out_inside_the_collector_is_not_attributed_to_this_bound()
    {
        // The collect's half of the same discrimination. This one matters
        // independently of the enumeration's: the collect budget is ten minutes
        // against an Orleans default of thirty seconds, so an upstream timeout
        // here is misread by a factor of twenty - a reader concludes a tree hung
        // for ten minutes when the call in fact failed almost immediately, and
        // sizes the budget against a number that was never measured.
        WalGcSchedulerPhaseCensus.ResetForTests();
        const string Tree = "walgc-attribution-upstream";
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<LatticeWalGcReport>>(_ => throw new TimeoutException("response did not arrive"));

        var (scheduler, logs) = SchedulerWithLogs(FactoryWithTrees(Tree), gc, time);
        await StartArmedAsync(scheduler, time);

        var cadenceArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(cadenceArmed);

        AssertAttribution(CollectAbandonment(logs), InsideOperation);

        await scheduler.StopAsync(CancellationToken.None);
    }
}
