using Orleans.Lattice.Testing;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Issue #3284: the open-slice budget bounds only time in which the slice could
/// have made progress, and the options that govern it are configurable.
/// <para>
/// <b>The defect this replaces was regenerative, not merely ineffective.</b> The
/// budget was a bare wall-clock deadline, so it measured time the slice spent
/// queued for a WAL replay permit - time in which the key walk can bank nothing,
/// because it banks position per entry and an entry cannot be read before its
/// leaf activates. With a measured mean permit wait above the budget, every slice
/// was guaranteed to expire having banked zero; three such slices tripped the
/// empty-deferral escalation and the index could not open at all. Worse, each
/// expiry re-enqueued a waiter, so the budget lengthened the very queue that
/// caused it.
/// </para>
/// <para>
/// <b>Both halves are asserted here, and neither alone is the fix.</b> A slice
/// that banked nothing must be extended rather than cancelled, or the wedge
/// remains; and the extensions must be capped, or the open is unbounded again -
/// which is the thirty-minute coordinator wedge issue #3130 removed, and would
/// also make the escalation for genuinely dead storage unreachable.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    /// <summary>
    /// A slice that has banked nothing survives its first budget boundary.
    /// </summary>
    /// <remarks>
    /// <b>This is the assertion the previous implementation fails.</b> A bare
    /// <c>CancellationTokenSource(budget, clock)</c> cancels at the first
    /// boundary unconditionally, so the walk token would already read cancelled
    /// here. Read off the token the parked scan was handed rather than off the
    /// open's completion, because the manual clock fires its callbacks inside
    /// <c>Advance</c> - so this is a deterministic statement about what that tick
    /// did, where "the task has not completed" would merely be a statement about
    /// how fast this machine is.
    /// </remarks>
    [Test]
    public async Task A_slice_that_has_banked_nothing_is_extended_past_its_first_boundary()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, load: null, BudgetedOptions(clock, OpenBudget, maxExtensions: 3));

        // Parks before serving a single mapping, which is what queueing for a
        // replay permit looks like from here: the slice is alive, the store is
        // reachable, and nothing whatsoever has been banked.
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), 0);

        var advancing = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();

        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        Assert.That(
            store.CapturedScanToken.IsCancellationRequested,
            Is.False,
            "A BUDGET MUST NOT BOUND TIME IN WHICH PROGRESS IS IMPOSSIBLE. The slice banked nothing, "
            + "so cancelling it here buys back a turn at the cost of a retry that queues afresh - and "
            + "under the permit saturation that produced this issue, every retry does the same, for "
            + "ever, while lengthening the queue that caused it.");

        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        Assert.That(
            store.CapturedScanToken.IsCancellationRequested,
            Is.False,
            "Asserted at a second boundary too: an implementation that merely doubled the budget would "
            + "satisfy the reading above and be the same defect one factor out.");

        // Two extensions used, one left, so the third boundary is the cap.
        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        Assert.That(
            store.CapturedScanToken.IsCancellationRequested,
            Is.True,
            "THE EXTENSIONS MUST BE CAPPED. Without a cap this is an unbounded open inside a "
            + "non-reentrant coordinator turn, which is the thirty-minute wedge issue #3130 removed, "
            + "and the escalation that catches genuinely dead storage becomes unreachable.");

        await advancing;
        Assert.That(
            handle.IsServing,
            Is.False,
            "The slice ended on its cap having banked nothing, so it is a deferral and the handle "
            + "holds no index to serve from.");
    }

    /// <summary>
    /// A slice that HAS banked progress still fires at its first boundary, so the
    /// coordinator's turn is returned on the ordinary cadence.
    /// </summary>
    /// <remarks>
    /// The complement of the test above, and the reason the deadline is armed on
    /// progress rather than simply lengthened. A productive slice must not be
    /// granted any extension at all: the budget exists to return a non-reentrant
    /// turn, and a plane that banks steadily would otherwise hold that turn for
    /// seven budgets instead of one.
    /// </remarks>
    [Test]
    public async Task A_slice_that_has_banked_progress_is_not_extended()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, reporter, BudgetedOptions(clock, OpenBudget));

        // Serves part of the key map and then parks, so the slice has banked
        // something by the time the first boundary arrives.
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), FaultAfter);

        var advancing = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();

        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        Assert.That(
            store.CapturedScanToken.IsCancellationRequested,
            Is.True,
            "A PRODUCTIVE SLICE MUST STILL YIELD ON TIME. The extension exists for slices that could "
            + "bank nothing; granting it to one that banked something would hold the coordinator's "
            + "non-reentrant turn for every extension as well, which is the cost the budget exists to "
            + "avoid.");

        await advancing;
        Assert.That(reporter.Snapshot().Deferred, Is.EqualTo(1));
    }

    /// <summary>
    /// A zero extension count reproduces the historical elapsed-only bound
    /// exactly, so the change can be turned off in the field.
    /// </summary>
    [Test]
    public async Task A_zero_extension_count_restores_the_elapsed_only_bound()
    {
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);

        using var handle = NewHandle(
            SeededSource(), store, prefix, load: null, BudgetedOptions(clock, OpenBudget, maxExtensions: 0));
        store.BlockAfter(VectorIndexStorageKeys.KeyMapPrefix(prefix), 0);

        var advancing = handle.AdvanceAsync(Ct);
        await store.BlockedAsync();

        clock.Advance(OpenBudget + TimeSpan.FromMilliseconds(1));
        Assert.That(
            store.CapturedScanToken.IsCancellationRequested,
            Is.True,
            "At zero extensions an unproductive slice must be cancelled at the first boundary, which "
            + "is what the bound did before issue #3284.");

        await advancing;
    }

    /// <summary>
    /// The options resolve from the environment, so a budget that wedges a plane
    /// can be moved without redeploying the library.
    /// </summary>
    /// <remarks>
    /// <b>This is issue #3284's third item, and its absence was the reason the
    /// other two could not be worked around in the field.</b> The container
    /// registration was a bare <c>TryAddSingleton&lt;RepoContextAnnOptions&gt;()</c>,
    /// which binds the parameterless constructor, so every value was a
    /// compile-time constant. Asserted through <see cref="RepoContextAnnOptions.FromEnvironment"/>
    /// itself rather than through the container, because it is the resolution -
    /// not the registration - that had no seam at all.
    /// </remarks>
    [Test]
    public void The_open_slice_bounds_resolve_from_the_environment()
    {
        var defaults = new RepoContextAnnOptions();

        using (new EnvironmentVariableScope(RepoContextAnnOptions.OpenSliceBudgetSecondsVariable, "37"))
        using (new EnvironmentVariableScope(RepoContextAnnOptions.MaxOpenSliceExtensionsVariable, "11"))
        {
            var resolved = RepoContextAnnOptions.FromEnvironment();

            Assert.Multiple(() =>
            {
                Assert.That(resolved.OpenSliceBudget, Is.EqualTo(TimeSpan.FromSeconds(37)));
                Assert.That(resolved.MaxOpenSliceExtensions, Is.EqualTo(11));
                Assert.That(
                    resolved.OpenSliceBudget,
                    Is.Not.EqualTo(defaults.OpenSliceBudget),
                    "The supplied value has to differ from the default, or this fixture would pass "
                    + "against a resolver that ignored the environment entirely.");
            });
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextAnnOptions.FromEnvironment().OpenSliceBudget,
                Is.EqualTo(defaults.OpenSliceBudget),
                "An absent variable must fall back to the default.");
            Assert.That(
                RepoContextAnnOptions.FromEnvironment().MaxOpenSliceExtensions,
                Is.EqualTo(defaults.MaxOpenSliceExtensions));
        });
    }

    /// <summary>
    /// A malformed or negative value falls back to the default rather than
    /// removing the bound.
    /// </summary>
    /// <remarks>
    /// Fail-safe rather than fail-open, and the direction matters: a budget of
    /// zero <i>removes</i> the bound, so a resolver that parsed a typo to zero
    /// would silently restore the unbounded open a deployment thought it had left
    /// behind, with the supplied value looking deliberate in the container spec.
    /// </remarks>
    [TestCase("not-a-number")]
    [TestCase("-1")]
    [TestCase("   ")]
    public void A_malformed_open_slice_budget_falls_back_to_the_default(string raw)
    {
        var defaults = new RepoContextAnnOptions();

        using var budget = new EnvironmentVariableScope(
            RepoContextAnnOptions.OpenSliceBudgetSecondsVariable, raw);
        using var extensions = new EnvironmentVariableScope(
            RepoContextAnnOptions.MaxOpenSliceExtensionsVariable, raw);

        var resolved = RepoContextAnnOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(resolved.OpenSliceBudget, Is.EqualTo(defaults.OpenSliceBudget));
            Assert.That(resolved.MaxOpenSliceExtensions, Is.EqualTo(defaults.MaxOpenSliceExtensions));
        });
    }

    /// <summary>
    /// The new variables are published by the package registry, so the startup
    /// effective-configuration report can say they were read.
    /// </summary>
    /// <remarks>
    /// Issue #2460 established that a variable this package resolves but does not
    /// publish is reported to the operator as <c>[SUPPLIED BUT NOT READ BY THIS
    /// HOST]</c> - the exact inversion of the truth. A configuration surface added
    /// without registering its keys would recreate that defect for the one setting
    /// an operator reaches for during the incident these variables exist for.
    /// </remarks>
    [Test]
    public void The_open_slice_variables_are_published_and_reported()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEnvironmentVariables.All,
                Does.Contain(RepoContextAnnOptions.OpenSliceBudgetSecondsVariable));
            Assert.That(
                RepoContextEnvironmentVariables.All,
                Does.Contain(RepoContextAnnOptions.MaxOpenSliceExtensionsVariable));
        });

        using var scope = new EnvironmentVariableScope(
            RepoContextAnnOptions.OpenSliceBudgetSecondsVariable, "37");

        var snapshot = RepoContextEnvironmentVariables.DescribeResolvedSettings()
            .Single(s => s.Name == RepoContextAnnOptions.OpenSliceBudgetSecondsVariable);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.WasDeclared, Is.True);
            Assert.That(
                snapshot.Resolved,
                Does.Contain("37"),
                "The report states the PARSED value, so a supplied setting that was honoured has to "
                + "read back as the value in force rather than as the default beside it.");
        });
    }

    /// <summary>
    /// Sets an environment variable for the life of the scope and restores exactly
    /// what was there before, including its absence.
    /// </summary>
    private sealed class EnvironmentVariableScope : IDisposable
    {
        private readonly string _name;
        private readonly string? _previous;

        public EnvironmentVariableScope(string name, string? value)
        {
            _name = name;
            _previous = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);
        }

        public void Dispose() => Environment.SetEnvironmentVariable(_name, _previous);
    }
}
