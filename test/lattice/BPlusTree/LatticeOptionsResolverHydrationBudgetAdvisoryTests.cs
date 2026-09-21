using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The leaf hydration residency budget bounds the peak footprint of a windowed
/// leaf walk only while <c>TrimToBudget</c> can actually evict a window behind
/// the walk, which it cannot when the budget is unbounded or is not materially
/// smaller than the leaf (issue #2836). The regime is legitimate, so nothing is
/// clamped, but it is also SILENT - every converted seam still returns the right
/// answer - so <see cref="LatticeOptionsResolver"/> emits a one-shot advisory
/// naming both configured values.
/// <para>
/// The decision is factored out as a pure predicate so the boundary cases can be
/// asserted directly rather than inferred from log output, and the emission
/// tests then only have to prove that the predicate is wired to a warning and
/// that the warning does not repeat per activation.
/// </para>
/// </summary>
[TestFixture]
public class LatticeOptionsResolverHydrationBudgetAdvisoryTests
{
    [SetUp]
    public void Setup() => LatticeOptionsResolver.ResetWarnedHydrationBudgetTreesForTests();

    [Test]
    public void The_shipped_defaults_do_not_trip_the_advisory()
    {
        // 1 MiB against 64 MiB: the budget is two orders of magnitude below the
        // split threshold, so windowing bounds what it claims to bound. If this
        // ever reddens, an option default moved and the advisory would fire for
        // every tree on a stock deployment.
        Assert.That(
            LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions()),
            Is.False);
    }

    [Test]
    public void An_unbounded_budget_trips_the_advisory()
    {
        // Zero means "never evict", which is the WORST case for residency and
        // not merely an unset value - a windowed walk over a leaf approaching
        // the split threshold goes wholly resident and stays that way.
        Assert.That(
            LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
            {
                LeafHydrationResidentBytes = 0L,
            }),
            Is.True);
    }

    [Test]
    public void A_budget_within_an_order_of_magnitude_of_the_split_threshold_trips_the_advisory()
    {
        Assert.That(
            LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
            {
                MaxLeafBytes = 64L * 1024 * 1024,
                LeafHydrationResidentBytes = 32L * 1024 * 1024,
            }),
            Is.True);
    }

    [Test]
    public void A_budget_just_below_a_tenth_of_the_split_threshold_does_not_trip_the_advisory()
    {
        // The boundary, asserted from both sides so the comparison cannot drift
        // into an always-true or always-false test.
        var threshold = 1_000_000L;
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
                {
                    MaxLeafBytes = threshold,
                    LeafHydrationResidentBytes = (threshold / 10) - 1,
                }),
                Is.False);
            Assert.That(
                LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
                {
                    MaxLeafBytes = threshold,
                    LeafHydrationResidentBytes = threshold / 10,
                }),
                Is.True);
        });
    }

    [Test]
    public void A_disarmed_byte_bound_does_not_trip_the_advisory()
    {
        // MaxLeafBytes of 0 restores pure key-count splitting, so there is no
        // byte threshold for a budget to fail to bound. Without this conjunct
        // the unbounded-budget arm would fire for every tree that opted out of
        // byte splitting, which is noise rather than an advisory.
        Assert.That(
            LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
            {
                MaxLeafBytes = 0L,
                LeafHydrationResidentBytes = 0L,
            }),
            Is.False);
    }

    [Test]
    public void Partial_hydration_off_does_not_trip_the_advisory()
    {
        // With partial hydration disabled there is no frame and no windowed
        // walk, so there is nothing for the budget to undermine.
        Assert.That(
            LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
            {
                LeafPartialHydrationEnabled = false,
                LeafHydrationResidentBytes = 0L,
            }),
            Is.False);
    }

    [Test]
    public void An_extreme_budget_does_not_overflow_the_comparison()
    {
        // Written as budget >= threshold / 10 rather than budget * 10 >=
        // threshold precisely so this cannot wrap negative and invert the test.
        Assert.That(
            LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(new LatticeOptions
            {
                MaxLeafBytes = 64L * 1024 * 1024,
                LeafHydrationResidentBytes = long.MaxValue,
            }),
            Is.True);
    }

    [Test]
    public void The_advisory_null_checks_its_argument()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeOptionsResolver.LeafHydrationBudgetUnderminesWindowing(null!));
    }

    [Test]
    public async Task The_advisory_is_emitted_once_per_tree()
    {
        var logger = BuildLogger();
        var resolver = BuildResolver(
            new LatticeOptions { LeafHydrationResidentBytes = 0L },
            logger);

        await resolver.ResolveAsync("hydration-advisory-tree");
        await resolver.ResolveAsync("hydration-advisory-tree");
        await resolver.ResolveAsync("hydration-advisory-tree");

        Assert.That(CountWarnings(logger), Is.EqualTo(1));
    }

    [Test]
    public async Task A_healthy_budget_emits_no_advisory()
    {
        // The negative control for the arm above: the same harness, the same
        // tree, a configuration the predicate rejects, and no warning. Without
        // it the emission arm could pass on a resolver that warned
        // unconditionally.
        var logger = BuildLogger();
        var resolver = BuildResolver(new LatticeOptions(), logger);

        await resolver.ResolveAsync("hydration-healthy-tree");

        Assert.That(CountWarnings(logger), Is.Zero);
    }

    private static ILogger<LatticeOptionsResolver> BuildLogger()
    {
        var logger = Substitute.For<ILogger<LatticeOptionsResolver>>();
        logger.IsEnabled(LogLevel.Warning).Returns(true);
        return logger;
    }

    private static int CountWarnings(ILogger<LatticeOptionsResolver> logger)
        => logger.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILogger.Log)
                && c.GetArguments()[0] is LogLevel level
                && level == LogLevel.Warning);

    private static LatticeOptionsResolver BuildResolver(
        LatticeOptions options,
        ILogger<LatticeOptionsResolver> logger)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
            }));

        return new LatticeOptionsResolver(factory, monitor, logger);
    }
}
