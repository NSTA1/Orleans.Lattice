using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The integration gate for the bounded-cold-start mechanisms: every one of
/// them must be <b>armed on a silo that configures no options at all</b>.
/// <para>
/// This fixture exists because the epic's premise is a drop-in improvement that
/// heals deployments nobody reconfigures. An opt-in healing mechanism heals
/// nothing, and the repository already shipped one proof of that failure mode -
/// <see cref="LatticeOptions.LeafCachePreWarmCount"/> defaulted to <c>0</c>, so
/// the pre-warm path was dead on arrival for the very deployment it was built
/// for. Each assertion below therefore drives the mechanism's own decision seam
/// rather than merely reading back a number, so a default that is present but
/// no longer reaches its mechanism still fails.
/// </para>
/// <para>
/// The one mechanism with no switch is the recovering replay flush ceiling: it
/// is a defect repair in the leaf's deferred-offset ledger, unconditional by
/// design and with nothing to configure.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeMechanismDefaultsTests
{
    private const string Tree = "unconfigured-tree";

    /// <summary>
    /// Resolves the options a grain on a silo with <b>no</b> <c>ConfigureLattice</c>
    /// call would read: the named-options pipeline with nothing registered
    /// against it, which is exactly the shape the repository-context container
    /// leaves the core library in.
    /// </summary>
    private static LatticeOptions UnconfiguredSiloOptions()
    {
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddSingleton<IValidateOptions<LatticeOptions>, LatticeOptionsValidator>();
        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IOptionsMonitor<LatticeOptions>>().Get(Tree);
    }

    // ------------------------------------------------------------ every switch

    [Test]
    public void Responsive_wal_gc_is_armed_on_an_unconfigured_silo()
    {
        var options = UnconfiguredSiloOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.WalGcInterval, Is.GreaterThan(TimeSpan.Zero),
                "a non-positive interval disables the scheduler outright");
            Assert.That(options.WalGcStartupDelay, Is.GreaterThan(TimeSpan.Zero),
                "the stagger keeps the first pass out of the activation storm and de-correlates silos");
            Assert.That(options.WalGcStartupDelay, Is.LessThanOrEqualTo(options.WalGcInterval),
                "a host must never wait longer for its first pass than its own ceiling");
            Assert.That(options.WalGcMinInterval, Is.GreaterThan(TimeSpan.Zero),
                "a non-positive floor collapses the adaptive band to the historical fixed tick");
            Assert.That(options.WalGcMinInterval, Is.LessThan(options.WalGcInterval),
                "a floor at or above the ceiling collapses the band, leaving no adaptivity to arm");
        });
    }

    [Test]
    public void The_binary_leaf_snapshot_codec_is_armed_on_an_unconfigured_silo()
        => Assert.That(UnconfiguredSiloOptions().LeafSnapshotBinaryEncodingEnabled, Is.True);

    [Test]
    public void Shape_aware_split_admission_is_armed_on_an_unconfigured_silo()
    {
        var policy = ShardSplitAdmissionPolicy.FromOptions(UnconfiguredSiloOptions());

        Assert.Multiple(() =>
        {
            Assert.That(ShardSplitAdmissionCore.IsSplitSkew(1.0d, policy.MinSkewRatio), Is.False,
                "a uniformly loaded tree - the bulk-ingest shape - must not admit a split");
            Assert.That(ShardSplitAdmissionCore.IsSplitSkew(1.6d, policy.MinSkewRatio), Is.True,
                "a genuinely skewed tree must still get its hot-shard relief");
            Assert.That(ShardSplitAdmissionCore.HasSplitHeadroom(policy.MaxPhysicalShards, policy.MaxPhysicalShards),
                Is.False, "the physical-shard ceiling must be armed, not open-ended");
            Assert.That(policy.MinShardEntries, Is.GreaterThan(0),
                "the occupancy floor must be armed so an empty shard is never split");
        });
    }

    [Test]
    public void Bounded_leaf_hydration_is_armed_on_an_unconfigured_silo()
    {
        var options = UnconfiguredSiloOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.LeafPartialHydrationEnabled, Is.True);
            Assert.That(options.LeafHydrationResidentBytes, Is.GreaterThan(0L),
                "zero means unbounded residency, so nothing is ever evicted");
        });
    }

    [Test]
    public void Automatic_over_split_healing_is_armed_on_an_unconfigured_silo()
    {
        var options = UnconfiguredSiloOptions();
        var policy = ShardHealingPolicy.FromOptions(options);

        // An over-split tree must clear every structural gate, so the sweep goes
        // on to measure load rather than refusing on configuration alone.
        var structural = ShardHealingDecisionCore.DecideStructural(
            physicalShardCount: 12,
            baseShardCount: 4,
            policy);

        Assert.Multiple(() =>
        {
            Assert.That(structural, Is.Null,
                "an over-split tree must not be refused by a structural gate on the shipped defaults");
            Assert.That(options.ShardHealingInterval, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(policy.MaxConcurrentConsolidations, Is.GreaterThan(0),
                "a zero cap admits nothing, which pauses healing without disabling it");
        });
    }

    [Test]
    public void Leaf_cache_pre_warm_is_armed_on_an_unconfigured_silo()
    {
        var settings = TestOptionsResolver
            .Create(baseOptions: UnconfiguredSiloOptions())
            .GetLeafAccessTrackingSettings(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(settings.IsEnabled, Is.True,
                "pre-warm at zero is the epic's own worked example of a capability nobody switches on");
            Assert.That(settings.PreWarmCount, Is.EqualTo(LatticeOptions.DefaultLeafCachePreWarmCount));
            Assert.That(settings.PreWarmCount, Is.LessThanOrEqualTo(LatticeOptions.MaxLeafCachePreWarmCount));
        });
    }

    [Test]
    public void The_shipped_defaults_pass_options_validation()
    {
        var result = new LatticeOptionsValidator().Validate(Tree, new LatticeOptions());

        Assert.That(result.Succeeded, Is.True, result.FailureMessage);
    }

    [Test]
    public async Task Every_mechanism_default_survives_per_tree_option_resolution()
    {
        // Grains read ResolvedLatticeOptions, which LatticeOptionsResolver builds
        // with an explicit per-field initialiser. A default that is correct on
        // LatticeOptions but dropped in that projection is invisible to every
        // grain and looks exactly like the grain ignoring configuration.
        var resolved = await TestOptionsResolver
            .Create(baseOptions: UnconfiguredSiloOptions())
            .ResolveAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.WalGcStartupDelay, Is.EqualTo(LatticeOptions.DefaultWalGcStartupDelay));
            Assert.That(resolved.WalGcMinInterval, Is.EqualTo(LatticeOptions.DefaultWalGcMinInterval));
            Assert.That(resolved.LeafSnapshotBinaryEncodingEnabled, Is.True);
            Assert.That(resolved.LeafPartialHydrationEnabled, Is.True);
            Assert.That(resolved.LeafHydrationResidentBytes, Is.EqualTo(LatticeOptions.DefaultLeafHydrationResidentBytes));
            Assert.That(resolved.HotShardMinSkewRatio, Is.EqualTo(LatticeOptions.DefaultHotShardMinSkewRatio));
            Assert.That(resolved.MaxPhysicalShardsPerTree, Is.EqualTo(LatticeOptions.DefaultMaxPhysicalShardsPerTree));
            Assert.That(resolved.ShardHealingEnabled, Is.True);
            Assert.That(resolved.LeafCachePreWarmCount, Is.EqualTo(LatticeOptions.DefaultLeafCachePreWarmCount));
        });
    }

    // ------------------------------------------- cross-mechanism safety at the
    // ------------------------------------------- shipped defaults

    [Test]
    public void The_split_and_heal_triggers_are_disjoint_at_the_shipped_defaults()
    {
        var options = new LatticeOptions();

        Assert.That(
            ShardSplitAdmissionCore.AreTriggerRegionsDisjoint(
                options.HotShardConsolidationSkewRatio,
                options.HotShardMinSkewRatio),
            Is.True,
            "overlapping trigger regions would let one tree be split and immediately folded forever");
    }

    [Test]
    public void The_split_and_heal_loops_are_separated_in_the_load_domain_too()
    {
        var options = new LatticeOptions();

        // Backpressure at or below the split threshold means healing has already
        // yielded at any load where a split is even conceivable. Raising
        // backpressure above the split threshold would open a load band in which
        // both loops are simultaneously live.
        Assert.That(
            options.ShardHealingBackpressureOpsPerSecond,
            Is.LessThanOrEqualTo(options.HotShardOpsPerSecondThreshold),
            "healing must yield before the load at which the splitter can act");
    }

    [Test]
    public void Healing_stays_armed_when_the_splitter_is_switched_off()
    {
        // Disabling the splitter on an already-shattered deployment is exactly
        // the configuration that most needs healing, so the two must not be
        // coupled - by default or by an operator's single flag.
        var options = new LatticeOptions { AutoSplitEnabled = false };
        var policy = ShardHealingPolicy.FromOptions(options);

        Assert.Multiple(() =>
        {
            Assert.That(policy.Enabled, Is.True);
            Assert.That(
                ShardHealingDecisionCore.DecideStructural(
                    physicalShardCount: 12, baseShardCount: 4, policy),
                Is.Null);
        });
    }
}
