using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Verifies the unsatisfiable-ceiling verdict a WAL garbage-collection pass
/// now carries on its report (issue #3242).
/// <para>
/// The condition is arithmetic, not behavioural: a log-structured WAL provider
/// reclaims dead bytes only by rewriting a segment, and rewrites once dead
/// bytes reach a configured fraction <c>t</c> of total payload, so designed
/// steady-state occupancy peaks at <c>live / (1 - t)</c> - twice the live set
/// at the file provider's default 0.5 ratio. A
/// <see cref="LatticeOptions.WalMaxRetainedBytes"/> below that multiple is
/// therefore breached by a tree doing nothing wrong, and no pass can ever
/// bring it back inside.
/// </para>
/// <para>
/// Before this verdict existed such a tree reported <c>over_ceiling</c> or
/// <c>stranded</c> on every pass - both of which read as "a consumer is
/// lagging" and are answered by chasing the consumer, which is wasted effort
/// here. These fixtures pin the boundary in both directions, because an
/// assertion that only ever fires one way cannot distinguish a working
/// predicate from a hard-coded <c>true</c>.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcCeilingSatisfiabilityTests
{
    private const string Tree = "ceiling-satisfiability-tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(IWalStorageProvider provider)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        return sc.BuildServiceProvider();
    }

    private static async IAsyncEnumerable<WalEntry> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static WalEntry Entry(long offset, string key, byte[] value, HybridLogicalClock ts) => new()    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = ts,
        },
    };

    /// <summary>
    /// A single-partition tree holding three 1 KiB payloads and no consumer
    /// cursor, so nothing is trim-eligible and the retained figure is stable
    /// across passes. The returned figure is read from the provider itself
    /// rather than assumed, so the boundary cases below calibrate against the
    /// same quantity the pass measures instead of a hand-computed guess that
    /// could drift with the entry-framing rules.
    /// </summary>
    private static async Task<(InMemoryWalStorageProvider Provider, long Logical)> SeededTreeAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(Tree, 0,
        [
            Entry(0, "a", new byte[1024], Hlc(10)),
            Entry(1, "b", new byte[1024], Hlc(20)),
            Entry(2, "c", new byte[1024], Hlc(30)),
        ], CancellationToken.None);

        var logical = await provider.GetRetainedByteSizeAsync(Tree, 0, CancellationToken.None);
        Assert.That(logical, Is.GreaterThan(0),
            "the fixture is worthless unless the provider actually accounts logical bytes.");
        return (provider, logical);
    }

    private static LatticeWalGc Gc(IWalStorageProvider provider, long? ceiling) =>
        new(Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = ceiling }));

    // --------------------------------------------------- the predicate fires

    [Test]
    public async Task A_ceiling_one_byte_below_twice_the_working_set_is_reported_unsatisfiable()
    {
        var (provider, logical) = await SeededTreeAsync();

        // One byte inside the floor. Deliberately the tightest possible
        // failing case: a predicate that merely tested "ceiling < retained"
        // (the pre-existing over-ceiling condition) would pass this tree as
        // satisfiable, so the single byte is what separates the new rule from
        // the one that already existed.
        var report = await Gc(provider, (logical * 2) - 1).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CeilingUnsatisfiable, Is.True,
                "a ceiling below 2x the live set cannot be met by any pass, and must say so.");
            Assert.That(report.LogicalRetainedBytes, Is.EqualTo(logical),
                "and the verdict must be derived from the logical working set the provider reports.");
        });
    }

    [Test]
    public async Task The_measured_shortfall_from_issue_3242_is_reported_unsatisfiable()
    {
        // Calibrated directly against the issue's evidence package rather than
        // against a round number: repo-context-vector-index measured 2,226 MB
        // logical retained against a 4,096 MB ceiling, a 355 MB shortfall
        // below the 4,451 MB floor. Scaled down by 1 MB per byte so the
        // fixture holds the ratio the estate actually exhibited.
        const long LogicalMb = 2226;
        const long CeilingMb = 4096;

        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(Tree, 0,
            [Entry(0, "a", new byte[LogicalMb], Hlc(10))], CancellationToken.None);

        var logical = await provider.GetRetainedByteSizeAsync(Tree, 0, CancellationToken.None);
        var report = await Gc(provider, CeilingMb * logical / LogicalMb).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CeilingUnsatisfiable, Is.True,
                "the configuration the issue measured must be named by the signal the issue asked for.");
            Assert.That(report.ByteCeiling, Is.GreaterThan(report.LogicalRetainedBytes!.Value),
                "and it must fire while the ceiling is still comfortably above the live set - which is "
                + "exactly why the pre-existing over-ceiling condition never named it.");
        });
    }

    // ----------------------------------------------- the predicate stays quiet

    [Test]
    public async Task A_ceiling_at_exactly_twice_the_working_set_is_satisfiable()
    {
        var (provider, logical) = await SeededTreeAsync();

        // The floor is non-strict: a tree whose ceiling is exactly the peak
        // occupancy its own compaction cycle designs for is correctly sized,
        // not marginally misconfigured, and flagging it would make the signal
        // fire on the recommended configuration.
        var report = await Gc(provider, logical * 2).RunOnceAsync(Tree);

        Assert.That(report.CeilingUnsatisfiable, Is.False,
            "exactly 2x is the recommended sizing and must not be flagged.");
    }

    [Test]
    public async Task A_generous_ceiling_is_satisfiable()
    {
        var (provider, logical) = await SeededTreeAsync();

        var report = await Gc(provider, logical * 10).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CeilingUnsatisfiable, Is.False);
            Assert.That(report.LogicalRetainedBytes, Is.EqualTo(logical),
                "the working set is still sampled on a healthy tree, so an operator can see the "
                + "headroom rather than only being told there is some.");
        });
    }

    [Test]
    public async Task A_disabled_byte_pressure_policy_is_never_reported_unsatisfiable()
    {
        var (provider, _) = await SeededTreeAsync();

        // WalMaxRetainedBytes has no default, so this is the configuration
        // most deployments run. A tree with no ceiling has no unreachable
        // ceiling, and reporting one would put the signal permanently on for
        // the majority of the estate.
        var report = await Gc(provider, ceiling: null).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CeilingUnsatisfiable, Is.False);
            Assert.That(report.ByteCeiling, Is.Null);
        });
    }

    [Test]
    public async Task An_empty_tree_is_never_reported_unsatisfiable()
    {
        // Zero live bytes makes every positive ceiling satisfiable, and the
        // guard must reach that answer rather than dividing its way into a
        // degenerate one. This is also the shape every tree passes through at
        // creation, so a false positive here would fire on every new tree.
        var report = await Gc(new InMemoryWalStorageProvider(), ceiling: 1).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CeilingUnsatisfiable, Is.False);
            Assert.That(report.LogicalRetainedBytes, Is.Zero);
        });
    }

    [Test]
    public async Task A_provider_without_logical_byte_accounting_is_never_reported_unsatisfiable()
    {
        // The verdict is fail-quiet, not fail-closed: an unmeasurable working
        // set is "not provably unsatisfiable", and claiming otherwise would
        // indict a configuration nobody can check.
        var provider = Substitute.For<IWalStorageProvider>();
        provider.GetRetainedByteSizeAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(-1L));
        provider.GetPhysicalByteSizeAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(-1L));
        provider.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Empty());

        var report = await Gc(provider, ceiling: 1).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CeilingUnsatisfiable, Is.False);
            Assert.That(report.LogicalRetainedBytes, Is.Null,
                "and it must say the working set is unknown rather than reporting a fabricated zero, "
                + "which would be indistinguishable from an empty tree.");
        });
    }

    // ------------------------------------------------------- the constant itself

    [Test]
    public void The_working_set_multiple_is_the_reciprocal_of_the_headroom_the_default_ratio_leaves()
    {
        // Pins the derivation rather than the literal. Peak designed occupancy
        // is live / (1 - t) where t is the provider's dead-byte ratio
        // threshold; at the file provider's default 0.5 that is exactly 2. If
        // this ever needs changing, the ratio is what changed, and the doc on
        // LatticeOptions.WalMaxRetainedBytes has to change with it.
        const double DefaultDeadByteRatioThreshold = 0.5;

        Assert.That(
            LatticeOptions.WalMaxRetainedBytesWorkingSetMultiple,
            Is.EqualTo(1.0 / (1.0 - DefaultDeadByteRatioThreshold)).Within(1e-9),
            "the 2x sizing rule is derived from the compaction ratio, not chosen; a multiple that no "
            + "longer matches the ratio silently changes what the signal means.");
    }
}
