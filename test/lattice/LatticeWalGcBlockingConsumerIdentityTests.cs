using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWalGcReport.BlockingConsumerId"/> (issue
/// #2734): when an unusable durable materialiser pin short-circuits the cursor
/// floor, the report must name the consumer holding the tree, not merely state
/// that the tree is held.
/// <para>
/// The distinction is operational, not cosmetic. A blocked tree retains its WAL
/// without bound, and the only prior signal was the tree name - so identifying
/// the responsible leaf among potentially thousands meant guesswork, and a fix
/// could not demonstrate it had cleared <i>every</i> blocking leaf rather than
/// some. The consumer id embeds the owning leaf's grain id, so naming it turns
/// the condition from observable into actionable.
/// </para>
/// These tests are deliberately about <b>identity</b>, and every positive
/// assertion pins a specific expected id rather than merely asserting non-null.
/// A non-null assertion would be satisfied by any implementation that returned
/// an arbitrary or constant consumer, which is precisely the failure mode worth
/// guarding: the value is useless unless it is the <i>right</i> one.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcBlockingConsumerIdentityTests
{
    private const string Tree = "tree";

    // Two distinct leaves on the same tree. The ids follow the production shape
    // built by BPlusLeafGrain's cursor registry - "{prefix}{treeId}_{grainId}" -
    // so an assertion on these values is an assertion about a realistic id and
    // not about a token invented for the test.
    private const string BlockingLeafConsumer = "_lattice_materialiser_tree_leaf-blocking";
    private const string HealthyLeafConsumer = "_lattice_materialiser_tree_leaf-healthy";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(30)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor(int partitions = 1)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Issue #3300: the durability hold engages by default for any tree that
        // has never published a durable offset floor, which is true of every
        // tree in this fixture. This fixture asserts blocking-consumer identity,
        // a different axis, so opt out explicitly (0 disables the hold) rather
        // than depending on a default that has since changed.
        var options = new LatticeOptions { WalPartitions = partitions, WalDurabilityHoldCeilingBytes = 0 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    private static LatticeWalGc Gc(
        IWalStorageProvider provider,
        IWalCursorRegistry registry,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins,
        int partitions = 1) =>
        new(Services(provider, durablePins), registry, Monitor(partitions));

    [Test]
    public async Task RunOnceAsync_blocked_by_unusable_pin_names_the_blocking_consumer()
    {
        // The core contract. A missing leaf holding a Zero pin blocks the tree;
        // the report must say WHICH leaf, by its exact consumer id.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var report = await Gc(
            provider,
            registry,
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [BlockingLeafConsumer] = HybridLogicalClock.Zero,
            }).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "Precondition: this pass must actually be the blocked case, or the identity assertion below is vacuous.");
            Assert.That(report.BlockingConsumerId, Is.EqualTo(BlockingLeafConsumer),
                "A blocked pass must name the consumer whose unusable pin held the floor.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0),
                "Naming the blocker must not change what the pass trims - this is a diagnostic, not a policy change.");
        });
    }

    [Test]
    public async Task RunOnceAsync_names_the_unusable_pin_not_merely_the_first_pin_enumerated()
    {
        // The assertion above would also pass an implementation that returned
        // whichever pin it happened to read first. Seed a USABLE pin ahead of
        // the unusable one, so "first enumerated" and "actually blocking" are
        // different answers and only the correct one satisfies the test.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var pins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [HealthyLeafConsumer] = Hlc(10),
            [BlockingLeafConsumer] = HybridLogicalClock.Zero,
        };

        // Guard the guard: if the seeding ever collapsed to a single entry the
        // discrimination above would be lost and the test would still pass.
        Assert.That(pins, Has.Count.EqualTo(2), "The scenario needs both a usable and an unusable pin to discriminate.");

        var report = await Gc(provider, registry, pins).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(report.BlockingConsumerId, Is.EqualTo(BlockingLeafConsumer),
                "The named consumer must be the one carrying the unusable pin.");
            Assert.That(report.BlockingConsumerId, Is.Not.EqualTo(HealthyLeafConsumer),
                "A consumer with a usable pin is not blocking anything and must never be named.");
        });
    }

    [Test]
    public async Task RunOnceAsync_unblocked_pass_names_no_blocking_consumer()
    {
        // The negative control. Without this, an implementation that named a
        // consumer unconditionally would satisfy every assertion above.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var report = await Gc(
            provider,
            registry,
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [HealthyLeafConsumer] = Hlc(10),
            }).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.Not.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "Precondition: this pass must not be the blocked case.");
            Assert.That(report.BlockingConsumerId, Is.Null,
                "A pass that was never blocked must not name a blocker.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1),
                "The usable pin still floors the trim exactly as before.");
        });
    }

    [Test]
    public async Task RunOnceAsync_present_consumer_with_zero_pin_is_not_reported_as_blocking()
    {
        // A Zero pin under a consumer PRESENT in the registry is skipped before
        // the block is even evaluated: the in-memory cursor is fresher and
        // already folded into the floor. Naming it would send an operator after
        // a leaf that is behaving correctly, which is worse than naming nothing.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, BlockingLeafConsumer, Hlc(30));

        var report = await Gc(
            provider,
            registry,
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [BlockingLeafConsumer] = HybridLogicalClock.Zero,
            }).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.BlockingConsumerId, Is.Null,
                "A present consumer's stale durable pin is skipped and must not be reported as blocking.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "Steady-state trimming for a present consumer stays byte-for-byte unchanged.");
        });
    }

    [Test]
    public async Task RunOnceAsync_blocking_consumer_id_identifies_the_owning_leaf()
    {
        // The diagnostic is only useful if the operator can get from the
        // reported value to a specific leaf. Assert the structural property the
        // report's documentation promises: the id carries the owning leaf's
        // grain id, so it is resolvable rather than opaque.
        const string grainId = "leaf-00000000000000000000000000000042";
        var consumerId = $"_lattice_materialiser_{Tree}_{grainId}";

        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var report = await Gc(
            provider,
            registry,
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [consumerId] = HybridLogicalClock.Zero,
            }).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.BlockingConsumerId, Is.EqualTo(consumerId));
            Assert.That(report.BlockingConsumerId, Does.Contain(grainId),
                "The reported id must carry the owning leaf's grain id, or it cannot be acted on.");
            Assert.That(report.BlockingConsumerId, Does.Contain(Tree),
                "The reported id must carry the tree it belongs to.");
        });
    }

    [Test]
    public void Report_constructed_without_a_blocking_consumer_defaults_to_none()
    {
        // The parameter is additive and optional, so every existing construction
        // site keeps compiling. Pin the default explicitly: if it were ever given
        // a non-null default, every unblocked pass would start naming a blocker.
        var report = new LatticeWalGcReport(
            Tree, null, null, null, null, 1, 0, null, null, null, false, false);

        Assert.Multiple(() =>
        {
            Assert.That(report.BlockingConsumerId, Is.Null);
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.Available));
        });
    }

    // ------------------------------------------------- the blocking consumer set

    [Test]
    public async Task RunOnceAsync_names_every_blocking_consumer_not_only_the_first()
    {
        // Issue #2768. Naming one blocker was sufficient to diagnose a tree but
        // not to drain one: the healing sweep's limits are all per blocking
        // leaf, so a report naming a single leaf per pass turned them into a
        // per-tree rate limit and a tree carrying thousands of blocked leaves
        // could never converge.
        //
        // The old floor short-circuited the moment its blocked count reached
        // the partition count, which with a single partition is the first
        // unusable pin it saw - so this scenario previously reported exactly
        // one id no matter how many leaves were blocking.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var blocking = new[] { "_lattice_materialiser_tree_leaf-1", "_lattice_materialiser_tree_leaf-2", "_lattice_materialiser_tree_leaf-3" };
        var pins = blocking.ToDictionary(c => c, _ => HybridLogicalClock.Zero, StringComparer.Ordinal);

        Assert.That(pins, Has.Count.EqualTo(3), "The scenario needs several blocking leaves, or the set assertion is vacuous.");

        var report = await Gc(provider, registry, pins).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "Precondition: this pass must actually be the blocked case.");
            Assert.That(report.BlockingConsumerIds, Is.EquivalentTo(blocking),
                "A blocked pass must name every blocking consumer it found, so the sweep can work on more than one per pass.");
            Assert.That(report.BlockingConsumerId, Is.AnyOf(blocking),
                "And must keep naming a single primary blocker, which existing readers still consume.");
        });
    }

    [Test]
    public async Task RunOnceAsync_bounds_the_blocking_consumer_set()
    {
        // The other side of the same change. A blocked tree can carry thousands
        // of unusable pins, and a report that carried all of them would put an
        // unbounded list on every pass of a hot diagnostic path. The set is a
        // work queue for a sweep that touches a handful of leaves per pass, not
        // an inventory, so it is capped.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var overCap = LatticeWalGc.MaxReportedBlockingConsumers * 3;
        var pins = Enumerable.Range(0, overCap)
            .ToDictionary(i => $"_lattice_materialiser_tree_leaf-{i}", _ => HybridLogicalClock.Zero, StringComparer.Ordinal);

        Assert.That(pins, Has.Count.GreaterThan(LatticeWalGc.MaxReportedBlockingConsumers),
            "The scenario must exceed the cap, or the bound assertion is vacuous.");

        var report = await Gc(provider, registry, pins).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(report.BlockingConsumerIds, Has.Count.EqualTo(LatticeWalGc.MaxReportedBlockingConsumers),
                "The set must be capped, or a badly blocked tree puts an unbounded list on every pass.");
        });
    }

    [Test]
    public async Task RunOnceAsync_bounds_the_blocking_consumer_set_when_no_short_circuit_applies()
    {
        // The bound above is satisfied by the pass's cheap short-circuit, which
        // returns as soon as every partition is blocked AND the set is full -
        // so with a single partition that assertion cannot tell whether the
        // append itself is bounded. It is the append that has to be, because
        // the short-circuit only fires once every partition is blocked.
        //
        // Here several partitions exist and every blocking pin is attributable
        // to partition 0, so the other partitions stay unblocked, no
        // short-circuit is reachable, and the whole pin dictionary is walked.
        // The set is then bounded by the append guard or by nothing at all.
        const int Partitions = 4;
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var overCap = LatticeWalGc.MaxReportedBlockingConsumers * 3;
        var pins = Enumerable.Range(0, overCap)
            .ToDictionary(i => $"_lattice_materialiser_tree_leaf-{i}_0", _ => HybridLogicalClock.Zero, StringComparer.Ordinal);

        var report = await Gc(provider, registry, pins, Partitions).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "Precondition: this pass must be the blocked case.");
            Assert.That(report.BlockingConsumerIds, Has.Count.EqualTo(LatticeWalGc.MaxReportedBlockingConsumers),
                "With no short-circuit reachable, the append itself must bound the set.");
        });
    }

    [Test]
    public async Task RunOnceAsync_never_names_a_usable_pin_in_the_blocking_set()
    {
        // The negative control for the set, mirroring the one that already
        // guards the single id. Without it, an implementation that appended
        // every consumer it enumerated would satisfy both assertions above.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var pins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [HealthyLeafConsumer] = Hlc(10),
            [BlockingLeafConsumer] = HybridLogicalClock.Zero,
        };

        var report = await Gc(provider, registry, pins).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.BlockingConsumerIds, Is.EquivalentTo(new[] { BlockingLeafConsumer }),
                "Only consumers whose pin is unusable are blocking anything.");
            Assert.That(report.BlockingConsumerIds, Does.Not.Contain(HealthyLeafConsumer),
                "Naming a healthy leaf would send the sweep to reactivate a leaf that is holding nothing.");
        });
    }

    [Test]
    public async Task RunOnceAsync_unblocked_pass_carries_no_blocking_consumer_set()
    {
        // An unblocked pass must carry null rather than an empty list, so a
        // reader cannot mistake "not blocked" for "blocked by nobody".
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var report = await Gc(
            provider,
            registry,
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [HealthyLeafConsumer] = Hlc(10),
            }).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.Not.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "Precondition: this pass must not be the blocked case.");
            Assert.That(report.BlockingConsumerIds, Is.Null);
        });
    }
}
