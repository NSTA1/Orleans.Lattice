using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for issue #3366: a WAL GC pass whose cursor registry read
/// FAILED classified the tree as
/// <c>WalGcCursorAuthority.Durable</c> - the value that DISABLES the durability
/// hold - and then trimmed on HLC eligibility alone, with no evidence that any
/// leaf had durably applied the entries it released.
/// <para>
/// The original site was a bare <c>catch { return Durable; }</c> whose XML doc
/// described it as failing closed. It is not. <c>Durable</c> is the permissive
/// answer: it removes the only guard (<c>durabilityHold &amp;&amp; offsetFloor
/// is null</c>) standing between an unverified pass and the trim. The stated
/// rationale - "an unread registry is not evidence that nothing durable is
/// watching" - is true but symmetric, since an unread registry is equally not
/// evidence that something durable IS watching, so it cannot select between the
/// two answers. What breaks the tie is the asymmetry of the costs: holding
/// costs bounded disk that the hold ceiling already caps, while releasing costs
/// acknowledged writes that nothing can reconstruct.
/// </para>
/// <para>
/// The failure was also SILENT, which is why it survived. Both durability-hold
/// counters are gated on <c>holdConfigured</c>, and a <c>Durable</c>
/// classification makes that predicate false - so the very counters that would
/// have reported "I disabled the hold" were themselves disabled by the thing
/// they were meant to report. In the field this presented as
/// <c>wal_gc_durability_hold_forced</c> and <c>_engaged</c> being ABSENT
/// (never published at all, as distinct from zero) while
/// <c>trim_stop{reason="exhausted"}</c> climbed - the reading of a tree being
/// fully trimmed with the gate never once engaging.
/// </para>
/// <para>
/// Three independent properties are asserted, and they fail differently.
/// <b>Retention</b>: entries survive a pass whose registry threw, asserted by
/// reading the provider back rather than by trusting a counter. <b>Naming</b>:
/// the hold reports its own <c>cursor_unreadable</c> arm, so "could not
/// measure" is never reported as a measurement. <b>Bounding</b>: the ceiling
/// still forces progress, so failing closed cannot regress into the unbounded
/// retention of issue #3094.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcCursorRegistryUnreadableTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

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

    /// <summary>
    /// Decorates a real registry and fails only <see cref="SnapshotAsync"/>.
    /// Everything else delegates, so the pass proceeds exactly as it otherwise
    /// would and the classification read is the single variable under test.
    /// </summary>
    private sealed class SnapshotFailingCursorRegistry(IWalCursorRegistry inner, bool failSnapshot) : IWalCursorRegistry
    {
        public int SnapshotAttempts { get; private set; }

        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, CancellationToken cancellationToken = default) =>
            inner.ReportCursorAsync(treeName, consumerId, cursor, cancellationToken);

        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, HybridLogicalClock? blockedAtHlc, CancellationToken cancellationToken = default) =>
            inner.ReportCursorAsync(treeName, consumerId, cursor, blockedAtHlc, cancellationToken);

        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, VersionVector vector, CancellationToken cancellationToken = default) =>
            inner.ReportCursorAsync(treeName, consumerId, cursor, vector, cancellationToken);

        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, VersionVector vector, HybridLogicalClock? blockedAtHlc, CancellationToken cancellationToken = default) =>
            inner.ReportCursorAsync(treeName, consumerId, cursor, vector, blockedAtHlc, cancellationToken);

        public Task UnregisterAsync(string treeName, string consumerId, CancellationToken cancellationToken = default) =>
            inner.UnregisterAsync(treeName, consumerId, cancellationToken);

        public Task<HybridLogicalClock?> GetMinCursorAsync(string treeName, CancellationToken cancellationToken = default) =>
            inner.GetMinCursorAsync(treeName, cancellationToken);

        public Task<HybridLogicalClock?> GetMinCursorForDrainLagAsync(string treeName, long reportedAtOrAfterTicks, CancellationToken cancellationToken = default) =>
            inner.GetMinCursorForDrainLagAsync(treeName, reportedAtOrAfterTicks, cancellationToken);

        public Task<VersionVector?> GetCausalStableAsync(string treeName, CancellationToken cancellationToken = default) =>
            inner.GetCausalStableAsync(treeName, cancellationToken);

        public Task<HybridLogicalClock?> GetBlockedFloorAsync(string treeName, CancellationToken cancellationToken = default) =>
            inner.GetBlockedFloorAsync(treeName, cancellationToken);

        public Task<IReadOnlyList<WalCursorSnapshot>> SnapshotAsync(string treeName, CancellationToken cancellationToken = default)
        {
            SnapshotAttempts++;
            return failSnapshot
                // Shaped after the real trigger: the registry read needs a
                // replay permit, and a saturated cluster refuses it.
                ? throw new InvalidOperationException("cursor registry unavailable (simulated saturation)")
                : inner.SnapshotAsync(treeName, cancellationToken);
        }
    }

    private static IOptionsMonitor<LatticeOptions> Monitor(long? holdCeiling)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalDurabilityHoldCeilingBytes = holdCeiling,
        };

        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static async Task<InMemoryWalStorageProvider> SeededAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(1)), Entry(1, Hlc(2)), Entry(2, Hlc(3)) },
            CancellationToken.None);
        return provider;
    }

    private static async Task<(LatticeWalGc Sut, SnapshotFailingCursorRegistry Registry)> CollectorAsync(
        IWalStorageProvider provider,
        bool failSnapshot,
        long? holdCeiling,
        bool includeForeignConsumer = false)
    {
        var inner = new InMemoryWalCursorRegistry();
        if (includeForeignConsumer)
        {
            // A non-materialiser consumer whose cursor outlives this process.
            // This is the ONLY shape for which `Durable` is a correct answer.
            await inner.ReportCursorAsync(Tree, "shipper", Hlc(30));
        }

        await inner.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var registry = new SnapshotFailingCursorRegistry(inner, failSnapshot);

        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        // No durable pins and no pin offsets, so the durable materialiser offset
        // floor is ABSENT. That is the state the hold exists to protect, and it
        // is load-bearing for reaching the code under test:
        // ApplyDurableMaterialiserFloorAsync takes its own registry snapshot,
        // but returns early at `pins.Count == 0` BEFORE doing so. With pins
        // present the pass would fault at that earlier, unprotected call and
        // never reach the classification at all - so an empty pin store is both
        // the faithful reproduction of the observed production state and the
        // only shape in which the defect is reachable.
        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return (new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor(holdCeiling)), registry);
    }

    private static async Task<(List<string> Stops, List<string> Engaged, List<string> Forced)> RunAsync(LatticeWalGc sut)
    {
        var stops = new List<string>();
        var engaged = new List<string>();
        var forced = new List<string>();

        static Action<MeterListener> Collect(List<string> sink, bool onlyAdvanced) => l =>
            l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                if (onlyAdvanced && measurement <= 0)
                {
                    return;
                }

                string? reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                }

                lock (sink)
                {
                    sink.Add(reason ?? "<untagged>");
                }
            });

        using var stopListener = MeterListening.StartForInstrument(LatticeMetrics.WalGcTrimStops, Collect(stops, onlyAdvanced: true));
        using var engagedListener = MeterListening.StartForInstrument(LatticeMetrics.WalGcDurabilityHoldEngaged, Collect(engaged, onlyAdvanced: false));
        using var forcedListener = MeterListening.StartForInstrument(LatticeMetrics.WalGcDurabilityHoldForced, Collect(forced, onlyAdvanced: false));

        await sut.RunOnceAsync(Tree);
        return (stops, engaged, forced);
    }

    private static async Task<int> RetainedCountAsync(IWalStorageProvider provider)
    {
        var count = 0;
        await foreach (var _ in provider.ReadAsync(Tree, 0, -1, int.MaxValue, CancellationToken.None))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// The control, and the half of the pair that gives the assertion below its
    /// meaning. With the registry READABLE and a non-materialiser consumer
    /// present, <c>Durable</c> is the correct classification, the hold does not
    /// engage, and the entries are released. Without this arm, a test showing
    /// retention on a failed read could equally be showing a collector that
    /// never trims anything.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_ReadableRegistryWithDurableConsumer_TrimsEntries()
    {
        var provider = await SeededAsync();
        var (sut, registry) = await CollectorAsync(provider, failSnapshot: false, holdCeiling: 1024 * 1024, includeForeignConsumer: true);

        var (stops, engaged, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(registry.SnapshotAttempts, Is.GreaterThan(0), "the classification must actually read the registry");
            Assert.That(engaged, Is.Empty, "a durably-watched tree must not engage the hold");
            Assert.That(stops, Does.Not.Contain("durability_hold"));
        });

        Assert.That(await RetainedCountAsync(provider), Is.Zero, "eligible entries must be released when the registry is readable and a durable consumer is watching");
    }

    /// <summary>
    /// The defect itself. A registry read that THROWS must not be reported as
    /// evidence of durability. Asserted by reading the provider back, so it
    /// holds independently of whether any counter fires - a counter-only
    /// assertion would pass against a build that recorded the arm and trimmed
    /// anyway.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_UnreadableRegistry_RetainsEntriesInsteadOfTrimming()
    {
        var provider = await SeededAsync();
        var (sut, registry) = await CollectorAsync(provider, failSnapshot: true, holdCeiling: 1024 * 1024, includeForeignConsumer: true);

        var (stops, _, _) = await RunAsync(sut);

        // Retention asserted FIRST and on its own: it is the property that
        // matters, it is read back from the provider rather than from a
        // counter, and a pre-fix build fails here with a count of zero - the
        // data loss itself, not a mislabelled stop.
        Assert.That(
            await RetainedCountAsync(provider),
            Is.EqualTo(3),
            "entries must survive a pass that could not establish who is watching");

        Assert.Multiple(() =>
        {
            Assert.That(registry.SnapshotAttempts, Is.GreaterThan(0), "the classification must have attempted the read it failed");
            Assert.That(stops, Does.Contain("durability_hold"), "the pass must stop on the durability hold, not on an eligibility arm");
        });
    }

    /// <summary>
    /// The refusal must be attributable. Both pre-existing engaged arms
    /// (<c>never_pinned</c>, <c>pin_regressed</c>) are statements about the
    /// tree's pin history, and neither was measured on a pass whose registry
    /// read failed - so reporting either would assert a fact the pass does not
    /// have, and would send an operator to repair a materialiser when the fault
    /// is in the registry read path.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_UnreadableRegistry_ReportsItsOwnEngagedArm()
    {
        var provider = await SeededAsync();
        var (sut, _) = await CollectorAsync(provider, failSnapshot: true, holdCeiling: 1024 * 1024, includeForeignConsumer: true);

        var (_, engaged, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(engaged, Does.Contain("cursor_unreadable"), "the hold must name the reason it engaged");
            Assert.That(engaged, Does.Not.Contain("never_pinned"), "an unmeasured pin history must not be reported as measured");
            Assert.That(engaged, Does.Not.Contain("pin_regressed"), "an unmeasured pin history must not be reported as measured");
        });
    }

    /// <summary>
    /// Failing closed must stay BOUNDED. An unbounded hold would be the easy
    /// fix and the wrong one: it converts the data-loss defect of issue #3366
    /// into the unbounded WAL growth of issue #3094 on every deployment whose
    /// registry is transiently unreachable. With the retained footprint already
    /// over the ceiling, the collector must trim and say loudly that it did.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_UnreadableRegistryOverCeiling_ForcesProgress()
    {
        var provider = await SeededAsync();

        // A one-byte ceiling is already exceeded by the seeded entries, so the
        // hold is configured but has no budget.
        var (sut, _) = await CollectorAsync(provider, failSnapshot: true, holdCeiling: 1, includeForeignConsumer: true);

        var (stops, engaged, forced) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(forced, Is.Not.Empty, "exceeding the ceiling must be reported, not silently absorbed");
            Assert.That(engaged, Does.Not.Contain("cursor_unreadable"), "a hold with no budget has not engaged");
            Assert.That(stops, Does.Not.Contain("durability_hold"), "the hold must not block progress once its ceiling is exhausted");
        });

        Assert.That(
            await RetainedCountAsync(provider),
            Is.Zero,
            "the ceiling must force progress, so failing closed cannot regress into unbounded retention");
    }
}
