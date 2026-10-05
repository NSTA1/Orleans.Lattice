using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #4622: a standing durable block pin - a <see cref="HybridLogicalClock.Zero"/>
/// frontier the durable offset floor does not cover - holds its partition against
/// every admitting arm, the retention ceiling and the cursor included, whether or not
/// the leaf is in the in-memory registry. Such a leaf has never checkpointed, so its
/// cold activation could not detect a trim it missed. Any other leaf pin the offset
/// floor does not cover caps the retention ceiling at its frontier.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcBlockPinHoldTests
{
    private const string Tree = "tree";
    private const string Leaf = "_lattice_materialiser_tree_leaf-1_0";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public async Task A_retention_ceiling_does_not_trim_a_partition_a_standing_block_pin_holds()
    {
        var provider = await SeededAsync();
        var gc = Gc(provider, new InMemoryWalCursorRegistry(), retention: TimeSpan.FromMilliseconds(1));

        var report = await gc.RunOnceAsync(Tree);

        Assert.Multiple(async () =>
        {
            Assert.That(report.TtlCeilingHlc, Is.Not.Null, "the retention ceiling is armed");
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(await RetainedAsync(provider), Is.EqualTo(new long[] { 0, 1 }));
        });
    }

    [Test]
    public async Task A_registered_leafs_cursor_does_not_trim_a_partition_its_standing_block_pin_holds()
    {
        // The leaf is live and has reported its in-memory frontier, but its durable
        // pin is still the block pin: it has made nothing durable.
        var provider = await SeededAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, Leaf, Hlc(100));
        var gc = Gc(provider, registry, retention: null);

        var report = await gc.RunOnceAsync(Tree);

        Assert.Multiple(async () =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(await RetainedAsync(provider), Is.EqualTo(new long[] { 0, 1 }));
        });
    }

    [Test]
    public async Task A_retention_ceiling_is_capped_at_an_uncovered_leaf_pins_frontier()
    {
        // The leaf released the partition empty at frontier 10 (no offset), then
        // wrote the entry stamped 11: the max-merged pin store still reads (10, -1).
        var provider = await SeededAsync();
        var gc = Gc(provider, new InMemoryWalCursorRegistry(), retention: TimeSpan.FromMilliseconds(1), pinFrontier: Hlc(10));

        var report = await gc.RunOnceAsync(Tree);

        Assert.Multiple(async () =>
        {
            Assert.That(report.TtlCeilingHlc, Is.Not.Null, "the retention ceiling is armed");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1), "the entry at the frontier is trimmed");
            Assert.That(await RetainedAsync(provider), Is.EqualTo(new long[] { 1 }),
                "the entry stamped above the uncovered leaf pin's frontier is kept");
        });
    }

    [TestCase(10L, true, TestName = "The_leaf_pin_hold_age_ages_a_partition_whose_retention_a_leaf_pin_holds")]
    [TestCase(long.MaxValue, false, TestName = "The_leaf_pin_hold_age_stays_zero_when_the_leaf_pin_is_newer_than_the_window")]
    public async Task The_leaf_pin_hold_age_reports_how_long_retention_has_been_held(long frontierTicks, bool held)
    {
        var tree = "hold-age-" + Guid.NewGuid().ToString("N")[..8];
        var provider = await SeededAsync(tree);
        var time = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var gc = Gc(provider, new InMemoryWalCursorRegistry(), TimeSpan.FromMilliseconds(1), Hlc(frontierTicks), time);
        var observed = new Dictionary<int, long>();
        using var listener = Orleans.Lattice.Testing.MeterListening.StartForInstrument(
            WalGcLeafPinHoldCensus.Gauge,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? measuredTree = null;
                int? shard = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        measuredTree = tag.Value as string;
                    }
                    else if (tag.Key == LatticeMetrics.TagShard)
                    {
                        shard = (int?)tag.Value;
                    }
                }

                if (measuredTree == tree && shard is { } s)
                {
                    observed[s] = value;
                }
            }));

        await gc.RunOnceAsync(tree);
        time.Advance(TimeSpan.FromSeconds(90));
        await gc.RunOnceAsync(tree);
        listener.RecordObservableInstruments();

        Assert.That(observed.GetValueOrDefault(0, -2), Is.EqualTo(held ? 90 : 0),
            "the gauge publishes the age of the hold on partition 0");
    }

    private static Task<InMemoryWalStorageProvider> SeededAsync() => SeededAsync(Tree);

    private static async Task<InMemoryWalStorageProvider> SeededAsync(string tree)
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            tree,
            0,
            Enumerable.Range(0, 2).Select(o => new WalEntry
            {
                Offset = o,
                Mutation = new LatticeMutation
                {
                    TreeId = tree,
                    Kind = MutationKind.Set,
                    Key = $"k{o}",
                    Value = [1],
                    Timestamp = Hlc(10 + o),
                    OriginClusterId = "site-a",
                },
            }).ToArray(),
            CancellationToken.None);
        return provider;
    }

    private static LatticeWalGc Gc(
        IWalStorageProvider provider,
        InMemoryWalCursorRegistry registry,
        TimeSpan? retention,
        HybridLogicalClock? pinFrontier = null,
        TimeProvider? time = null)
    {
        // By default the leaf's durable pin is the block pin seeded at birth: a Zero
        // frontier and the "-1" nothing-durably-applied offset.
        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [Leaf] = pinFrontier ?? HybridLogicalClock.Zero }));
        pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(
            new Dictionary<string, long>(StringComparer.Ordinal) { [Leaf] = -1 }));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        var services = new ServiceCollection().AddSingleton(provider).AddSingleton(factory).BuildServiceProvider();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalRetention = retention,
            WalDurabilityHoldCeilingBytes = 0,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return new LatticeWalGc(services, registry, monitor, time);
    }

    private static async Task<List<long>> RetainedAsync(IWalStorageProvider provider)
    {
        var retained = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, -1, 16, CancellationToken.None))
        {
            retained.Add(entry.Offset);
        }

        return retained;
    }
    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}