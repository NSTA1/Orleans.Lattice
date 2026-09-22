using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Exercises the issue #3310 pin-delivery remedy through the reporter, pin
/// store and GC, rather than moving the GC's input floor by hand.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcPinDeliveryTests
{
    private const string Tree = "pin-delivery-3310";
    private const string Consumer = "_lattice_materialiser_pin-delivery-3310_leaf";
    private const int WaveSize = 8;

    [SetUp]
    public void SetUp() => WalMaterialiserPinPressure.ResetForTests();

    [TearDown]
    public void TearDown() => WalMaterialiserPinPressure.ResetForTests();

    [TestCase(false)]
    [TestCase(true)]
    public async Task RunOnceAsync_shed_ceiling_unfreezes_the_floor_without_trimming_the_uncovered_tail(
        bool armCeiling)
    {
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalMaterialiserPinShards = 1,
            WalMaterialiserPinFlushIntervalMs = 0,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", Tree));
        var state = new FakePersistentState<WalMaterialiserPinState>();
        var pin = new WalMaterialiserPinGrain(context, state, monitor);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);

        var registry = new InMemoryWalCursorRegistry();
        var provider = new InMemoryWalStorageProvider();
        using var services = new ServiceCollection()
            .AddSingleton<IWalStorageProvider>(provider)
            .AddSingleton(factory)
            .BuildServiceProvider();
        var gc = new LatticeWalGc(services, registry, monitor);
        var reporter = new LeafCursorReporter(registry, factory, monitor);
        var forced = 0L;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.MaterialiserPinShedForced,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, Tree))
                    {
                        forced += value;
                    }
                }
            }));

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, [new MaterialiserPinReport(Consumer, Hlc(100), 0)], CancellationToken.None);
        WalMaterialiserPinPressure.ForceShedForTests(Tree, 3_600_000);

        for (var wave = 0; wave < 3; wave++)
        {
            await AppendWaveAsync(wave);
            await gc.RunOnceAsync(Tree);
            Assert.That((await pin.GetPinOffsetsAsync())[Consumer], Is.Zero);
            Assert.That(await RetainedOffsetsAsync(), Has.Count.EqualTo((wave + 1) * WaveSize - 1),
                "The usable offset floor is frozen despite advancing checkpoint reports, not merely lagging.");
        }

        options.WalMaterialiserPinShedCeiling = armCeiling ? TimeSpan.FromMilliseconds(1) : null;
        var priorFloor = 0L;
        for (var wave = 3; wave < 6; wave++)
        {
            await AppendWaveAsync(wave);
            var floor = (await pin.GetPinOffsetsAsync())[Consumer];
            var report = await gc.RunOnceAsync(Tree);
            var retained = await RetainedOffsetsAsync();
            var head = (wave + 1) * WaveSize - 1;

            Assert.That(retained, Is.EqualTo(Enumerable.Range((int)floor + 1, head - (int)floor)),
                "Every entry above the delivered durability evidence must survive, even when forcing fires.");
            if (armCeiling)
            {
                Assert.That(floor, Is.GreaterThan(priorFloor),
                    "A real report must reach the pin store on every recovery wave; GC inputs are never restamped by the test.");
                Assert.That(report.EntriesTrimmed, Is.GreaterThan(0));
                Assert.That(retained, Has.Count.EqualTo(2),
                    "Retention must converge to the genuinely uncovered two-entry tail, not grow with total writes.");
            }
            else
            {
                Assert.That(floor, Is.Zero);
                Assert.That(report.EntriesTrimmed, Is.Zero);
                Assert.That(retained, Has.Count.EqualTo((wave + 1) * WaveSize - 1));
            }
            priorFloor = floor;
        }

        Assert.That(forced, armCeiling ? Is.GreaterThan(0) : Is.Zero,
            "The positive arm witnesses the counter; the disabled control must not force a report.");

        async Task AppendWaveAsync(int wave)
        {
            var entries = Enumerable.Range(wave * WaveSize, WaveSize)
                .Select(offset => new WalEntry
                {
                    Offset = offset,
                    Mutation = new LatticeMutation
                    {
                        TreeId = Tree,
                        Kind = MutationKind.Set,
                        Key = $"k{offset}",
                        Value = [1],
                        Timestamp = Hlc(100 + offset),
                        OriginClusterId = "site-a",
                    },
                }).ToArray();
            await provider.AppendBatchAsync(Tree, 0, entries, CancellationToken.None);
            var head = entries[^1].Offset;
            await registry.ReportCursorAsync(Tree, "shipper", Hlc(100 + head));
            await reporter.ReportAsync(Tree, Consumer, Hlc(100 + head), CancellationToken.None);

            // The reporter uses TickCount64, not TimeProvider. Cross its 1 s
            // debounce so each wave genuinely attempts the production shed gate.
            await Task.Delay(1100);
            reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100 + head), head - 2);
            // A successful force ends the run. The next shed opens a new one;
            // retry after its ceiling rather than assuming the run stayed open.
            await Task.Delay(10);
            reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100 + head), head - 2);
        }

        async Task<List<long>> RetainedOffsetsAsync()
        {
            var result = new List<long>();
            await foreach (var entry in provider.ReadAsync(Tree, 0, -1, 100, CancellationToken.None))
            {
                result.Add(entry.Offset);
            }
            return result;
        }
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };
}
