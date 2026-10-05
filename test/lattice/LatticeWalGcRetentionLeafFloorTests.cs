using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// A <see cref="LatticeOptions.WalRetention"/> trim is a retention event, not a
/// licence to drop acknowledged writes a leaf has not made durable. The TTL arm
/// admits an entry however old it is, but the durable materialiser offset floor
/// stops the scan first, so a leaf with a durable checkpoint is never overtaken by
/// a TTL trim: what the retention ceiling releases past its checkpoint the leaf's
/// snapshot already holds.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcRetentionLeafFloorTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1_0";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public async Task RunOnceAsync_retention_ttl_never_trims_past_a_leafs_durable_offset_floor()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            Enumerable.Range(0, 4).Select(o => new WalEntry
            {
                Offset = o,
                Mutation = new LatticeMutation
                {
                    TreeId = Tree,
                    Kind = MutationKind.Set,
                    Key = $"k{o}",
                    Value = [1],
                    Timestamp = Hlc(10 + o),
                    OriginClusterId = "site-a",
                },
            }).ToArray(),
            CancellationToken.None);

        // Every entry is far older than the one-millisecond retention window, and
        // the leaf's cursor admits every one of them; only its durable offset (1)
        // stands between the TTL arm and offsets 2 and 3.
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100));
        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [LeafConsumer] = Hlc(100) }));
        pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(
            new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = 1 }));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        var services = new ServiceCollection().AddSingleton<IWalStorageProvider>(provider).AddSingleton(factory).BuildServiceProvider();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalRetention = TimeSpan.FromMilliseconds(1),
            WalDurabilityHoldCeilingBytes = 0,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var report = await new LatticeWalGc(services, registry, monitor).RunOnceAsync(Tree);

        var retained = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, -1, 16, CancellationToken.None))
        {
            retained.Add(entry.Offset);
        }

        Assert.Multiple(() =>
        {
            Assert.That(report.TtlCeilingHlc, Is.Not.Null, "the retention ceiling is armed");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(2), "the ceiling trims through the leaf's durable offset");
            Assert.That(retained, Is.EqualTo(new long[] { 2, 3 }),
                "a retention trim passed the leaf's durable offset floor");
        });
    }
}
