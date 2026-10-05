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
/// cold activation could not detect a trim it missed.
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

    private static async Task<InMemoryWalStorageProvider> SeededAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            Enumerable.Range(0, 2).Select(o => new WalEntry
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
        return provider;
    }

    private static LatticeWalGc Gc(IWalStorageProvider provider, InMemoryWalCursorRegistry registry, TimeSpan? retention)
    {
        // The leaf's durable pin is the block pin seeded at birth: a Zero frontier
        // and the "-1" nothing-durably-applied offset.
        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [Leaf] = HybridLogicalClock.Zero }));
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
        return new LatticeWalGc(services, registry, monitor);
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
}
