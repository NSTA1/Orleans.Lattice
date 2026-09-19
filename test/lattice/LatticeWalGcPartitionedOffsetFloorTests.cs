using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the per-partition durable materialiser offset floor
/// (issue #3178).
/// <para>
/// The defect: <c>ComputeMaterialiserOffsetFloorAsync</c> reduced every
/// <c>(leaf, partition)</c> durable pin to a single tree-wide scalar minimum and
/// applied it to every shard. A leaf's pin for partition <c>p</c> only ever
/// advances when an entry is appended to partition <c>p</c>, so a partition that
/// converged and then fully drained holds its terminal checkpoint permanently -
/// and, minimised across partitions, that terminal value capped every sibling
/// partition's trim scan forever. No reactivation could lift it: the holding
/// partition is empty by construction, so there is nothing to replay and nothing
/// to advance over.
/// </para>
/// <para>
/// Measured on a live deployment before the fix: five trees
/// (<c>repo-context-content</c>, <c>-structural</c>, <c>-symbol</c>,
/// <c>-xref</c>, <c>view-sys-backup-catalog-index</c>) each had exactly one
/// entry-free WAL shard and reported <c>trim_stop{exhausted}=0</c> with
/// <c>entries_trimmed=0</c> for the whole process lifetime, while ten trees with
/// no entry-free shard reclaimed normally. For <c>repo-context-symbol</c>,
/// shard-0 was a 30-byte header-only file whose terminal offset was 13684 and
/// whose leaves all held a durable checkpoint of exactly 13684, while shard-4
/// held 157,971 bytes whose first retained entry was at offset 13685 - one past
/// the floor a partition holding none of those entries had frozen.
/// </para>
/// <para>
/// Note this is NOT the case the <c>-1</c> sentinel in the floor builder already
/// skips. That sentinel catches a partition that was never written. A
/// converged-then-drained partition was written, was fully consumed, and was
/// then trimmed to nothing, so its leaves hold a REAL durable checkpoint
/// (<c>offset &gt;= 0</c>). The gap was in the predicate, not an oversight about
/// empty partitions in general.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcPartitionedOffsetFloorTests
{
    private const string Tree = "tree";

    /// <summary>Pin for the drained partition 0 - a real, frozen checkpoint.</summary>
    private const string DrainedPartitionConsumer = "_lattice_materialiser_tree_leaf-1_0";

    /// <summary>Pin for the live partition 1, caught up at its own head.</summary>
    private const string LivePartitionConsumer = "_lattice_materialiser_tree_leaf-1_1";

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
    /// Partition 0 is drained: it holds no entries at all, exactly like the
    /// 30-byte header-only shards measured in production. Partition 1 retains
    /// three entries at offsets 10-12, every one of them strictly above
    /// partition 0's frozen checkpoint of 5.
    /// </summary>
    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            1,
            new[] { Entry(10, Hlc(10)), Entry(11, Hlc(11)), Entry(12, Hlc(12)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 2 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins,
        IReadOnlyDictionary<string, long>? durableOffsets)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));
        if (durableOffsets is not null)
        {
            pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult(durableOffsets));
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    private static async Task<List<long>> SurvivingOffsetsAsync(IWalStorageProvider provider, int partition)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, partition, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }
        return survivors;
    }

    private static async Task<LatticeWalGcReport> RunAsync(
        InMemoryWalStorageProvider provider,
        IReadOnlyDictionary<string, long> durableOffsets)
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, DrainedPartitionConsumer, Hlc(20));
        await registry.ReportCursorAsync(Tree, LivePartitionConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [DrainedPartitionConsumer] = Hlc(20),
            [LivePartitionConsumer] = Hlc(20),
        };

        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());
        return await sut.RunOnceAsync(Tree);
    }

    [Test]
    public async Task RunOnceAsync_drained_partition_checkpoint_does_not_pin_a_sibling_partition()
    {
        // Partition 0 is drained and its leaf's checkpoint is frozen at 5 - it
        // can never advance, because nothing will ever be appended to partition
        // 0 again. Partition 1's leaf is caught up at its own head, offset 12.
        //
        // Before the fix the tree-wide minimum was min(5, 12) = 5, applied to
        // partition 1, whose first retained entry at offset 10 is strictly above
        // it. The scan stopped on that very first entry and reclaimed nothing,
        // for the lifetime of the process.
        var provider = await SeededProviderAsync();
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [DrainedPartitionConsumer] = 5,
            [LivePartitionConsumer] = 12,
        };

        var report = await RunAsync(provider, durableOffsets);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
            "Partition 1 trims against its OWN pin (12), not against the drained partition 0's frozen checkpoint (5).");

        var survivors = await SurvivingOffsetsAsync(provider, 1);
        Assert.That(survivors, Is.Empty,
            "Every entry at or below partition 1's own durable checkpoint is reclaimable.");
    }

    [Test]
    public async Task RunOnceAsync_partition_pin_below_its_own_entries_still_retains_them()
    {
        // The safety complement, and the guard against over-relaxing: when the
        // pin for partition 1 genuinely sits below partition 1's entries, those
        // entries must still be retained. This is issue #3178's acceptance
        // criterion 2 - never advance a pin past an entry a leaf genuinely owns
        // and has not applied.
        var provider = await SeededProviderAsync();
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [DrainedPartitionConsumer] = 5,
            [LivePartitionConsumer] = 10,
        };

        var report = await RunAsync(provider, durableOffsets);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1),
            "Only the entry at or below partition 1's own checkpoint (10) is trimmed.");

        var survivors = await SurvivingOffsetsAsync(provider, 1);
        Assert.That(survivors, Is.EqualTo(new[] { 11L, 12L }),
            "Entries above partition 1's own durable checkpoint must survive.");
    }

    [Test]
    public async Task RunOnceAsync_unsuffixed_legacy_pin_still_constrains_every_partition()
    {
        // Fail-closed case. A consumer id with no partition suffix is the legacy
        // single-partition shape and could speak for any partition, so it must
        // keep constraining the whole tree. If it were dropped once any
        // suffixed pin existed, this would be a relaxation the change does not
        // intend and cannot justify.
        var provider = await SeededProviderAsync();
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            ["_lattice_materialiser_tree_leaf-legacy"] = 10,
            [LivePartitionConsumer] = 12,
        };

        var report = await RunAsync(provider, durableOffsets);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1),
            "The unattributable legacy pin (10) is folded into partition 1's floor, so only offset 10 trims.");

        var survivors = await SurvivingOffsetsAsync(provider, 1);
        Assert.That(survivors, Is.EqualTo(new[] { 11L, 12L }),
            "An unsuffixed pin must keep constraining every partition.");
    }

    [Test]
    public void TryParseConsumerPartition_accepts_only_a_canonical_partition_suffix()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeWalGc.TryParseConsumerPartition("_lattice_materialiser_tree_leaf-1_0", out var p0), Is.True);
            Assert.That(p0, Is.EqualTo(0));

            Assert.That(LatticeWalGc.TryParseConsumerPartition("_lattice_materialiser_tree_leaf-1_7", out var p7), Is.True);
            Assert.That(p7, Is.EqualTo(7));

            Assert.That(LatticeWalGc.TryParseConsumerPartition("_lattice_materialiser_tree_leaf-1", out _), Is.False,
                "The legacy single-partition shape carries no suffix and must be unattributable.");
            Assert.That(LatticeWalGc.TryParseConsumerPartition("_lattice_materialiser_tree_leaf-1_", out _), Is.False,
                "An empty suffix is not a partition.");
            Assert.That(LatticeWalGc.TryParseConsumerPartition("_lattice_materialiser_tree_leaf-1_07", out _), Is.False,
                "A leading zero is not a canonical partition suffix and is far likelier to be part of a grain id.");
            Assert.That(LatticeWalGc.TryParseConsumerPartition("_lattice_materialiser_tree_leaf-1_x", out _), Is.False);
            Assert.That(LatticeWalGc.TryParseConsumerPartition(string.Empty, out _), Is.False);
        });
    }
}
