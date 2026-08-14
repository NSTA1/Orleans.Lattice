using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the durable leaf-materialiser <em>offset</em> floor in
/// <see cref="LatticeWalGc"/>. These reproduce the repocontext semantic-search
/// livelock: tombstone-compaction reap envelopes reuse the reaped entry's OLD
/// (low) HLC but are appended at HIGH WAL offsets, breaking the
/// HLC-monotonic-in-offset invariant the HLC trim floor relies on. A reap's low
/// HLC is <see cref="LatticeWalGc"/>-eligible under any positive cursor, so the
/// GC would trim it PAST a lagging leaf's applied checkpoint offset, tripping
/// the offset-space fall-off detector and wedging ingest. The offset floor makes
/// the GC never trim an entry at or above the lowest durably-applied leaf
/// checkpoint offset, so the low-HLC/high-offset reaps survive until the leaf
/// applies them.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcOffsetFloorTests
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

    // Offsets 0 and 1 carry rising HLCs (a healthy applied prefix ending at the
    // leaf's checkpoint offset 1 / HLC 20). Offsets 2 and 3 are tombstone-reap
    // envelopes: appended AFTER offset 1 but carrying the reaped entries' OLD
    // low HLCs (5, 6) - lower than the checkpoint HLC yet at higher offsets.
    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(5)), Entry(3, Hlc(6)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
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

    private static async Task<List<long>> SurvivingOffsetsAsync(IWalStorageProvider provider)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }
        return survivors;
    }

    [Test]
    public async Task RunOnceAsync_offset_floor_retains_low_hlc_reaps_above_leaf_checkpoint()
    {
        // The forward consumer (shipper) is at the WAL head; the leaf's durable
        // pin is at its checkpoint (HLC 20, offset 1). The reaps at offsets 2/3
        // carry HLCs 5/6 - BELOW the HLC floor (20) - so without the offset
        // floor the GC would trim them along with the applied prefix, dropping
        // committed WAL the lagging leaf has not yet applied and tripping the
        // offset-space fall-off detector.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
            "The offset floor stops the trim just above the durable leaf checkpoint offset (the applied prefix 0..1 is trimmed).");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 2L, 3L }),
            "The low-HLC/high-offset reap envelopes above the leaf checkpoint must survive.");
    }

    [Test]
    public async Task RunOnceAsync_without_offset_floor_trims_low_hlc_reaps_the_pre_fix_bug()
    {
        // Control: the SAME WAL and HLC pins, but the pin store reports NO
        // offsets (as an old pin grain would during a rolling upgrade, or the
        // pre-fix build). The HLC floor alone trims every entry whose HLC is at
        // or below the floor (20) - including the reaps at offsets 2/3 - which
        // is exactly the fall-off-inducing over-trim the offset floor prevents.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets: null), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "Without the offset floor the HLC-eligible reaps are trimmed - the reproduced bug.");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.Empty);
    }
}
