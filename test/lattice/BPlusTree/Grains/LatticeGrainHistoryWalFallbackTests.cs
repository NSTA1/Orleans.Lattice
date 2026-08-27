using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Grain-level coverage for the best-effort write-ahead-log fallback of
/// <see cref="LatticeGrain.ScanEntryHistoryAsync"/> - the path taken when a tree
/// has not opted into a durable history view. Drives the grain directly with a
/// <see cref="FakeCommitLogReader"/> so the offset-ordered window, the honest
/// truncation report at the garbage-collection trim point, the per-mutation kind
/// mapping, and continuation paging are all asserted deterministically without a
/// real per-shard WAL.
/// </summary>
[TestFixture]
public sealed class LatticeGrainHistoryWalFallbackTests
{
    private const string TreeId = "hist-wal-fallback";

    private static LatticeGrain CreateGrain(IServiceProvider services)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 1 });

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, new LatticeOptions { WalPartitions = 1 });
        return new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }

    private static IServiceProvider ServicesWith(ICommitLogReader? reader)
    {
        var services = Substitute.For<IServiceProvider>();
        // No IViewCatalog -> no history view -> the read path falls back to the WAL.
        services.GetService(typeof(IViewCatalog)).Returns(null);
        services.GetService(typeof(ICommitLogReader)).Returns(reader);
        return services;
    }

    private static LatticeMutation Set(string key, byte[] value, long wall, string? origin = null) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = new HybridLogicalClock { WallClockTicks = wall },
        OriginClusterId = origin,
    };

    private static LatticeMutation CrdtDelta(string key, byte[] delta, long wall) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Delta = delta,
        Mode = LatticeMergeMode.PnCounter,
        Timestamp = new HybridLogicalClock { WallClockTicks = wall },
    };

    private static LatticeMutation Delete(string key, long wall) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Delete,
        Key = key,
        IsTombstone = true,
        Timestamp = new HybridLogicalClock { WallClockTicks = wall },
    };

    [Test]
    public async Task ScanEntryHistoryAsync_with_no_reader_registered_returns_empty_none_page()
    {
        var grain = CreateGrain(ServicesWith(reader: null));

        var page = await grain.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.None));
            Assert.That(page.Revisions, Is.Empty);
            Assert.That(page.Truncated, Is.False);
            Assert.That(page.Continuation, Is.Null);
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_wal_fallback_returns_only_the_keys_revisions_in_offset_order()
    {
        var reader = new FakeCommitLogReader();
        reader.Append(TreeId, 0, Set("k", new byte[] { 1 }, 10));
        reader.Append(TreeId, 0, Set("other", new byte[] { 9 }, 11));
        reader.Append(TreeId, 0, Set("k", new byte[] { 2 }, 12));
        var grain = CreateGrain(ServicesWith(reader));

        var page = await grain.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.WalWindow));
            Assert.That(page.Revisions, Has.Count.EqualTo(2));
            Assert.That(page.Revisions[0].Hlc.WallClockTicks, Is.EqualTo(10));
            Assert.That(page.Revisions[1].Hlc.WallClockTicks, Is.EqualTo(12));
            Assert.That(page.Revisions.All(r => r.SourceKey == "k"), Is.True);
            Assert.That(page.Truncated, Is.False, "nothing trimmed, so the window is the full history");
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_wal_fallback_reports_truncation_after_trim()
    {
        var reader = new FakeCommitLogReader();
        reader.Append(TreeId, 0, Set("k", new byte[] { 1 }, 10));
        reader.Append(TreeId, 0, Set("k", new byte[] { 2 }, 20));
        reader.Append(TreeId, 0, Set("k", new byte[] { 3 }, 30));
        reader.Append(TreeId, 0, Set("k", new byte[] { 4 }, 40));
        // Garbage collection has trimmed offsets [0, 2): the oldest readable entry
        // is now offset 2 (wall 30), so a partial window must be flagged.
        reader.TrimBefore(TreeId, 0, 2);
        var grain = CreateGrain(ServicesWith(reader));

        var page = await grain.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Truncated, Is.True);
            Assert.That(page.EarliestAvailable.WallClockTicks, Is.EqualTo(30),
                "earliest available is the oldest still-readable entry");
            Assert.That(page.Revisions, Has.Count.EqualTo(2));
            Assert.That(page.Revisions[0].Hlc.WallClockTicks, Is.EqualTo(30));
            Assert.That(page.Revisions[1].Hlc.WallClockTicks, Is.EqualTo(40));
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_wal_fallback_maps_delete_and_crdt_delta_kinds()
    {
        var reader = new FakeCommitLogReader();
        reader.Append(TreeId, 0, Set("k", new byte[] { 1 }, 10));
        reader.Append(TreeId, 0, CrdtDelta("k", new byte[] { 7, 7 }, 20));
        reader.Append(TreeId, 0, Delete("k", 30));
        var grain = CreateGrain(ServicesWith(reader));

        var page = await grain.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions, Has.Count.EqualTo(3));
            Assert.That(page.Revisions[0].Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(page.Revisions[0].ValuePreview, Is.EqualTo(new byte[] { 1 }));
            Assert.That(page.Revisions[1].Kind, Is.EqualTo(HistoryRowKind.CrdtDelta));
            Assert.That(page.Revisions[1].Delta, Is.EqualTo(new byte[] { 7, 7 }));
            Assert.That(page.Revisions[1].Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(page.Revisions[2].Kind, Is.EqualTo(HistoryRowKind.Delete));
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_wal_fallback_pages_through_continuation()
    {
        var reader = new FakeCommitLogReader();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(TreeId, 0, Set("k", new byte[] { (byte)i }, 10 + i));
        }

        var grain = CreateGrain(ServicesWith(reader));

        var collected = new List<long>();
        string? continuation = null;
        for (var guard = 0; guard < 10; guard++)
        {
            var page = await grain.ScanEntryHistoryAsync("k", null, null, 2, continuation);
            collected.AddRange(page.Revisions.Select(r => r.Hlc.WallClockTicks));
            continuation = page.Continuation;
            if (continuation is null)
            {
                break;
            }
        }

        Assert.That(collected, Is.EqualTo(new long[] { 10, 11, 12, 13, 14 }));
    }

    [Test]
    public async Task ScanEntryHistoryAsync_wal_fallback_honours_hlc_bounds()
    {
        var reader = new FakeCommitLogReader();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(TreeId, 0, Set("k", new byte[] { (byte)i }, 10 + i));
        }

        var grain = CreateGrain(ServicesWith(reader));

        var page = await grain.ScanEntryHistoryAsync(
            "k",
            new HybridLogicalClock { WallClockTicks = 11 },
            new HybridLogicalClock { WallClockTicks = 13 },
            100,
            null);

        Assert.That(page.Revisions.Select(r => r.Hlc.WallClockTicks), Is.EqualTo(new long[] { 11, 12, 13 }));
    }

    [Test]
    public void ScanEntryHistoryAsync_rejects_null_key()
    {
        var grain = CreateGrain(ServicesWith(reader: null));
        Assert.That(
            async () => await grain.ScanEntryHistoryAsync(null!, null, null, 10, null),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void ScanEntryHistoryAsync_rejects_nonpositive_limit()
    {
        var grain = CreateGrain(ServicesWith(reader: null));
        Assert.That(
            async () => await grain.ScanEntryHistoryAsync("k", null, null, 0, null),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ScanEntryHistoryAsync_rejects_reserved_system_tree_id()
    {
        var grain = CreateGrainForId(LatticeConstants.RegistryTreeId);
        Assert.That(
            async () => await grain.ScanEntryHistoryAsync("k", null, null, 10, null),
            Throws.TypeOf<LatticeReservedTreeNamespaceException>());
    }

    private static LatticeGrain CreateGrainForId(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        return new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }
}
