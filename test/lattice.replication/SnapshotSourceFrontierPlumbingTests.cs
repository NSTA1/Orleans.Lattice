using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586 part 2b-2: the source's applied frontier rides a snapshot export
/// from source to receiver - the open read on the metadata, for a drop floor in
/// force before the drain; the close read on the trailer, for the pin - and the
/// receiver installs it only when the export's source generation held still
/// under the lineage it was read under.
/// </summary>
[TestFixture]
public class SnapshotSourceFrontierPlumbingTests
{
    private const string Tree = "ssf-tree";
    private const string Source = "site-a";
    private static readonly Guid Lineage = Guid.Parse("22222222-2222-2222-2222-222222222222");

    private static readonly SnapshotSourceGeneration Open = new()
    {
        PhysicalTreeId = "ssf-physical",
        ShardMapVersion = 3,
        Lineage = Lineage,
        DeleteEpoch = 0,
        IsDeleted = false,
    };

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static SnapshotSourceFrontier Frontier(long s, Guid? lineage = null) => new()
    {
        Lineage = lineage ?? Lineage,
        LowWatermarks = new Dictionary<string, HybridLogicalClock> { ["site-q"] = Hlc(s) },
        Held = new Dictionary<string, HybridLogicalClock[]> { ["site-q"] = [Hlc(s - 1)] },
    };

    private static async IAsyncEnumerable<RemoteSnapshotStreamItem> ItemsAsync(params RemoteSnapshotStreamItem[] items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }

    [Test]
    public void The_frontier_type_and_its_wire_slots_are_additive_and_stable()
    {
        var type = typeof(SnapshotSourceFrontier);
        var ids = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.GetCustomAttribute<IdAttribute>()?.Id)
            .Where(id => id is not null)
            .Select(id => (int)id!.Value)
            .OrderBy(id => id)
            .ToArray();
        const BindingFlags Internal = BindingFlags.NonPublic | BindingFlags.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(type.GetCustomAttribute<AliasAttribute>()?.Alias, Is.EqualTo("olr.sx"));
            Assert.That(type.GetCustomAttributes(typeof(ImmutableAttribute), false), Is.Not.Empty);
            Assert.That(ids, Is.EqualTo(new[] { 0, 1, 2 }));
            Assert.That(typeof(RemoteSnapshotMetadata).GetProperty("SourceFrontier", Internal)!.GetCustomAttribute<IdAttribute>()!.Id, Is.EqualTo(6u));
            Assert.That(typeof(RemoteSnapshotStreamItem).GetProperty("SourceFrontier", Internal)!.GetCustomAttribute<IdAttribute>()!.Id, Is.EqualTo(2u));
        });
    }

    [Test]
    public void The_frontier_survives_the_wire_on_the_metadata_and_the_trailer()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var metadataSerializer = services.GetRequiredService<Serializer<RemoteSnapshotMetadata>>();
        var itemSerializer = services.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>();
        var metadata = new RemoteSnapshotMetadata { TreeName = Tree, CausalStableFrontier = new VersionVector(), OpenGeneration = Open, SourceFrontier = Frontier(40) };
        var trailer = new RemoteSnapshotStreamItem { CloseGeneration = Open, SourceFrontier = Frontier(30) };

        var metadataBack = metadataSerializer.Deserialize(metadataSerializer.SerializeToArray(metadata));
        var trailerBack = itemSerializer.Deserialize(itemSerializer.SerializeToArray(trailer));

        Assert.Multiple(() =>
        {
            Assert.That(metadataBack.SourceFrontier!.LowWatermarks["site-q"], Is.EqualTo(Hlc(40)));
            Assert.That(metadataBack.SourceFrontier.Held["site-q"], Is.EqualTo(new[] { Hlc(39) }));
            Assert.That(metadataBack.SourceFrontier.Lineage, Is.EqualTo(Lineage));
            Assert.That(trailerBack.SourceFrontier!.LowWatermarks["site-q"], Is.EqualTo(Hlc(30)));
        });
    }

    [Test]
    public async Task The_sender_service_publishes_the_open_frontier_on_the_metadata_and_the_close_frontier_on_the_trailer()
    {
        SnapshotStream? stream = null;
        async IAsyncEnumerable<SnapshotEntry> Entries()
        {
            await Task.Yield();
            yield return new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Hlc(10) };
            stream!.CloseGeneration = Open;
            stream.SourceFrontier = Frontier(30);
        }

        stream = new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), Entries())
        {
            OpenGeneration = Open,
            OpenFrontier = Frontier(40),
        };
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(stream));
        var service = new LatticeRemoteSnapshotService(
            provider, new StubReplicationContext(Source, LatticeMergeMode.LwwRegister), NullLogger<LatticeRemoteSnapshotService>.Instance);

        var metadata = await service.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero);
        var items = new List<RemoteSnapshotStreamItem>();
        await foreach (var item in service.RequestSnapshotItemsAsync(Tree, Source, HybridLogicalClock.Zero))
        {
            items.Add(item);
        }

        Assert.Multiple(() =>
        {
            Assert.That(metadata.SourceFrontier!.LowWatermarks["site-q"], Is.EqualTo(Hlc(40)));
            Assert.That(items[^1].SourceFrontier!.LowWatermarks["site-q"], Is.EqualTo(Hlc(30)));
            Assert.That(items[0].SourceFrontier, Is.Null, "an entry item carries no frontier");
        });
    }

    [Test]
    public async Task The_receiver_provider_surfaces_the_open_frontier_at_once_and_the_close_frontier_after_the_drain()
    {
        var transport = Substitute.For<IRemoteSnapshotItemTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new RemoteSnapshotMetadata
            {
                TreeName = Tree,
                CausalStableFrontier = new VersionVector(),
                OpenGeneration = Open,
                SourceFrontier = Frontier(40),
            }));
        transport.RequestSnapshotItemsAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ItemsAsync(
                new RemoteSnapshotStreamItem { Entry = new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Hlc(10) } },
                new RemoteSnapshotStreamItem { CloseGeneration = Open, SourceFrontier = Frontier(30) }));
        var provider = new RemoteSnapshotProvider(transport, NullLogger<RemoteSnapshotProvider>.Instance);

        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None);
        var atOpen = stream.OpenFrontier;
        var closeBeforeDrain = stream.SourceFrontier;
        await foreach (var _ in stream.Entries)
        {
        }

        Assert.Multiple(() =>
        {
            Assert.That(atOpen!.LowWatermarks["site-q"], Is.EqualTo(Hlc(40)), "known before the first entry, for the drop floor");
            Assert.That(closeBeforeDrain, Is.Null);
            Assert.That(stream.SourceFrontier!.LowWatermarks["site-q"], Is.EqualTo(Hlc(30)));
        });
    }

    [Test]
    public void A_stable_export_installs_its_frontier()
    {
        var exported = Frontier(30);

        Assert.That(BootstrapFrontierInstall.Decide(Open, Open, exported), Is.SameAs(exported));
    }

    private static IEnumerable<TestCaseData> Unstable()
    {
        yield return new TestCaseData(Open, Open with { ShardMapVersion = 4 }).SetName("the shard map moved");
        yield return new TestCaseData(Open, Open with { PhysicalTreeId = "other" }).SetName("the physical tree moved");
        yield return new TestCaseData(Open, Open with { Lineage = Guid.NewGuid() }).SetName("the lineage moved");
        yield return new TestCaseData(Open, Open with { DeleteEpoch = 1 }).SetName("the delete epoch moved");
        yield return new TestCaseData(Open, Open with { IsDeleted = true }).SetName("deleted at close");
        yield return new TestCaseData(Open with { IsDeleted = true }, Open with { IsDeleted = true }).SetName("deleted throughout");
        yield return new TestCaseData(Open with { Lineage = null }, Open with { Lineage = null }).SetName("no lineage");
        yield return new TestCaseData(Open with { ShardMapVersion = null }, Open).SetName("unknown at open");
    }

    [TestCaseSource(nameof(Unstable))]
    public void An_export_whose_generation_did_not_hold_still_installs_nothing(SnapshotSourceGeneration open, SnapshotSourceGeneration close)
    {
        Assert.That(BootstrapFrontierInstall.Decide(open, close, Frontier(30)), Is.Null);
    }

    [Test]
    public void A_frontier_read_under_another_lineage_or_missing_generations_install_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BootstrapFrontierInstall.Decide(Open, Open, Frontier(30, Guid.NewGuid())), Is.Null);
            Assert.That(BootstrapFrontierInstall.Decide(null, Open, Frontier(30)), Is.Null);
            Assert.That(BootstrapFrontierInstall.Decide(Open, null, Frontier(30)), Is.Null);
            Assert.That(BootstrapFrontierInstall.Decide(Open, Open, null), Is.Null);
        });
    }
}
