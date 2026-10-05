using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The source generation captured around a snapshot export (issue #4537) must
/// reach the receiver's bootstrap coordinator intact: the opening value rides
/// <see cref="RemoteSnapshotMetadata.OpenGeneration"/>, the closing value is a
/// trailer item after the entry stream, and a receiver that never sees either
/// treats the generation as unknown and skips the delete reconcile.
/// </summary>
[TestFixture]
public class SnapshotSourceGenerationPlumbingTests
{
    private const string Tree = "ssg-tree";
    private const string Source = "site-a";

    private static readonly SnapshotSourceGeneration Open = new()
    {
        PhysicalTreeId = "ssg-physical",
        ShardMapVersion = 3,
        Lineage = Guid.Parse("11111111-1111-1111-1111-111111111111"),
        DeleteEpoch = 0,
        IsDeleted = false,
    };

    private static readonly SnapshotSourceGeneration Close = Open with { ShardMapVersion = 4 };

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static async IAsyncEnumerable<SnapshotEntry> AsAsync(params SnapshotEntry[] entries)
    {
        foreach (var entry in entries)
        {
            await Task.Yield();
            yield return entry;
        }
    }

    private static async IAsyncEnumerable<RemoteSnapshotStreamItem> ItemsAsync(params RemoteSnapshotStreamItem[] items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }

    [Test]
    public void SnapshotSourceGeneration_carries_its_stable_wire_attributes_and_sequential_ids()
    {
        var type = typeof(SnapshotSourceGeneration);
        var ids = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.GetCustomAttribute<IdAttribute>()?.Id)
            .Where(id => id is not null)
            .Select(id => (int)id!.Value)
            .OrderBy(id => id)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(type.GetCustomAttributes(typeof(GenerateSerializerAttribute), false), Is.Not.Empty);
            Assert.That(type.GetCustomAttributes(typeof(ImmutableAttribute), false), Is.Not.Empty);
            Assert.That(type.GetCustomAttribute<AliasAttribute>()?.Alias, Is.EqualTo("olr.sg"));
            Assert.That(ids, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
        });
    }

    [Test]
    public void SnapshotSourceGeneration_equality_is_structural_and_defaults_to_unknown()
    {
        var copy = Open with { };
        var unknown = default(SnapshotSourceGeneration);

        Assert.Multiple(() =>
        {
            Assert.That(copy, Is.EqualTo(Open));
            Assert.That(copy.GetHashCode(), Is.EqualTo(Open.GetHashCode()));
            Assert.That(Close, Is.Not.EqualTo(Open));
            Assert.That(unknown.PhysicalTreeId, Is.Null);
            Assert.That(unknown.ShardMapVersion, Is.Null);
            Assert.That(unknown.Lineage, Is.Null);
            Assert.That(unknown.DeleteEpoch, Is.Null);
            Assert.That(unknown.IsDeleted, Is.Null);
        });
    }

    [Test]
    public void Wire_slots_for_the_generation_are_additive()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(RemoteSnapshotMetadata).GetProperty(nameof(RemoteSnapshotMetadata.OpenGeneration))!
                .GetCustomAttribute<IdAttribute>()!.Id, Is.EqualTo(5u));
            Assert.That(typeof(RemoteSnapshotStreamItem).GetProperty(nameof(RemoteSnapshotStreamItem.CloseGeneration))!
                .GetCustomAttribute<IdAttribute>()!.Id, Is.EqualTo(1u));
            Assert.That(default(RemoteSnapshotMetadata).OpenGeneration, Is.Null, "an older sender decodes to unknown");
            Assert.That(default(RemoteSnapshotStreamItem).CloseGeneration, Is.Null, "an entry item is not a trailer");
        });
    }

    [Test]
    public async Task Sender_service_publishes_the_open_generation_and_ends_the_items_with_the_close_trailer()
    {
        SnapshotStream? stream = null;
        async IAsyncEnumerable<SnapshotEntry> Entries()
        {
            await Task.Yield();
            yield return new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Hlc(10) };
            stream!.CloseGeneration = Close;
        }

        stream = new SnapshotStream(Tree, Hlc(100), new VersionVector(), Entries()) { OpenGeneration = Open };
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));
        var service = new LatticeRemoteSnapshotService(
            provider,
            new StubReplicationContext(Source, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var metadata = await service.GetMetadataAsync(Tree, Source, HybridLogicalClock.Zero);
        var items = new List<RemoteSnapshotStreamItem>();
        await foreach (var item in service.RequestSnapshotItemsAsync(Tree, Source, HybridLogicalClock.Zero))
        {
            items.Add(item);
        }

        Assert.Multiple(() =>
        {
            Assert.That(metadata.OpenGeneration, Is.EqualTo(Open));
            Assert.That(items, Has.Count.EqualTo(2));
            Assert.That(items[0].Entry.Key, Is.EqualTo("k1"));
            Assert.That(items[0].CloseGeneration, Is.Null);
            Assert.That(items[1].CloseGeneration, Is.EqualTo(Close), "the trailer carries the generation captured after the stream");
        });
    }

    [Test]
    public async Task Sender_service_omits_the_trailer_when_no_close_generation_was_captured()
    {
        var stream = new SnapshotStream(Tree, Hlc(100), new VersionVector(),
            AsAsync(new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Hlc(10) }));
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));
        var service = new LatticeRemoteSnapshotService(
            provider,
            new StubReplicationContext(Source, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var items = new List<RemoteSnapshotStreamItem>();
        await foreach (var item in service.RequestSnapshotItemsAsync(Tree, Source, HybridLogicalClock.Zero))
        {
            items.Add(item);
        }

        Assert.That(items.Select(i => i.CloseGeneration), Is.All.Null);
        Assert.That(items, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Receiver_provider_surfaces_the_open_generation_and_takes_the_close_from_the_trailer()
    {
        var transport = Substitute.For<IRemoteSnapshotItemTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new RemoteSnapshotMetadata
            {
                TreeName = Tree,
                AsOfHlc = Hlc(100),
                CausalStableFrontier = new VersionVector(),
                OpenGeneration = Open,
            }));
        transport.RequestSnapshotItemsAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ItemsAsync(
                new RemoteSnapshotStreamItem { Entry = new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Hlc(10) } },
                new RemoteSnapshotStreamItem { CloseGeneration = Close }));
        var provider = new RemoteSnapshotProvider(transport, NullLogger<RemoteSnapshotProvider>.Instance);

        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None);
        Assert.That(stream.CloseGeneration, Is.Null, "the close generation is unknown until the stream is drained");
        var keys = new List<string>();
        await foreach (var entry in stream.Entries)
        {
            keys.Add(entry.Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(stream.OpenGeneration, Is.EqualTo(Open));
            Assert.That(keys, Is.EqualTo(new[] { "k1" }), "the trailer is not a snapshot entry");
            Assert.That(stream.CloseGeneration, Is.EqualTo(Close));
        });
    }

    [Test]
    public async Task Receiver_provider_over_a_legacy_transport_leaves_both_generations_unknown()
    {
        var transport = Substitute.For<IRemoteSnapshotTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new RemoteSnapshotMetadata
            {
                TreeName = Tree,
                AsOfHlc = Hlc(100),
                CausalStableFrontier = new VersionVector(),
            }));
        transport.RequestSnapshotAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(AsAsync(new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = Hlc(10) }));
        var provider = new RemoteSnapshotProvider(transport, NullLogger<RemoteSnapshotProvider>.Instance);

        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None);
        await foreach (var _ in stream.Entries)
        {
        }

        Assert.Multiple(() =>
        {
            Assert.That(stream.OpenGeneration, Is.Null);
            Assert.That(stream.CloseGeneration, Is.Null, "a legacy sender is unknown, which skips the reconcile fail-safe");
        });
    }
}
