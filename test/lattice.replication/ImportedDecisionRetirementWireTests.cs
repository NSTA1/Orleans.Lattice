using System.Collections.Immutable;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4524: the export's own boundary rides the snapshot trailer from the
/// sender service to the receiver's stream, additively on the wire, and the
/// receiver's retirement state round-trips under stable aliases.
/// </summary>
[TestFixture]
public class ImportedDecisionRetirementWireTests
{
    private const string Tree = "idr-wire-tree";
    private const string Source = "site-a";

    private static readonly CrossTreeSiblingBoundary Boundary = new()
    {
        PhysicalTreeId = "idr-physical",
        Tails = [3, 0, 7, 1],
        ExportEpoch = 2,
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
    public void The_new_wire_slots_and_aliases_are_additive_and_stable()
    {
        const BindingFlags Internal = BindingFlags.NonPublic | BindingFlags.Instance;
        Assert.Multiple(() =>
        {
            Assert.That(typeof(RemoteSnapshotStreamItem).GetProperty("ExportBoundary", Internal)!.GetCustomAttribute<IdAttribute>()!.Id, Is.EqualTo(4u));
            Assert.That(typeof(IImportedDecisionRetirementGrain).GetCustomAttribute<AliasAttribute>()?.Alias, Is.EqualTo("olr.ir"));
            Assert.That(typeof(ImportedDecisionRetirementState).GetCustomAttribute<AliasAttribute>()?.Alias, Is.EqualTo("olr.is"));
            Assert.That(typeof(ImportedDecisionSet).GetCustomAttribute<AliasAttribute>()?.Alias, Is.EqualTo("olr.ie"));
        });
    }

    [Test]
    public void A_trailer_carrying_only_the_export_boundary_is_a_trailer_and_survives_the_wire()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>();
        var trailer = new RemoteSnapshotStreamItem { ExportBoundary = Boundary };

        var back = serializer.Deserialize(serializer.SerializeToArray(trailer));

        Assert.Multiple(() =>
        {
            Assert.That(trailer.IsTrailer, Is.True);
            Assert.That(default(RemoteSnapshotStreamItem).IsTrailer, Is.False);
            Assert.That(back.ExportBoundary!.PhysicalTreeId, Is.EqualTo("idr-physical"));
            Assert.That(back.ExportBoundary.Tails, Is.EqualTo(new long[] { 3, 0, 7, 1 }));
        });
    }

    [Test]
    public void The_retirement_state_survives_the_wire()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<ImportedDecisionRetirementState>>();
        var txid = Guid.NewGuid();
        var state = new ImportedDecisionRetirementState();
        state.Sources[Source] = new ImportedDecisionSet { ExportBoundary = Boundary, TransactionIds = [txid] };
        state.Sources["legacy"] = new ImportedDecisionSet { TransactionIds = [Guid.NewGuid()] };

        var back = serializer.Deserialize(serializer.SerializeToArray(state));

        Assert.Multiple(() =>
        {
            Assert.That(back.Sources[Source].ExportBoundary!.PhysicalTreeId, Is.EqualTo(Boundary.PhysicalTreeId));
            Assert.That(back.Sources[Source].ExportBoundary!.Tails, Is.EqualTo(Boundary.Tails.ToArray()));
            Assert.That(back.Sources[Source].TransactionIds, Is.EquivalentTo(new[] { txid }));
            Assert.That(back.Sources["legacy"].ExportBoundary, Is.Null, "a source without a cut stays retained");
        });
    }

    [Test]
    public async Task The_receiver_provider_surfaces_the_export_boundary_after_the_drain()
    {
        var transport = Substitute.For<IRemoteSnapshotItemTransport>();
        transport.GetMetadataAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new RemoteSnapshotMetadata { TreeName = Tree, CausalStableFrontier = new VersionVector() }));
        transport.RequestSnapshotItemsAsync(Tree, Source, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(ItemsAsync(
                new RemoteSnapshotStreamItem { Entry = new SnapshotEntry { Key = "k1", Value = [1], Timestamp = HybridLogicalClock.Zero } },
                new RemoteSnapshotStreamItem { ExportBoundary = Boundary }));
        var provider = new RemoteSnapshotProvider(transport, NullLogger<RemoteSnapshotProvider>.Instance);

        var stream = await provider.ExportAsync(Tree, Source, HybridLogicalClock.Zero, CancellationToken.None);
        var beforeDrain = stream.ExportBoundary;
        var keys = new List<string>();
        await foreach (var entry in stream.Entries)
        {
            keys.Add(entry.Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(beforeDrain, Is.Null);
            Assert.That(stream.ExportBoundary, Is.SameAs(Boundary));
            Assert.That(keys, Is.EqualTo(new[] { "k1" }), "the trailer is not surfaced as an entry");
        });
    }

    [Test]
    public async Task A_sender_without_an_export_gate_emits_no_boundary_so_the_receiver_retains()
    {
        async IAsyncEnumerable<SnapshotEntry> Entries()
        {
            await Task.Yield();
            yield return new SnapshotEntry { Key = "k1", Value = [1], Timestamp = HybridLogicalClock.Zero };
        }

        var stream = new SnapshotStream(Tree, HybridLogicalClock.Zero, new VersionVector(), Entries());
        var provider = Substitute.For<ISnapshotProvider>();
        provider.ExportAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(stream));
        var service = new LatticeRemoteSnapshotService(
            provider, new StubReplicationContext(Source, LatticeMergeMode.LwwRegister), NullLogger<LatticeRemoteSnapshotService>.Instance);

        var items = new List<RemoteSnapshotStreamItem>();
        await foreach (var item in service.RequestSnapshotItemsAsync(Tree, Source, HybridLogicalClock.Zero))
        {
            items.Add(item);
        }

        Assert.That(items.Select(i => i.ExportBoundary), Is.All.Null);
    }
}
