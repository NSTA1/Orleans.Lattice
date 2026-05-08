using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="ISnapshotProvider"/> public contract: the
/// silo's default registration is
/// <see cref="LatticeSnapshotProvider"/>, exporting a populated tree
/// returns a <see cref="SnapshotStream"/> whose
/// <see cref="SnapshotStream.TreeName"/> matches the request and whose
/// <see cref="SnapshotStream.Entries"/> drain to a non-empty list with
/// every <see cref="SnapshotEntry.Key"/> /
/// <see cref="SnapshotEntry.Value"/> /
/// <see cref="SnapshotEntry.Timestamp"/> populated, and exporting an
/// unpopulated tree returns an empty stream.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    private static async Task<List<SnapshotEntry>> DrainSnapshotAsync(SnapshotStream stream)
    {
        var collected = new List<SnapshotEntry>();
        await foreach (var entry in stream.Entries)
        {
            collected.Add(entry);
        }
        return collected;
    }

    [Test]
    public async Task ISnapshotProvider_export_yields_every_live_entry_with_stamped_hlc()
    {
        var treeId = NextTreeId("snap-export");
        var lattice = await CreateReplicatedTreeAsync(treeId);

        await lattice.SetAsync("a", Bytes("alpha"));
        await lattice.SetAsync("b", Bytes("beta"));
        await lattice.SetAsync("c", Bytes("gamma"));

        var provider = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ISnapshotProvider>();

        var stream = await provider.ExportAsync(treeId, HybridLogicalClock.Zero);
        var entries = await DrainSnapshotAsync(stream);

        Assert.Multiple(() =>
        {
            Assert.That(stream.TreeName, Is.EqualTo(treeId));
            Assert.That(stream.CausalStableFrontier, Is.Not.Null);
            Assert.That(entries, Has.Count.EqualTo(3));
            var byKey = entries.ToDictionary(e => e.Key, e => e);
            Assert.That(Str(byKey["a"].Value), Is.EqualTo("alpha"));
            Assert.That(Str(byKey["b"].Value), Is.EqualTo("beta"));
            Assert.That(Str(byKey["c"].Value), Is.EqualTo("gamma"));
            Assert.That(entries.All(e => e.Timestamp.CompareTo(HybridLogicalClock.Zero) > 0), Is.True);
        });
    }

    [Test]
    public async Task ISnapshotProvider_export_returns_empty_stream_for_unpopulated_tree()
    {
        var treeId = NextTreeId("snap-empty");
        await CreateReplicatedTreeAsync(treeId);

        var provider = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ISnapshotProvider>();

        var stream = await provider.ExportAsync(treeId, HybridLogicalClock.Zero);
        var entries = await DrainSnapshotAsync(stream);

        Assert.Multiple(() =>
        {
            Assert.That(stream.TreeName, Is.EqualTo(treeId));
            Assert.That(entries, Is.Empty);
        });
    }
}
