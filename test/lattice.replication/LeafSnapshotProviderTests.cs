using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Adapters;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers <see cref="LeafSnapshotProvider"/>, the default
/// <see cref="ILeafSnapshotProvider"/> that filters a whole-tree snapshot
/// export down to a single leaf's half-open key range and reads the head
/// offset through the commit-log reader.
/// </summary>
[TestFixture]
public class LeafSnapshotProviderTests
{
    private static SnapshotEntry Entry(string key, byte value)
        => new()
        {
            Key = key,
            Value = new[] { value },
            Timestamp = HybridLogicalClock.Zero,
        };

    private static async IAsyncEnumerable<SnapshotEntry> Stream(params SnapshotEntry[] entries)
    {
        foreach (var entry in entries)
        {
            yield return entry;
        }

        await Task.CompletedTask;
    }

    private static ISnapshotProvider SnapshotProviderYielding(params SnapshotEntry[] entries)
    {
        var provider = Substitute.For<ISnapshotProvider>();
        var stream = new SnapshotStream(
            "tree-1", HybridLogicalClock.Zero, new VersionVector(), Stream(entries));
        provider.ExportAsync("tree-1", HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(stream));
        return provider;
    }

    [Test]
    public async Task StreamAsync_yields_only_entries_inside_the_half_open_range()
    {
        var provider = SnapshotProviderYielding(
            Entry("a", 1), Entry("b", 2), Entry("c", 3), Entry("m", 4));
        var reader = Substitute.For<ICommitLogReader>();
        var adapter = new LeafSnapshotProvider(provider, reader);

        var keys = new List<string>();
        await foreach (var mutation in adapter.StreamAsync("tree-1", 0, "b", "m"))
        {
            keys.Add(mutation.Key);
        }

        // "a" is below the start; "m" is at/above the exclusive end.
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task StreamAsync_yields_to_end_of_tree_when_range_end_is_null()
    {
        var provider = SnapshotProviderYielding(
            Entry("a", 1), Entry("b", 2), Entry("z", 3));
        var reader = Substitute.For<ICommitLogReader>();
        var adapter = new LeafSnapshotProvider(provider, reader);

        var keys = new List<string>();
        await foreach (var mutation in adapter.StreamAsync("tree-1", 0, "b", null))
        {
            keys.Add(mutation.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "b", "z" }));
    }

    [Test]
    public async Task StreamAsync_projects_entry_as_a_set_mutation()
    {
        var provider = SnapshotProviderYielding(Entry("b", 7));
        var reader = Substitute.For<ICommitLogReader>();
        var adapter = new LeafSnapshotProvider(provider, reader);

        LatticeMutation? projected = null;
        await foreach (var mutation in adapter.StreamAsync("tree-1", 0, "a", null))
        {
            projected = mutation;
        }

        Assert.That(projected, Is.Not.Null);
        Assert.That(projected!.Value.Kind, Is.EqualTo(MutationKind.Set));
        Assert.That(projected.Value.TreeId, Is.EqualTo("tree-1"));
        Assert.That(projected.Value.Key, Is.EqualTo("b"));
        Assert.That(projected.Value.Value, Is.EqualTo(new byte[] { 7 }));
        Assert.That(projected.Value.IsTombstone, Is.False);
    }

    [Test]
    public void StreamAsync_throws_on_empty_treeId()
    {
        var adapter = new LeafSnapshotProvider(
            Substitute.For<ISnapshotProvider>(), Substitute.For<ICommitLogReader>());

        Assert.That(
            async () =>
            {
                await foreach (var _ in adapter.StreamAsync(string.Empty, 0, "a", null))
                {
                }
            },
            Throws.ArgumentException);
    }

    [Test]
    public void StreamAsync_throws_on_negative_shardIndex()
    {
        var adapter = new LeafSnapshotProvider(
            Substitute.For<ISnapshotProvider>(), Substitute.For<ICommitLogReader>());

        Assert.That(
            async () =>
            {
                await foreach (var _ in adapter.StreamAsync("tree-1", -1, "a", null))
                {
                }
            },
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void StreamAsync_throws_on_null_leafKeyRangeStart()
    {
        var adapter = new LeafSnapshotProvider(
            Substitute.For<ISnapshotProvider>(), Substitute.For<ICommitLogReader>());

        Assert.That(
            async () =>
            {
                await foreach (var _ in adapter.StreamAsync("tree-1", 0, null!, null))
                {
                }
            },
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetSnapshotOffsetAsync_delegates_to_the_commit_log_reader()
    {
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync("tree-1", 3, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(123L));
        var adapter = new LeafSnapshotProvider(Substitute.For<ISnapshotProvider>(), reader);

        var offset = await adapter.GetSnapshotOffsetAsync("tree-1", 3, CancellationToken.None);

        Assert.That(offset, Is.EqualTo(123L));
    }
}
