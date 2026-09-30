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
    public async Task StreamAsync_carries_the_committed_row_expiry()
    {
        var expiring = Entry("b", 7) with { ExpiresAtTicks = 638_000_000_000_000_000 };
        var adapter = new LeafSnapshotProvider(
            SnapshotProviderYielding(expiring), Substitute.For<ICommitLogReader>());

        var projected = await SingleAsync(adapter);

        // A dropped expiry rebuilt a TTL row as durable, so it outlived its lease.
        Assert.That(projected.ExpiresAtTicks, Is.EqualTo(expiring.ExpiresAtTicks));
        Assert.That(projected.IsPrepared, Is.False);
        Assert.That(projected.TransactionId, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task StreamAsync_projects_a_prepared_set_as_a_prepared_mutation_not_a_committed_value()
    {
        var txId = Guid.NewGuid();
        var prepared = Entry("b", 9) with
        {
            IsPrepared = true,
            TransactionId = txId,
            AtomicBatchSize = 3,
            AtomicBatchIndex = 1,
            ExpiresAtTicks = 42,
            Delta = new byte[] { 5, 6 },
            Mode = LatticeMergeMode.GCounter,
        };
        var adapter = new LeafSnapshotProvider(
            SnapshotProviderYielding(prepared), Substitute.For<ICommitLogReader>());

        var projected = await SingleAsync(adapter);

        // Surfacing an in-flight saga's prepare as a plain Set would publish a
        // value the saga may yet abort.
        Assert.That(projected.IsPrepared, Is.True);
        Assert.That(projected.Kind, Is.EqualTo(MutationKind.Set));
        Assert.That(projected.TransactionId, Is.EqualTo(txId));
        Assert.That(projected.AtomicBatchSize, Is.EqualTo(3));
        Assert.That(projected.AtomicBatchIndex, Is.EqualTo(1));
        Assert.That(projected.Value, Is.EqualTo(new byte[] { 9 }));
        Assert.That(projected.ExpiresAtTicks, Is.EqualTo(42));
        Assert.That(projected.Delta, Is.EqualTo(new byte[] { 5, 6 }));
        Assert.That(projected.Mode, Is.EqualTo(LatticeMergeMode.GCounter));
    }

    [Test]
    public async Task StreamAsync_projects_a_prepared_delete_as_a_prepared_tombstone()
    {
        var txId = Guid.NewGuid();
        var prepared = Entry("b", 9) with
        {
            IsPrepared = true,
            IsTombstone = true,
            TransactionId = txId,
            ExpiresAtTicks = 42,
        };
        var adapter = new LeafSnapshotProvider(
            SnapshotProviderYielding(prepared), Substitute.For<ICommitLogReader>());

        var projected = await SingleAsync(adapter);

        // A prepared delete's value slot is ignored on the wire; projecting it as
        // a Set resurrected that slot as a live value for a key being deleted.
        Assert.That(projected.IsPrepared, Is.True);
        Assert.That(projected.Kind, Is.EqualTo(MutationKind.Delete));
        Assert.That(projected.IsTombstone, Is.True);
        Assert.That(projected.Value, Is.Null);
        Assert.That(projected.ExpiresAtTicks, Is.Zero);
        Assert.That(projected.TransactionId, Is.EqualTo(txId));
    }

    private static async Task<LatticeMutation> SingleAsync(LeafSnapshotProvider adapter)
    {
        var projected = new List<LatticeMutation>();
        await foreach (var mutation in adapter.StreamAsync("tree-1", 0, "a", null))
        {
            projected.Add(mutation);
        }

        Assert.That(projected, Has.Count.EqualTo(1));
        return projected[0];
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
