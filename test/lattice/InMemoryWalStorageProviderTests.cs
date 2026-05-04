using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the core <see cref="InMemoryWalStorageProvider"/>
/// after the type was promoted from the replication package. Pins the
/// dense-offset append, snapshot-then-yield read, recovery, and trim
/// semantics on the LatticeMutation-shaped WalEntry.
/// </summary>
[TestFixture]
public class InMemoryWalStorageProviderTests
{
    private const string Tree = "tree";

    private static WalEntry Entry(long offset, string key = "k") => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    [Test]
    public async Task GetHighestOffsetAsync_returns_minus_one_for_empty_shard()
    {
        var sut = new InMemoryWalStorageProvider();

        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);

        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendBatchAsync_persists_entries_with_dense_offsets()
    {
        var sut = new InMemoryWalStorageProvider();
        var batch = new[] { Entry(0), Entry(1), Entry(2) };

        await sut.AppendBatchAsync(Tree, 0, batch, CancellationToken.None);

        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(2L));
    }

    [Test]
    public void AppendBatchAsync_rejects_non_dense_first_offset()
    {
        var sut = new InMemoryWalStorageProvider();
        var batch = new[] { Entry(5) };

        Assert.That(
            async () => await sut.AppendBatchAsync(Tree, 0, batch, CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task AppendBatchAsync_rejects_gap_inside_batch()
    {
        var sut = new InMemoryWalStorageProvider();
        var batch = new[] { Entry(0), Entry(2) };

        Assert.That(
            async () => await sut.AppendBatchAsync(Tree, 0, batch, CancellationToken.None),
            Throws.InvalidOperationException);

        // Validation runs ahead of mutation — no entry from the rejected
        // batch survives. Head must still report -1.
        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendBatchAsync_empty_list_is_a_noop()
    {
        var sut = new InMemoryWalStorageProvider();

        await sut.AppendBatchAsync(Tree, 0, Array.Empty<WalEntry>(), CancellationToken.None);

        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public void AppendBatchAsync_throws_on_null_treeId()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.AppendBatchAsync(null!, 0, Array.Empty<WalEntry>(), CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AppendBatchAsync_throws_on_null_entries()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.AppendBatchAsync(Tree, 0, null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ReadAsync_yields_entries_in_offset_order_from_exclusive_lower_bound()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0, "a"), Entry(1, "b"), Entry(2, "c") }, CancellationToken.None);

        var collected = new List<WalEntry>();
        await foreach (var w in sut.ReadAsync(Tree, 0, fromOffsetExclusive: 0, maxEntries: 10, CancellationToken.None))
        {
            collected.Add(w);
        }

        Assert.Multiple(() =>
        {
            Assert.That(collected.Count, Is.EqualTo(2));
            Assert.That(collected[0].Offset, Is.EqualTo(1L));
            Assert.That(collected[1].Offset, Is.EqualTo(2L));
            Assert.That(collected[0].Mutation.Key, Is.EqualTo("b"));
            Assert.That(collected[1].Mutation.Key, Is.EqualTo("c"));
        });
    }

    [Test]
    public async Task ReadAsync_caps_returned_entries_at_max_entries()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var collected = new List<WalEntry>();
        await foreach (var w in sut.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 2, CancellationToken.None))
        {
            collected.Add(w);
        }

        Assert.That(collected.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task ReadAsync_yields_nothing_for_unknown_shard()
    {
        var sut = new InMemoryWalStorageProvider();

        var collected = new List<WalEntry>();
        await foreach (var w in sut.ReadAsync(Tree, 99, fromOffsetExclusive: -1, maxEntries: 10, CancellationToken.None))
        {
            collected.Add(w);
        }

        Assert.That(collected, Is.Empty);
    }

    [Test]
    public void ReadAsync_throws_on_zero_max_entries()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync(Tree, 0, -1, 0, CancellationToken.None))
                {
                }
            },
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task TrimAsync_removes_entries_through_the_supplied_offset()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        await sut.TrimAsync(Tree, 0, throughOffsetInclusive: 1, CancellationToken.None);

        var survivors = new List<WalEntry>();
        await foreach (var w in sut.ReadAsync(Tree, 0, -1, 10, CancellationToken.None))
        {
            survivors.Add(w);
        }

        Assert.Multiple(() =>
        {
            Assert.That(survivors.Count, Is.EqualTo(2));
            Assert.That(survivors[0].Offset, Is.EqualTo(2L));
            Assert.That(survivors[1].Offset, Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task TrimAsync_is_idempotent()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        await sut.TrimAsync(Tree, 0, 0, CancellationToken.None);
        await sut.TrimAsync(Tree, 0, 0, CancellationToken.None);

        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(1L));
    }

    [Test]
    public async Task TrimAsync_unknown_shard_is_a_noop()
    {
        var sut = new InMemoryWalStorageProvider();

        await sut.TrimAsync(Tree, 0, 100, CancellationToken.None);

        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public void Operations_throw_on_pre_cancelled_token()
    {
        var sut = new InMemoryWalStorageProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0) }, cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(
                async () => await sut.GetHighestOffsetAsync(Tree, 0, cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(
                async () => await sut.TrimAsync(Tree, 0, 0, cts.Token),
                Throws.InstanceOf<OperationCanceledException>());
        });
    }

    [Test]
    public async Task Different_shards_are_isolated()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0, "shard0") }, CancellationToken.None);
        await sut.AppendBatchAsync(Tree, 1, new[] { Entry(0, "shard1") }, CancellationToken.None);

        var head0 = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        var head1 = await sut.GetHighestOffsetAsync(Tree, 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(head0, Is.EqualTo(0L));
            Assert.That(head1, Is.EqualTo(0L));
        });
    }
}
