using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the core <see cref="InMemoryWalStorageProvider"/>
/// after the type was promoted from the replication package. Pins the
/// within-batch dense-offset append contract, the no-overlap invariant
/// against the log as a whole (out-of-order batch arrival is permitted
/// to support multi-in-flight flushes), snapshot-then-yield read,
/// recovery, and trim semantics on the LatticeMutation-shaped WalEntry.
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
    public async Task AppendBatchAsync_accepts_non_zero_first_offset_on_empty_shard()
    {
        // Under WalMaxPendingBatches > 1 the WAL grain assigns offsets
        // on the grain turn but flush completion can arrive in any order
        // against the provider. The provider therefore no longer requires
        // a batch to start at `currentHighest + 1`; it only requires no
        // overlap with persisted offsets. An empty shard accepting a
        // batch that starts at offset 5 is the canonical example of the
        // earlier-finishing second flush landing before the first.
        var sut = new InMemoryWalStorageProvider();
        var batch = new[] { Entry(5), Entry(6) };

        await sut.AppendBatchAsync(Tree, 0, batch, CancellationToken.None);

        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(6L));
    }

    [Test]
    public async Task AppendBatchAsync_rejects_batch_overlapping_existing_offsets()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        // Overlap with offset 2 must be rejected even when the batch is
        // otherwise dense within itself.
        Assert.That(
            async () => await sut.AppendBatchAsync(Tree, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None),
            Throws.InvalidOperationException);

        // Validation runs ahead of mutation - the head stays at 2.
        var head = await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(2L));
    }

    [Test]
    public async Task AppendBatchAsync_accepts_out_of_order_batch_arrival()
    {
        // The grain assigns offsets [0..1] to flush A and [2..3] to
        // flush B; under multi-in-flight flushes B can land at the
        // provider first. The provider must accept B and then accept A
        // into the gap below, preserving offset order in storage.
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        await sut.AppendBatchAsync(Tree, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        var collected = new List<WalEntry>();
        await foreach (var w in sut.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 10, CancellationToken.None))
        {
            collected.Add(w);
        }

        Assert.Multiple(() =>
        {
            Assert.That(collected.Count, Is.EqualTo(4));
            Assert.That(collected[0].Offset, Is.EqualTo(0L));
            Assert.That(collected[1].Offset, Is.EqualTo(1L));
            Assert.That(collected[2].Offset, Is.EqualTo(2L));
            Assert.That(collected[3].Offset, Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task AppendBatchAsync_rejects_gap_inside_batch()
    {
        var sut = new InMemoryWalStorageProvider();
        var batch = new[] { Entry(0), Entry(2) };

        Assert.That(
            async () => await sut.AppendBatchAsync(Tree, 0, batch, CancellationToken.None),
            Throws.InvalidOperationException);

        // Validation runs ahead of mutation - no entry from the rejected
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
