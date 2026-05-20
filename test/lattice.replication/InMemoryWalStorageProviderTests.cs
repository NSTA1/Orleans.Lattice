using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class InMemoryWalStorageProviderTests
{
    private static WalEntry MakeEntry(long offset, string key = "k") => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    [Test]
    public async Task GetHighestOffsetAsync_returns_minus_one_when_shard_empty()
    {
        var sut = new InMemoryWalStorageProvider();

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);

        Assert.That(highest, Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_when_shard_empty()
    {
        var sut = new InMemoryWalStorageProvider();

        var lowest = await sut.GetLowestOffsetAsync("tree", 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_zero_on_untrimmed_shard()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2) }, CancellationToken.None);

        var lowest = await sut.GetLowestOffsetAsync("tree", 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_first_surviving_offset_after_partial_trim()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2), MakeEntry(3) }, CancellationToken.None);

        await sut.TrimAsync("tree", 0, throughOffsetInclusive: 1, CancellationToken.None);

        var lowest = await sut.GetLowestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(lowest, Is.EqualTo(2L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_after_full_trim()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None);

        await sut.TrimAsync("tree", 0, throughOffsetInclusive: 1, CancellationToken.None);

        var lowest = await sut.GetLowestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(lowest, Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendBatchAsync_persists_dense_offsets_and_advances_highest()
    {
        var sut = new InMemoryWalStorageProvider();
        var batch = new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2) };

        await sut.AppendBatchAsync("tree", 0, batch, CancellationToken.None);

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(2L));
    }

    [Test]
    public async Task AppendBatchAsync_chained_batches_preserve_offset_density()
    {
        var sut = new InMemoryWalStorageProvider();

        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None);
        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(2) }, CancellationToken.None);

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(2L));
    }

    [Test]
    public async Task AppendBatchAsync_accepts_non_zero_first_offset_on_empty_shard()
    {
        // With LatticeOptions.WalMaxPendingBatches > 1, the
        // grain assigns offsets serially on its turn but flush
        // completion can arrive at the provider out of order. The
        // provider therefore no longer requires the first batch to
        // start at offset 0; it only requires within-batch density and
        // no overlap with persisted offsets.
        var sut = new InMemoryWalStorageProvider();

        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(5) }, CancellationToken.None);

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(5L));
    }

    [Test]
    public void AppendBatchAsync_throws_on_overlap_with_persisted_offsets()
    {
        var sut = new InMemoryWalStorageProvider();
        sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None).GetAwaiter().GetResult();

        Assert.That(
            async () => await sut.AppendBatchAsync(
                "tree", 0, new[] { MakeEntry(1), MakeEntry(2) }, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void AppendBatchAsync_throws_on_gap_inside_batch()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.AppendBatchAsync(
                "tree", 0, new[] { MakeEntry(0), MakeEntry(2) }, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task AppendBatchAsync_failed_batch_does_not_mutate_state()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None);

        // A within-batch gap is rejected ahead of any mutation; the
        // head must not advance past offset 1.
        Assert.That(
            async () => await sut.AppendBatchAsync(
                "tree", 0, new[] { MakeEntry(2), MakeEntry(4) }, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(1L));
    }

    [Test]
    public async Task AppendBatchAsync_empty_batch_is_a_no_op()
    {
        var sut = new InMemoryWalStorageProvider();

        await sut.AppendBatchAsync("tree", 0, Array.Empty<WalEntry>(), CancellationToken.None);

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(-1L));
    }

    [Test]
    public void AppendBatchAsync_throws_on_null_tree_id()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.AppendBatchAsync(
                null!, 0, new[] { MakeEntry(0) }, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AppendBatchAsync_throws_on_null_entries()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.AppendBatchAsync("tree", 0, null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AppendBatchAsync_observes_cancellation()
    {
        var sut = new InMemoryWalStorageProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await sut.AppendBatchAsync(
                "tree", 0, new[] { MakeEntry(0) }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task ReadAsync_returns_empty_for_unknown_shard()
    {
        var sut = new InMemoryWalStorageProvider();
        var emitted = new List<WalEntry>();

        await foreach (var entry in sut.ReadAsync("tree", 0, -1, 100, CancellationToken.None))
        {
            emitted.Add(entry);
        }

        Assert.That(emitted, Is.Empty);
    }

    [Test]
    public async Task ReadAsync_yields_entries_strictly_above_from_offset()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(
            "tree", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2) }, CancellationToken.None);

        var emitted = new List<long>();
        await foreach (var entry in sut.ReadAsync("tree", 0, 0, 100, CancellationToken.None))
        {
            emitted.Add(entry.Offset);
        }

        Assert.That(emitted, Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public async Task ReadAsync_includes_offset_zero_when_from_offset_is_minus_one()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync("tree", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None);

        var emitted = new List<long>();
        await foreach (var entry in sut.ReadAsync("tree", 0, -1, 100, CancellationToken.None))
        {
            emitted.Add(entry.Offset);
        }

        Assert.That(emitted, Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task ReadAsync_caps_at_max_entries()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(
            "tree", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2), MakeEntry(3) }, CancellationToken.None);

        var emitted = new List<long>();
        await foreach (var entry in sut.ReadAsync("tree", 0, -1, 2, CancellationToken.None))
        {
            emitted.Add(entry.Offset);
        }

        Assert.That(emitted, Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public void ReadAsync_throws_on_non_positive_max_entries()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync("tree", 0, -1, 0, CancellationToken.None))
                {
                }
            },
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReadAsync_throws_on_null_tree_id()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync(null!, 0, -1, 1, CancellationToken.None))
                {
                }
            },
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetHighestOffsetAsync_throws_on_null_tree_id()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.GetHighestOffsetAsync(null!, 0, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetHighestOffsetAsync_observes_cancellation()
    {
        var sut = new InMemoryWalStorageProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await sut.GetHighestOffsetAsync("tree", 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task TrimAsync_removes_entries_through_inclusive_offset()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(
            "tree", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2), MakeEntry(3) }, CancellationToken.None);

        await sut.TrimAsync("tree", 0, throughOffsetInclusive: 1, CancellationToken.None);

        var emitted = new List<long>();
        await foreach (var entry in sut.ReadAsync("tree", 0, -1, 100, CancellationToken.None))
        {
            emitted.Add(entry.Offset);
        }

        Assert.That(emitted, Is.EqualTo(new[] { 2L, 3L }));
    }

    [Test]
    public async Task TrimAsync_preserves_highest_offset()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(
            "tree", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2) }, CancellationToken.None);

        await sut.TrimAsync("tree", 0, throughOffsetInclusive: 1, CancellationToken.None);

        var highest = await sut.GetHighestOffsetAsync("tree", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(2L));
    }

    [Test]
    public async Task TrimAsync_is_idempotent()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(
            "tree", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None);

        await sut.TrimAsync("tree", 0, 0, CancellationToken.None);
        await sut.TrimAsync("tree", 0, 0, CancellationToken.None);

        var emitted = new List<long>();
        await foreach (var entry in sut.ReadAsync("tree", 0, -1, 100, CancellationToken.None))
        {
            emitted.Add(entry.Offset);
        }
        Assert.That(emitted, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public async Task TrimAsync_no_op_for_unknown_shard()
    {
        var sut = new InMemoryWalStorageProvider();

        await sut.TrimAsync("unknown", 0, 100, CancellationToken.None);

        var highest = await sut.GetHighestOffsetAsync("unknown", 0, CancellationToken.None);
        Assert.That(highest, Is.EqualTo(-1L));
    }

    [Test]
    public void TrimAsync_throws_on_null_tree_id()
    {
        var sut = new InMemoryWalStorageProvider();

        Assert.That(
            async () => await sut.TrimAsync(null!, 0, 0, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void TrimAsync_observes_cancellation()
    {
        var sut = new InMemoryWalStorageProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await sut.TrimAsync("tree", 0, 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Shards_are_isolated_by_tree_id_and_index()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync("tree-a", 0, new[] { MakeEntry(0), MakeEntry(1) }, CancellationToken.None);
        await sut.AppendBatchAsync("tree-a", 1, new[] { MakeEntry(0) }, CancellationToken.None);
        await sut.AppendBatchAsync("tree-b", 0, new[] { MakeEntry(0), MakeEntry(1), MakeEntry(2) }, CancellationToken.None);

        var ha0 = await sut.GetHighestOffsetAsync("tree-a", 0, CancellationToken.None);
        var ha1 = await sut.GetHighestOffsetAsync("tree-a", 1, CancellationToken.None);
        var hb0 = await sut.GetHighestOffsetAsync("tree-b", 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ha0, Is.EqualTo(1L));
            Assert.That(ha1, Is.EqualTo(0L));
            Assert.That(hb0, Is.EqualTo(2L));
        });
    }
}
