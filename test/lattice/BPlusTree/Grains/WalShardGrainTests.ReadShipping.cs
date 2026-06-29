using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [Test]
    public async Task ReadShippingAsync_returns_empty_page_when_wal_is_empty()
    {
        var grain = await CreateGrainAsync();

        var page = await grain.ReadShippingAsync(0L, 10, CancellationToken.None);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(0L));
    }

    [Test]
    public async Task ReadShippingAsync_negative_fromSequence_throws()
    {
        var grain = await CreateGrainAsync();

        Assert.That(
            async () => await grain.ReadShippingAsync(-1L, 1, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadShippingAsync_zero_maxEntries_throws()
    {
        var grain = await CreateGrainAsync();

        Assert.That(
            async () => await grain.ReadShippingAsync(0L, 0, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadShippingAsync_returns_entries_in_ascending_sequence_order()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var page = await grain.ReadShippingAsync(0L, 10, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(3));
        Assert.That(page.Entries[0].Sequence, Is.EqualTo(0L));
        Assert.That(page.Entries[1].Sequence, Is.EqualTo(1L));
        Assert.That(page.Entries[2].Sequence, Is.EqualTo(2L));
        Assert.That(page.NextSequence, Is.EqualTo(3L));
    }

    [Test]
    public async Task ReadShippingAsync_caps_returned_entries_to_maxEntries()
    {
        var grain = await CreateGrainAsync();
        for (var i = 0; i < 5; i++)
        {
            await grain.AppendAsync(MakeEntry($"k{i}"), CancellationToken.None);
        }

        var page = await grain.ReadShippingAsync(0L, 2, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(2));
        Assert.That(page.NextSequence, Is.EqualTo(2L));
    }

    [Test]
    public async Task ReadShippingAsync_emits_payloads_that_decode_to_the_appended_records()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new OrleansBinaryWalRecordEncoder(services.GetRequiredService<Serializer<WalRecord>>());
        var grain = await CreateGrainAsync(encoder: encoder);

        var entry = MakeEntry("k1", new byte[] { 7, 8, 9 });
        await grain.AppendAsync(entry, CancellationToken.None);

        var page = await grain.ReadShippingAsync(0L, 10, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(1));
        var decoded = encoder.Decode(page.Entries[0].EncodedPayload);
        Assert.That(decoded.Key, Is.EqualTo("k1"));
        Assert.That(decoded.Value, Is.EqualTo(new byte[] { 7, 8, 9 }));
        Assert.That(decoded.Op, Is.EqualTo(MutationKind.Set));
    }

    [Test]
    public async Task ReadShippingAsync_resumes_from_supplied_fromSequence()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var page = await grain.ReadShippingAsync(2L, 10, CancellationToken.None);

        Assert.That(page.Entries.Count, Is.EqualTo(1));
        Assert.That(page.Entries[0].Sequence, Is.EqualTo(2L));
        Assert.That(page.NextSequence, Is.EqualTo(3L));
    }

    [Test]
    public async Task ReadShippingAsync_advances_next_sequence_to_fromSequence_when_no_entries_match()
    {
        var grain = await CreateGrainAsync();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var page = await grain.ReadShippingAsync(10L, 10, CancellationToken.None);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(10L));
    }

    [Test]
    public async Task ReadShippingAsync_payload_bytes_are_byte_for_byte_equal_to_an_independent_encode()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new OrleansBinaryWalRecordEncoder(services.GetRequiredService<Serializer<WalRecord>>());
        var grain = await CreateGrainAsync(encoder: encoder);

        var entry = MakeEntry("byte-equiv", new byte[] { 42, 43, 44, 45 });
        await grain.AppendAsync(entry, CancellationToken.None);

        var page = await grain.ReadShippingAsync(0L, 1, CancellationToken.None);

        var decoded = encoder.Decode(page.Entries[0].EncodedPayload);
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        encoder.Encode(in decoded, writer);
        Assert.That(page.Entries[0].EncodedPayload, Is.EqualTo(writer.WrittenSpan.ToArray()));
    }

    [Test]
    public async Task ReadShippingAsync_at_tail_does_not_touch_storage()
    {
        // Regression for the idle tail-poll flood: when the shipper's
        // per-partition cursor already sits at the WAL tail the read
        // provably returns nothing and must be answered from the
        // in-memory cursor without a storage round-trip.
        var provider = new ReadCountingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var tail = await grain.GetNextSequenceAsync(CancellationToken.None);
        provider.ReadCount = 0;

        var page = await grain.ReadShippingAsync(tail, 256, CancellationToken.None);

        Assert.That(provider.ReadCount, Is.Zero);
        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(tail));
    }

    [Test]
    public async Task ReadShippingAsync_beyond_tail_does_not_touch_storage()
    {
        var provider = new ReadCountingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var tail = await grain.GetNextSequenceAsync(CancellationToken.None);
        provider.ReadCount = 0;

        var page = await grain.ReadShippingAsync(tail + 5, 256, CancellationToken.None);

        Assert.That(provider.ReadCount, Is.Zero);
        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(tail + 5));
    }

    [Test]
    public async Task ReadShippingAsync_on_empty_wal_does_not_touch_storage()
    {
        var provider = new ReadCountingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider);
        provider.ReadCount = 0;

        var page = await grain.ReadShippingAsync(0L, 256, CancellationToken.None);

        Assert.That(provider.ReadCount, Is.Zero);
        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.NextSequence, Is.EqualTo(0L));
    }

    [Test]
    public async Task ReadShippingAsync_with_backlog_below_tail_still_reads_storage()
    {
        // Guards the short-circuit against over-eager elision: a cursor
        // behind the tail must still hit storage and return the backlog.
        var provider = new ReadCountingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);
        provider.ReadCount = 0;

        var page = await grain.ReadShippingAsync(1L, 256, CancellationToken.None);

        Assert.That(provider.ReadCount, Is.GreaterThan(0));
        Assert.That(page.Entries.Count, Is.EqualTo(2));
        Assert.That(page.Entries[0].Sequence, Is.EqualTo(1L));
        Assert.That(page.Entries[1].Sequence, Is.EqualTo(2L));
        Assert.That(page.NextSequence, Is.EqualTo(3L));
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> decorator that counts the
    /// storage read calls reaching the inner provider, so a test can
    /// assert that the idle tail-poll fast-path issued no round-trip.
    /// <see cref="WalShardGrain.ReadShippingAsync"/> reads through the
    /// default <c>ReadEncodedAsync</c>, which drains <c>ReadAsync</c>;
    /// counting <c>ReadAsync</c> therefore observes every storage read.
    /// </summary>
    private sealed class ReadCountingWalStorageProvider(IWalStorageProvider inner) : IWalStorageProvider
    {
        public int ReadCount { get; set; }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
        {
            ReadCount++;
            return inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}