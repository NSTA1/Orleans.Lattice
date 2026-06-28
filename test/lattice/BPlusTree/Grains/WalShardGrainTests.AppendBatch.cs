using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Batched AppendBatchAsync on IWalShardGrain. Pins the
// single-grain-call collapse for the leaf bulk-write fast path and
// the same all-or-nothing flush semantics AppendAsync already
// guarantees. The base fixture lives in WalShardGrainTests.cs.
public partial class WalShardGrainTests
{
    [Test]
    public async Task AppendBatchAsync_empty_returns_empty_and_does_not_flush()
    {
        var provider = new CapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider);

        var offsets = await grain.AppendBatchAsync(Array.Empty<WalRecord>(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.Empty);
            Assert.That(provider.BatchSizes, Is.Empty);
        });
    }

    [Test]
    public void AppendBatchAsync_throws_on_null_entries()
    {
        Assert.That(async () =>
        {
            var grain = await CreateGrainAsync();
            await grain.AppendBatchAsync(null!, CancellationToken.None);
        }, Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task AppendBatchAsync_throws_on_oversized_entry_count()
    {
        // Defence-in-depth: a pathologically large batch must be rejected
        // before the grain pre-allocates per-entry working arrays sized to
        // entries.Count, so a caller cannot drive an unbounded up-front
        // allocation. The synthetic list reports an oversized Count but is
        // never indexed (the guard throws first), so the test stays cheap.
        var grain = await CreateGrainAsync();
        var oversized = new OversizedWalRecordList((1 << 20) + 1);

        Assert.That(
            async () => await grain.AppendBatchAsync(oversized, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // Reports a large Count without materialising any entries. Indexing it
    // throws, proving the size guard rejects the batch before the grain
    // touches a single element.
    private sealed class OversizedWalRecordList(int count) : IReadOnlyList<WalRecord>
    {
        public int Count => count;

        public WalRecord this[int index] =>
            throw new InvalidOperationException("Oversized batch must be rejected before indexing.");

        public IEnumerator<WalRecord> GetEnumerator() =>
            throw new InvalidOperationException("Oversized batch must be rejected before enumeration.");

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Test]
    public async Task AppendBatchAsync_returns_dense_ascending_offsets_starting_at_zero()
    {
        var grain = await CreateGrainAsync();

        var entries = new WalRecord[]
        {
            MakeEntry("k01"),
            MakeEntry("k02"),
            MakeEntry("k03"),
            MakeEntry("k04"),
        };
        var offsets = await grain.AppendBatchAsync(entries, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Has.Count.EqualTo(4));
            Assert.That(offsets[0], Is.EqualTo(0L));
            Assert.That(offsets[1], Is.EqualTo(1L));
            Assert.That(offsets[2], Is.EqualTo(2L));
            Assert.That(offsets[3], Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task AppendBatchAsync_persists_in_a_single_provider_flush_when_under_limits()
    {
        // Default WalMaxBatchEntries=100, WalMaxBatchBytes=4 MiB: a
        // 16-entry batch lands in one provider flush, proving the
        // grain-side coalescing collapses to a single
        // AppendEncodedBatchAsync call.
        var provider = new CapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider);

        var entries = new WalRecord[16];
        for (var i = 0; i < entries.Length; i++)
        {
            entries[i] = MakeEntry($"k{i:D2}");
        }

        var offsets = await grain.AppendBatchAsync(entries, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Has.Count.EqualTo(16));
            Assert.That(provider.BatchSizes, Has.Count.EqualTo(1),
                "16-entry batch under the per-batch caps must flush once.");
            Assert.That(provider.BatchSizes[0], Is.EqualTo(16),
                "The single flush must carry every entry.");
        });
    }

    [Test]
    public async Task AppendBatchAsync_splits_across_flushes_when_exceeding_max_entries()
    {
        // 5-entry batch with WalMaxBatchEntries=2 must cut over into
        // at least 3 separate flushes (2+2+1). The grain protocol
        // serialises the cutover under the in-flight cap, so the
        // resulting offset sequence remains dense and ascending.
        var provider = new CapturingWalStorageProvider(new InMemoryWalStorageProvider());
        var grain = await CreateGrainAsync(provider, new LatticeOptions
        {
            WalMaxBatchEntries = 2,
            WalMaxPendingBatches = 1,
        });

        var entries = new WalRecord[]
        {
            MakeEntry("k01"),
            MakeEntry("k02"),
            MakeEntry("k03"),
            MakeEntry("k04"),
            MakeEntry("k05"),
        };
        var offsets = await grain.AppendBatchAsync(entries, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Has.Count.EqualTo(5));
            for (var i = 0; i < 5; i++)
                Assert.That(offsets[i], Is.EqualTo((long)i));
            var totalFlushed = 0;
            foreach (var size in provider.BatchSizes) totalFlushed += size;
            Assert.That(totalFlushed, Is.EqualTo(5),
                "Every entry must be flushed exactly once across the cutover flushes.");
            Assert.That(provider.BatchSizes.Count, Is.GreaterThanOrEqualTo(2),
                "Per-entry cap must force at least one cutover for a 5-entry batch.");
        });
    }

    [Test]
    public async Task AppendBatchAsync_offsets_are_readable_back_through_ReadAsync()
    {
        // Round-trip sanity: an AppendBatchAsync call's offsets must
        // address the same WAL entries on the read side, in the same
        // input order.
        var grain = await CreateGrainAsync();

        var entries = new WalRecord[]
        {
            MakeEntry("apple"),
            MakeEntry("banana"),
            MakeEntry("cherry"),
        };
        var offsets = await grain.AppendBatchAsync(entries, CancellationToken.None);

        var page = await grain.ReadAsync(offsets[0], entries.Length, CancellationToken.None);
        Assert.That(page.Entries, Has.Count.EqualTo(3));
        Assert.Multiple(() =>
        {
            Assert.That(page.Entries[0].Entry.Key, Is.EqualTo("apple"));
            Assert.That(page.Entries[1].Entry.Key, Is.EqualTo("banana"));
            Assert.That(page.Entries[2].Entry.Key, Is.EqualTo("cherry"));
        });
    }

    [Test]
    public async Task AppendBatchAsync_advances_next_sequence_by_batch_count()
    {
        var grain = await CreateGrainAsync();

        var beforeNext = await grain.GetNextSequenceAsync(CancellationToken.None);
        var entries = new WalRecord[8];
        for (var i = 0; i < entries.Length; i++) entries[i] = MakeEntry($"k{i:D2}");
        await grain.AppendBatchAsync(entries, CancellationToken.None);
        var afterNext = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.That(afterNext - beforeNext, Is.EqualTo(8L));
    }
}
