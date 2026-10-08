using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Behavioural coverage for the Azure Table WAL provider's read path -
/// <see cref="AzureTableWalStorageProvider.ReadAsync"/>,
/// <see cref="AzureTableWalStorageProvider.ReadEncodedAsync"/>,
/// <see cref="AzureTableWalStorageProvider.GetHighestOffsetAsync"/>,
/// <see cref="AzureTableWalStorageProvider.GetLowestOffsetAsync"/>, and
/// <see cref="AzureTableWalStorageProvider.GetRetainedByteSizeAsync"/> - driven
/// against an in-memory table rather than a live Azurite endpoint.
/// <para>
/// Every pre-existing fixture that exercised these methods carried
/// <c>[Category("AzureStorageEmulator")]</c>, which the repository's standard
/// filter excludes, so the whole read path measured at zero line coverage in a
/// default run while appearing thoroughly tested in review. These fixtures
/// close that gap by substituting the SDK client surface, so they carry no
/// slow-suite category and run in the default filter.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderReadPathTests
{
    private const string TreeId = "tree-read";
    private const int ShardIndex = 0;

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private (AzureTableWalStorageProvider Provider, InMemoryWalTable Table) CreateProvider(
        Action<AzureTableWalStorageOptions>? configure = null)
    {
        var table = new InMemoryWalTable();
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Tread",
            Compression = LatticeCompression.None,
        };
        configure?.Invoke(options);
        var provider = new AzureTableWalStorageProvider(
            Options.Create(options),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)]);
        return (provider, table);
    }

    private static List<WalEntry> Entries(long firstOffset, int count) =>
        Enumerable.Range(0, count)
            .Select(i => new WalEntry
            {
                Offset = firstOffset + i,
                Mutation = new LatticeMutation
                {
                    Key = $"k{firstOffset + i}",
                    Value = [(byte)(firstOffset + i), 0x5A],
                },
            })
            .ToList();

    private static async Task<List<WalEntry>> DrainAsync(IAsyncEnumerable<WalEntry> source)
    {
        var drained = new List<WalEntry>();
        await foreach (var entry in source)
        {
            drained.Add(entry);
        }

        return drained;
    }

    [Test]
    public async Task ReadAsync_returns_every_appended_entry_in_offset_order()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
        Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "k0", "k1", "k2", "k3" }));
        Assert.That(read[2].Mutation.Value, Is.Not.Null);
        Assert.That(read[2].Mutation.Value!.ToArray(), Is.EqualTo(new byte[] { 2, 0x5A }));
    }

    [Test]
    public async Task ReadAsync_spans_multiple_batches_in_ascending_batch_order()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L }));
    }

    [Test]
    public async Task ReadAsync_honours_the_exclusive_lower_bound_and_skips_earlier_batches()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, 3L, 100, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 4L, 5L }));
    }

    [Test]
    public async Task ReadAsync_starting_mid_batch_yields_only_the_entries_above_the_bound()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 5), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, 1L, 100, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L, 3L, 4L }));
    }

    [Test]
    public async Task ReadAsync_stops_at_maxEntries_within_a_single_batch()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 6), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 2, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task ReadAsync_stops_at_maxEntries_on_a_batch_boundary()
    {
        // The cap is re-checked at the top of the manifest loop as well as
        // inside the entry loop. A batch-aligned cap is what reaches the
        // outer arm; an unaligned one reaches only the inner arm.
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(4, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 4, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task ReadAsync_returns_nothing_for_an_absent_shard()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        var read = await DrainAsync(provider.ReadAsync("no-such-tree", 7, -1L, 100, CancellationToken.None));

        Assert.That(read, Is.Empty);
    }

    [Test]
    public async Task ReadAsync_round_trips_a_compressed_payload()
    {
        // Compression changes the stored bytes and the row's Compression tag,
        // so the read path must inflate before deserialising. Pairing this
        // with the uncompressed cases above proves the tag is honoured rather
        // than ignored.
        var (provider, table) = CreateProvider(o => o.Compression = LatticeCompression.Zstd);
        await using var _ = provider;

        var entries = new List<WalEntry>
        {
            new()
            {
                Offset = 0L,
                Mutation = new LatticeMutation
                {
                    Key = "compressible",
                    Value = Enumerable.Repeat((byte)0x7F, 4096).ToArray(),
                },
            },
        };

        await provider.AppendBatchAsync(TreeId, ShardIndex, entries, CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var stored = table.Snapshot().Single(r => r.RowKey.StartsWith("E", StringComparison.Ordinal));
        Assert.That(stored.Compression, Is.Not.Zero, "the row should carry a non-zero compression tag");

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 10, CancellationToken.None));
        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(read[0].Mutation.Value, Is.Not.Null);
        Assert.That(entries[0].Mutation.Value, Is.Not.Null);
        Assert.That(read[0].Mutation.Value!.ToArray(), Is.EqualTo(entries[0].Mutation.Value!.ToArray()));
    }

    [Test]
    public void ReadAsync_rejects_a_maxEntries_below_one()
    {
        var (provider, _) = CreateProvider();

        Assert.That(
            async () => await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 0, CancellationToken.None)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReadEncodedAsync_returns_the_stored_bytes_verbatim_with_parallel_offsets()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var page = await provider.ReadEncodedAsync(
            TreeId, ShardIndex, -1L, 100, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 0L, 1L, 2L }));

        var storedPayloads = table.Snapshot()
            .Where(r => r.RowKey.StartsWith("E", StringComparison.Ordinal))
            .OrderBy(r => r.Offset)
            .Select(r => r.Payload!)
            .ToList();
        var returned = page.EncodedEntries.ToArray().Select(s => s.ToArray()).ToList();

        Assert.That(returned, Has.Count.EqualTo(3));
        for (var i = 0; i < returned.Count; i++)
        {
            Assert.That(returned[i], Is.EqualTo(storedPayloads[i]), $"segment {i} should be the stored bytes");
        }
    }

    [Test]
    public async Task ReadEncodedAsync_honours_maxEntries_and_the_lower_bound()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var page = await provider.ReadEncodedAsync(
            TreeId, ShardIndex, 1L, 3, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 2L, 3L, 4L }));
    }

    [Test]
    public async Task ReadEncodedAsync_returns_an_empty_page_for_an_absent_shard()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        var page = await provider.ReadEncodedAsync(
            "absent", 3, -1L, 10, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.Empty);
        Assert.That(page.EncodedEntries.ToArray(), Is.Empty);
    }

    [Test]
    public async Task ReadEncodedAsync_inflates_a_compressed_row_back_to_the_encoded_bytes()
    {
        var (compressed, _) = CreateProvider(o => o.Compression = LatticeCompression.Zstd);
        await using var _ = compressed;
        var (plain, _) = CreateProvider();
        await using var __ = plain;

        var entries = new List<WalEntry>
        {
            new()
            {
                Offset = 0L,
                Mutation = new LatticeMutation
                {
                    Key = "compressible",
                    Value = Enumerable.Repeat((byte)0x3C, 4096).ToArray(),
                },
            },
        };

        await compressed.AppendBatchAsync(TreeId, ShardIndex, entries, CancellationToken.None);
        await compressed.FlushPhaseTwoAsync(CancellationToken.None);
        await plain.AppendBatchAsync(TreeId, ShardIndex, entries, CancellationToken.None);
        await plain.FlushPhaseTwoAsync(CancellationToken.None);

        var fromCompressed = await compressed.ReadEncodedAsync(
            TreeId, ShardIndex, -1L, 10, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);
        var fromPlain = await plain.ReadEncodedAsync(
            TreeId, ShardIndex, -1L, 10, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        // The inflated bytes must equal what the uncompressed provider stored:
        // compression is transparent to the shipper's encoded view.
        Assert.That(
            fromCompressed.EncodedEntries.ToArray().Single().ToArray(),
            Is.EqualTo(fromPlain.EncodedEntries.ToArray().Single().ToArray()));
    }

    [Test]
    public void ReadEncodedAsync_rejects_a_maxEntries_below_one()
    {
        var (provider, _) = CreateProvider();

        Assert.That(
            async () => await provider.ReadEncodedAsync(
                TreeId, ShardIndex, -1L, 0, Substitute.For<IWalRecordEncoder>(), CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetHighestOffsetAsync_returns_minus_one_before_anything_is_written()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetHighestOffsetAsync_tracks_the_committed_tail()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(3L));

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(4, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(5L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_before_anything_is_written()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        Assert.That(
            await provider.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_the_first_live_entry_offset()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(
            await provider.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(0L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_walks_past_a_fully_trimmed_batch()
    {
        // Trim can empty a batch partition while leaving a later manifest row
        // live, so the lowest offset must come from the first NON-EMPTY batch
        // rather than from the first manifest row. This is the forward-walk
        // arm of the method.
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);

        Assert.That(
            await provider.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(3L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_when_every_batch_is_empty()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        // Delete the entry rows but keep the manifest row, which is exactly
        // the state a crash between the two trim deletes leaves behind.
        foreach (var row in table.Snapshot().Where(r => r.RowKey.StartsWith("E", StringComparison.Ordinal)))
        {
            await table.BuildTableClient().DeleteEntityAsync(
                row.PartitionKey, row.RowKey, Azure.ETag.All, CancellationToken.None);
        }

        Assert.That(
            await provider.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_is_zero_for_an_absent_shard()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        Assert.That(
            await provider.GetRetainedByteSizeAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.Zero);
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_sums_the_manifest_payload_bytes()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var expected = table.Snapshot()
            .Where(r => r.RowKey.StartsWith("M", StringComparison.Ordinal))
            .Sum(r => r.PayloadBytes);

        Assert.That(expected, Is.GreaterThan(0L), "the manifest rows should carry a payload-byte total");
        Assert.That(
            await provider.GetRetainedByteSizeAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(expected));
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_drops_when_a_batch_is_fully_trimmed()
    {
        var (provider, _) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var before = await provider.GetRetainedByteSizeAsync(TreeId, ShardIndex, CancellationToken.None);
        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);
        var after = await provider.GetRetainedByteSizeAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(after, Is.LessThan(before));
        Assert.That(after, Is.GreaterThan(0L), "the surviving batch should still be accounted");
    }

    [Test]
    public void Read_methods_reject_a_null_treeId()
    {
        var (provider, _) = CreateProvider();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await DrainAsync(provider.ReadAsync(null!, 0, -1L, 1, CancellationToken.None)),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await provider.ReadEncodedAsync(
                    null!, 0, -1L, 1, Substitute.For<IWalRecordEncoder>(), CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await provider.GetHighestOffsetAsync(null!, 0, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await provider.GetLowestOffsetAsync(null!, 0, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await provider.GetRetainedByteSizeAsync(null!, 0, CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }
}
