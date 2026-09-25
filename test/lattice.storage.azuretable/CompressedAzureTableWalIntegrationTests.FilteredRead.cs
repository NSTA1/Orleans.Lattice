using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// The filtered replay read (issue #3565) over compressed rows. A row is
/// classified from the routing prefix of its inflated payload, so compression
/// changes where the prefix is read from and nothing about the answer.
/// </summary>
public partial class CompressedAzureTableWalIntegrationTests
{
    private AzureTableWalStorageProvider CreateRoutedProvider(string tableName, LatticeCompression compression) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = tableName,
                PipelinePhaseTwoCommits = false,
                Compression = compression,
                CompressionMinPayloadBytes = 0,
            }),
            _serializer,
            saturationSignal: null,
            compressors: new ILatticeCompressor[] { new ZstdLatticeCompressor(3) },
            routing: new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>()));

    private static async Task<List<WalEntry>> ReadFilteredAsync(
        AzureTableWalStorageProvider sut,
        WalKeyFilter filter,
        long toOffsetInclusive,
        int maxEntries = 1024)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadFilteredAsync(TreeId, 0, -1L, toOffsetInclusive, maxEntries, filter, CancellationToken.None))
        {
            collected.Add(entry);
        }

        return collected;
    }

    [Test]
    public async Task ReadFilteredAsync_classifies_compressed_rows_from_their_inflated_prefix()
    {
        await using var routed = CreateRoutedProvider(_tableName, LatticeCompression.Zstd);
        var batch = new[] { Entry(0, "a0"), Entry(1, "m1"), Entry(2, "b2"), Entry(3, "m3"), Entry(4, "z4") };
        await routed.AppendBatchAsync(TreeId, 0, batch, CancellationToken.None);

        var read = await ReadFilteredAsync(routed, new WalKeyFilter("m", "n"), toOffsetInclusive: 4);

        var stored = await _adminClient.GetTableClient(_tableName).GetEntityAsync<AzureTableWalEntity>(
            AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, 0, 0L),
            AzureTableWalStorageProvider.BuildEntryRowKey(4L));
        Assert.Multiple(() =>
        {
            Assert.That(stored.Value.Compression, Is.EqualTo((byte)LatticeCompression.Zstd), "The rows must actually be compressed.");
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L, 4L }));
            Assert.That(read[0].Mutation.Value, Is.EqualTo(batch[1].Mutation.Value));
            Assert.That(read[1].Mutation.Value, Is.EqualTo(batch[3].Mutation.Value));
            Assert.That(read[2].Mutation.Key, Is.EqualTo("z4"), "The last examined row is excluded and arrives routing-only.");
            Assert.That(read[2].Mutation.Value, Is.Null);
        });
    }

    [Test]
    public async Task ReadFilteredAsync_with_and_without_a_routing_reader_returns_the_same_rows()
    {
        // Uncompressed and compressed rows side by side, keys that need a real
        // UTF-8 decode to compare, and a filter on both axes.
        var legacyWriter = CreateProvider(_tableName, LatticeCompression.None, minPayloadBytes: 0);
        await legacyWriter.AppendBatchAsync(
            TreeId, 0, new[] { Entry(0, "a0"), Entry(1, "m1"), Entry(2, "\u00FCber"), Entry(3, "m\uD83D\uDE00") }, CancellationToken.None);
        await _sut.AppendBatchAsync(
            TreeId, 0, new[] { Entry(4, "m4"), Entry(5, "b5"), Entry(6, "m6"), Entry(7, "m7") }, CancellationToken.None);
        var map = ShardMap.CreateDefault(64, 2);
        var filter = new WalKeyFilter("m", null, map, 1);
        await using var routed = CreateRoutedProvider(_tableName, LatticeCompression.Zstd);

        var fast = await ReadFilteredAsync(routed, filter, toOffsetInclusive: 7);
        var slow = await ReadFilteredAsync(_sut, filter, toOffsetInclusive: 7);

        static string Render(WalEntry e) =>
            $"{e.Offset}:{e.Mutation.Kind}:{e.Mutation.Key}:{Convert.ToHexString(e.Mutation.Value ?? [])}";

        var owned = new[] { "a0", "m1", "\u00FCber", "m\uD83D\uDE00", "m4", "b5", "m6", "m7" }.Where(filter.Owns).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(fast.Select(Render), Is.EqualTo(slow.Select(Render)));
            Assert.That(fast.Where(e => e.Mutation.Value is not null).Select(e => e.Mutation.Key), Is.EqualTo(owned));
            Assert.That(owned, Is.Not.Empty, "A filter that owns nothing would make the comparison vacuous.");
            Assert.That(owned, Has.Length.LessThan(6), "A filter that excludes only the out-of-range keys would not test the shard axis.");
        });
    }
}
