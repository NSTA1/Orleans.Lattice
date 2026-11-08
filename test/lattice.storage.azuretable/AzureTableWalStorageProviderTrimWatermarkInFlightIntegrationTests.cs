using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Azurite-backed regression coverage for issue #6036: a trim-watermark probe
/// taken while a shard's first pipelined batch is accepted but not yet committed
/// must not read that batch as trimmed. Before the fix the probe inferred the
/// watermark from the highest offset, which folds in the accepted batch, while the
/// lowest offset saw no committed manifest row; the result was cached, and every
/// later read on this provider instance clamped past the batch and returned nothing.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalStorageProviderTrimWatermarkInFlightIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-trim-watermark-in-flight";
    private const int EntriesPerBatch = 4;

    // Holds the first arrival's phase-2 commit back long enough that the probe
    // below deterministically observes the accepted-but-uncommitted state.
    private static readonly TimeSpan LongCoalescingWindow = TimeSpan.FromSeconds(3);

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _adminClient = new TableServiceClient(AzuriteConnectionString);

        try
        {
            await foreach (var _ in _adminClient.QueryAsync(maxPerPage: 1))
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp() => _tableName = "T" + Guid.NewGuid().ToString("N");

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup.
        }
    }

    private AzureTableWalStorageProvider CreateProvider() =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = true,
                PhaseTwoCoalescingWindow = LongCoalescingWindow,
            }),
            _serializer);

    private static WalEntry[] Batch(long firstOffset)
    {
        var batch = new WalEntry[EntriesPerBatch];
        for (var i = 0; i < EntriesPerBatch; i++)
        {
            batch[i] = new WalEntry
            {
                Offset = firstOffset + i,
                Mutation = new LatticeMutation
                {
                    TreeId = TreeId,
                    Kind = MutationKind.Set,
                    Key = $"k-{firstOffset + i:D4}",
                    Value = System.Text.Encoding.UTF8.GetBytes($"v-{firstOffset + i}"),
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    OriginClusterId = "site-a",
                },
            };
        }

        return batch;
    }

    private static async Task<List<long>> ReadOffsetsAsync(AzureTableWalStorageProvider provider, long fromOffsetExclusive)
    {
        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync(
            TreeId, 0, fromOffsetExclusive, maxEntries: int.MaxValue, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }

        return offsets;
    }

    [Test]
    public async Task A_watermark_probe_during_the_first_uncommitted_batch_does_not_report_it_trimmed()
    {
        await using var sut = CreateProvider();

        await sut.AppendBatchAsync(TreeId, 0, Batch(0L), CancellationToken.None);
        var watermark = await sut.GetTrimWatermarkAsync(TreeId, 0, CancellationToken.None);
        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(watermark, Is.EqualTo(-1L),
            "nothing was ever trimmed: an accepted batch whose manifest row has not landed is not a trimmed range");
    }

    [Test]
    public async Task A_watermark_probe_during_the_first_uncommitted_batch_does_not_hide_it_from_later_reads()
    {
        await using var sut = CreateProvider();

        await sut.AppendBatchAsync(TreeId, 0, Batch(0L), CancellationToken.None);
        _ = await sut.GetTrimWatermarkAsync(TreeId, 0, CancellationToken.None);
        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(await ReadOffsetsAsync(sut, -1L), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
            "once the batch commits, a read from the start of the log must return every entry it holds");
    }

    [Test]
    public async Task A_watermark_probe_during_the_first_uncommitted_batch_does_not_hide_it_from_replay_or_shipping_reads()
    {
        // The leaf replay and replay-page ring read through ReadFilteredAsync and
        // the shipper through ReadEncodedAsync; both clamp to the same cached
        // watermark as ReadAsync, so each must still see the committed batch.
        await using var sut = CreateProvider();

        await sut.AppendBatchAsync(TreeId, 0, Batch(0L), CancellationToken.None);
        _ = await sut.GetTrimWatermarkAsync(TreeId, 0, CancellationToken.None);
        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        var filtered = new List<long>();
        await foreach (var entry in sut.ReadFilteredAsync(
            TreeId, 0, -1L, long.MaxValue, int.MaxValue, new WalKeyFilter(null, null), CancellationToken.None))
        {
            filtered.Add(entry.Offset);
        }

        var encoded = await sut.ReadEncodedAsync(TreeId, 0, -1L, int.MaxValue, new NoEncoder(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(filtered, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }), "the leaf replay read must see every committed entry");
            Assert.That(encoded.Offsets.ToArray(), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }), "the shipping read must see every committed entry");
        });
    }

    /// <summary>The provider holds encoded bytes verbatim and ignores the encoder.</summary>
    private sealed class NoEncoder : IWalRecordEncoder
    {
        public void Encode(in WalRecord record, System.Buffers.IBufferWriter<byte> writer) =>
            throw new NotSupportedException();

        public WalRecord Decode(ReadOnlySpan<byte> encoded) => throw new NotSupportedException();
    }

    [Test]
    public async Task A_refresh_replaces_a_cached_watermark_above_the_stored_trim_point()
    {
        await using var sut = CreateProvider();

        await sut.AppendBatchAsync(TreeId, 0, Batch(0L), CancellationToken.None);
        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        // A watermark cached above anything stored hides every entry beneath it,
        // whatever put it there; an ordinary probe keeps the larger value.
        sut.NoteTrimWatermarkForTesting(TreeId, 0, 3L);
        _ = await sut.GetTrimWatermarkAsync(TreeId, 0, CancellationToken.None);
        var hidden = await ReadOffsetsAsync(sut, -1L);

        var refreshed = await sut.RefreshTrimWatermarkAsync(TreeId, 0, CancellationToken.None);
        var healed = await ReadOffsetsAsync(sut, -1L);

        Assert.Multiple(() =>
        {
            Assert.That(hidden, Is.Empty, "the cached watermark clamps every read past the stored entries");
            Assert.That(refreshed, Is.EqualTo(-1L), "nothing was trimmed, so the stored watermark is -1");
            Assert.That(healed, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
                "a refresh must replace the cached watermark with the stored one, so every entry reads again");
        });
    }
}
