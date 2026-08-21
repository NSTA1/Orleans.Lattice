using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// End-to-end tests for <see cref="AzureTableWalStorageProvider"/>'s
/// per-row payload compression, driven against an Azure Table Storage
/// endpoint (canonically Azurite). Mirrors the append/read coverage of
/// <see cref="AzureTableWalStorageProviderIntegrationTests"/> but with
/// <see cref="AzureTableWalStorageOptions.Compression"/> =
/// <see cref="LatticeCompression.Zstd"/> and
/// <see cref="AzureTableWalStorageOptions.CompressionMinPayloadBytes"/> =
/// <c>0</c>, so every row exercises the compressed encode/decode path.
/// <para>
/// Gated under the <c>AzureStorageEmulator</c> NUnit category so the
/// default dev loop skips it when no emulator is running.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class CompressedAzureTableWalIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-compressed";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private AzureTableWalStorageProvider _sut = null!;

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
                + $"Start it via 'azurite --silent --location <dir>' or skip the AzureStorageEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _tableName = "T" + Guid.NewGuid().ToString("N");
        _sut = CreateProvider(_tableName, LatticeCompression.Zstd, minPayloadBytes: 0);
    }

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup; a missing table or a 409 mid-delete
            // is acceptable - the next test gets a fresh GUID.
        }
    }

    private AzureTableWalStorageProvider CreateProvider(
        string tableName,
        LatticeCompression compression,
        int minPayloadBytes) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = tableName,
                // Synchronous phase-2 so read-your-writes holds the
                // instant AppendBatchAsync returns; see the rationale on
                // AzureTableWalStorageProviderIntegrationTests.CreateProvider.
                PipelinePhaseTwoCommits = false,
                Compression = compression,
                CompressionMinPayloadBytes = minPayloadBytes,
            }),
            _serializer,
            saturationSignal: null,
            compressors: new ILatticeCompressor[] { new ZstdLatticeCompressor(3) });

    private static WalEntry Entry(long offset, string key = "k", int valueBytes = 1024)
    {
        // A compressible value (repeated byte) so the row clears the
        // threshold and shrinks under Zstd.
        var value = new byte[valueBytes];
        Array.Fill(value, (byte)(offset & 0xFF));
        return new WalEntry
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = key,
                Value = value,
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };
    }

    private static async Task<List<WalEntry>> ReadAllAsync(
        AzureTableWalStorageProvider sut,
        long fromOffsetExclusive = -1L,
        int maxEntries = 1024)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, fromOffsetExclusive, maxEntries, CancellationToken.None))
        {
            collected.Add(entry);
        }
        return collected;
    }

    [Test]
    public async Task AppendBatchAsync_then_ReadAsync_round_trips_through_compression()
    {
        var batch = new[] { Entry(0), Entry(1), Entry(2) };

        await _sut.AppendBatchAsync(TreeId, 0, batch, CancellationToken.None);
        var read = await ReadAllAsync(_sut);

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
            for (var i = 0; i < batch.Length; i++)
            {
                Assert.That(read[i].Mutation.Key, Is.EqualTo(batch[i].Mutation.Key), $"entry[{i}] key");
                Assert.That(read[i].Mutation.Value, Is.EqualTo(batch[i].Mutation.Value), $"entry[{i}] value");
            }
        });
    }

    [Test]
    public async Task AppendBatchAsync_round_trips_every_LatticeMutation_field_under_compression()
    {
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var value = new byte[2048];
        Array.Fill(value, (byte)0x5A);
        var mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "users/42",
            Value = value,
            Timestamp = hlc,
            IsTombstone = false,
            ExpiresAtTicks = 1_700_000_000_000L,
            OriginClusterId = "site-b",
        };

        await _sut.AppendBatchAsync(TreeId, 0, new[] { new WalEntry { Offset = 0L, Mutation = mutation } }, CancellationToken.None);
        var read = await ReadAllAsync(_sut);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(read[0].Mutation.Key, Is.EqualTo("users/42"));
            Assert.That(read[0].Mutation.Value, Is.EqualTo(value));
            Assert.That(read[0].Mutation.Timestamp, Is.EqualTo(hlc));
            Assert.That(read[0].Mutation.ExpiresAtTicks, Is.EqualTo(1_700_000_000_000L));
            Assert.That(read[0].Mutation.OriginClusterId, Is.EqualTo("site-b"));
        });
    }

    [Test]
    public async Task ReadAsync_respects_fromOffsetExclusive_and_maxEntries_under_compression()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var read = await ReadAllAsync(_sut, fromOffsetExclusive: 1L, maxEntries: 2);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L, 3L }));
    }

    [Test]
    public async Task Rows_are_persisted_with_the_Zstd_compression_tag()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0) }, CancellationToken.None);

        // Read the raw entry row back through an out-of-band admin client
        // to confirm the on-disk Compression column carries the Zstd tag
        // and the stored Payload is the length-prefixed compressed form
        // (smaller than the uncompressed encode).
        var table = _adminClient.GetTableClient(_tableName);
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, 0, 0L);
        var rowKey = AzureTableWalStorageProvider.BuildEntryRowKey(0L);
        var entity = await table.GetEntityAsync<AzureTableWalEntity>(batchPartitionKey, rowKey);

        Assert.That(entity.Value.Compression, Is.EqualTo((byte)LatticeCompression.Zstd));
        Assert.That(entity.Value.Payload, Is.Not.Null.And.Length.GreaterThan(sizeof(int)));
    }

    [Test]
    public async Task AppendBatchAsync_emits_compression_savings_counters_tagged_by_tree()
    {
        using var collector = new WalCompressionCounterCollector();

        // Compressible rows so the stored total lands strictly below the
        // uncompressed total, proving the savings the dashboard derives.
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var uncompressed = collector.Sum(LatticeMetrics.StorageWalUncompressedBytesName, TreeId);
        var stored = collector.Sum(LatticeMetrics.StorageWalStoredBytesName, TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(uncompressed, Is.GreaterThan(0), "the batch encoded payload bytes");
            Assert.That(stored, Is.GreaterThan(0), "the batch stored payload bytes");
            Assert.That(stored, Is.LessThan(uncompressed), "compressible rows must store fewer bytes than encoded");
        });
    }

    /// <summary>
    /// Captures long-valued measurements on the core
    /// <see cref="LatticeMetrics.Meter"/>, filtering at read time by
    /// instrument name and <c>tree</c> tag so parallel fixtures writing the
    /// same shared counters do not cross-pollute.
    /// </summary>
    private sealed class WalCompressionCounterCollector : IDisposable
    {
        private readonly System.Diagnostics.Metrics.MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _lock = new();

        public WalCompressionCounterCollector()
        {
            _listener = new System.Diagnostics.Metrics.MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(
            System.Diagnostics.Metrics.Instrument instrument,
            long value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            lock (_lock)
            {
                _records.Add((instrument.Name, value, tags.ToArray()));
            }
        }

        public long Sum(string name, string tree)
        {
            lock (_lock)
            {
                return _records
                    .Where(r => r.Name == name
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree))
                    .Sum(r => r.Value);
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task AppendEncodedBatchAsync_then_ReadEncodedAsync_round_trips_through_compression()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var record = new WalRecord
        {
            TreeId = TreeId,
            Op = MutationKind.Set,
            Key = "encoded-compressed",
            Value = Enumerable.Repeat((byte)0x7E, 2048).ToArray(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
        };
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        encoder.Encode(record, writer);
        var producedBytes = writer.WrittenSpan.ToArray();

        await _sut.AppendEncodedBatchAsync(
            TreeId,
            0,
            new ReadOnlyMemory<ArraySegment<byte>>(new[] { new ArraySegment<byte>(producedBytes) }),
            new ReadOnlyMemory<long>(new[] { 0L }),
            encoder,
            CancellationToken.None);

        var page = await _sut.ReadEncodedAsync(TreeId, 0, -1L, 64, encoder, CancellationToken.None);

        Assert.That(page.EncodedEntries.Length, Is.EqualTo(1));
        Assert.That(
            page.EncodedEntries.Span[0].ToArray(),
            Is.EqualTo(producedBytes),
            "ReadEncodedAsync must inflate the compressed row back to the exact encoded bytes AppendEncodedBatchAsync was given");

        var decoded = encoder.Decode(page.EncodedEntries.Span[0].AsSpan());
        Assert.That(decoded.Key, Is.EqualTo("encoded-compressed"));
        Assert.That(decoded.Value, Is.EqualTo(record.Value));
    }

    [Test]
    public async Task Rows_written_without_compression_read_back_unmodified_under_a_compressing_reader()
    {
        // Backwards-compat: a silo that never enabled compression writes
        // rows tagged Compression = None (the same shape a pre-feature
        // silo produces). A later silo that enables compression must read
        // those rows back verbatim - the tag column drives the decode, so
        // legacy None rows bypass decompression entirely.
        var legacyWriter = CreateProvider(_tableName, LatticeCompression.None, minPayloadBytes: 0);
        var batch = new[] { Entry(0, "legacy-0"), Entry(1, "legacy-1") };
        await legacyWriter.AppendBatchAsync(TreeId, 0, batch, CancellationToken.None);

        var compressingReader = CreateProvider(_tableName, LatticeCompression.Zstd, minPayloadBytes: 0);
        var read = await ReadAllAsync(compressingReader);

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
            Assert.That(read[0].Mutation.Key, Is.EqualTo("legacy-0"));
            Assert.That(read[0].Mutation.Value, Is.EqualTo(batch[0].Mutation.Value));
            Assert.That(read[1].Mutation.Key, Is.EqualTo("legacy-1"));
        });
    }

    [Test]
    public async Task Compressed_rows_are_readable_by_a_reader_with_compression_disabled_but_the_compressor_registered()
    {
        // The decode path keys on the row's tag, not the reader's
        // Compression option - so a reader with Compression = None still
        // inflates rows a compressing producer wrote, as long as the
        // matching compressor is registered.
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0, "x") }, CancellationToken.None);

        var disabledReader = CreateProvider(_tableName, LatticeCompression.None, minPayloadBytes: 0);
        var read = await ReadAllAsync(disabledReader);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(read[0].Mutation.Key, Is.EqualTo("x"));
        Assert.That(read[0].Mutation.Value, Is.EqualTo(Entry(0, "x").Mutation.Value));
    }
}
