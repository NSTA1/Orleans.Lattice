using System.Buffers.Binary;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the per-row payload compression seam on
/// <see cref="AzureTableWalStorageProvider"/>. These exercise the
/// encode hot path (<see cref="AzureTableWalStorageProvider.EncodeEntriesForBatch"/>
/// -&gt; <c>BuildEntryEntity</c> -&gt; <c>CompressPayload</c>) and the
/// construction-time compressor wiring without touching an Azure Tables
/// endpoint. The decode half of the round-trip is covered end-to-end by
/// <see cref="CompressedAzureTableWalIntegrationTests"/> (emulator-gated);
/// here the test re-implements the documented on-disk layout
/// (<c>[4-byte LE uncompressed length][compressed bytes]</c>) with a
/// standalone <see cref="ZstdLatticeCompressor"/> to prove the encoder
/// writes exactly that shape.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    private static AzureTableWalStorageProvider CreateCompressingProvider(
        LatticeCompression compression = LatticeCompression.Zstd,
        int minPayloadBytes = 0,
        IEnumerable<ILatticeCompressor>? compressors = null)
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
            Compression = compression,
            CompressionMinPayloadBytes = minPayloadBytes,
        });
        return new AzureTableWalStorageProvider(
            options,
            serializer,
            saturationSignal: null,
            compressors: compressors ?? new ILatticeCompressor[] { new ZstdLatticeCompressor(3) });
    }

    private static WalEntry CompressibleEntry(long offset, int valueBytes)
    {
        // Highly-compressible payload: a single repeated byte so Zstd
        // shrinks it well below the original even at small sizes.
        var value = new byte[valueBytes];
        Array.Fill(value, (byte)0xAB);
        return new WalEntry
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = "compress-pin",
                Kind = MutationKind.Set,
                Key = "k-" + offset.ToString("D6", System.Globalization.CultureInfo.InvariantCulture),
                Value = value,
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
                Category = MutationCategory.User,
            },
        };
    }

    private static WalEntry IncompressibleEntry(long offset, int valueBytes)
    {
        // Incompressible payload: deterministic pseudo-random bytes that
        // Zstd cannot shrink, so the inflation guard must store them
        // verbatim rather than as a (larger) compressed frame.
        var value = new byte[valueBytes];
        new Random(20260611).NextBytes(value);
        return new WalEntry
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = "compress-pin",
                Kind = MutationKind.Set,
                Key = "k-" + offset.ToString("D6", System.Globalization.CultureInfo.InvariantCulture),
                Value = value,
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
                Category = MutationCategory.User,
            },
        };
    }

    [Test]
    public void EncodeEntriesForBatch_tags_compressed_rows_with_Zstd_when_enabled()
    {
        var provider = CreateCompressingProvider(minPayloadBytes: 0);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(partitionKey, new[] { CompressibleEntry(0, 2048) }, actions);

        var entity = (AzureTableWalEntity)actions[0].Entity;
        Assert.That(entity.Compression, Is.EqualTo((byte)LatticeCompression.Zstd));
    }

    [Test]
    public void EncodeEntriesForBatch_compressed_payload_round_trips_to_the_original_record()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
            Compression = LatticeCompression.Zstd,
            CompressionMinPayloadBytes = 0,
        });
        var provider = new AzureTableWalStorageProvider(
            options, serializer, saturationSignal: null,
            compressors: new ILatticeCompressor[] { new ZstdLatticeCompressor(3) });

        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var entry = CompressibleEntry(0, 4096);
        var actions = new List<TableTransactionAction>();
        provider.EncodeEntriesForBatch(partitionKey, new[] { entry }, actions);

        var entity = (AzureTableWalEntity)actions[0].Entity;
        Assert.That(entity.Payload, Is.Not.Null);

        // Reverse the documented layout: 4-byte LE uncompressed length
        // prefix followed by the compressed frame.
        var uncompressedLength = BinaryPrimitives.ReadInt32LittleEndian(entity.Payload);
        var decompressed = new byte[uncompressedLength];
        using var compressor = new ZstdLatticeCompressor(3);
        compressor.Decompress(entity.Payload.AsSpan(sizeof(int)), decompressed, uncompressedLength);

        var record = serializer.Deserialize(new ReadOnlyMemory<byte>(decompressed));
        Assert.Multiple(() =>
        {
            Assert.That(record.Key, Is.EqualTo(entry.Mutation.Key));
            Assert.That(record.Value, Is.EqualTo(entry.Mutation.Value));
            Assert.That(record.Op, Is.EqualTo(entry.Mutation.Kind));
        });
    }

    [Test]
    public void EncodeEntriesForBatch_stores_fewer_bytes_for_a_compressible_payload()
    {
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var entry = CompressibleEntry(0, 8192);

        var uncompressed = new List<TableTransactionAction>();
        CreateCompressingProvider(compression: LatticeCompression.None)
            .EncodeEntriesForBatch(partitionKey, new[] { entry }, uncompressed);

        var compressed = new List<TableTransactionAction>();
        CreateCompressingProvider(minPayloadBytes: 0)
            .EncodeEntriesForBatch(partitionKey, new[] { entry }, compressed);

        var rawLen = ((AzureTableWalEntity)uncompressed[0].Entity).Payload!.Length;
        var compressedLen = ((AzureTableWalEntity)compressed[0].Entity).Payload!.Length;
        Assert.That(compressedLen, Is.LessThan(rawLen));
    }

    [Test]
    public void EncodeEntriesForBatch_stores_incompressible_payload_verbatim()
    {
        // The inflation guard: an above-threshold payload that Zstd cannot
        // shrink must be stored verbatim (tag None), never as a larger
        // compressed frame, so enabling compression by default never grows
        // a row's footprint.
        var provider = CreateCompressingProvider(minPayloadBytes: 0);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(partitionKey, new[] { IncompressibleEntry(0, 2048) }, actions);

        var entity = (AzureTableWalEntity)actions[0].Entity;
        Assert.That(entity.Compression, Is.EqualTo((byte)LatticeCompression.None));
    }

    [Test]
    public void EncodeEntriesForBatch_leaves_payloads_below_threshold_uncompressed()
    {
        var provider = CreateCompressingProvider(minPayloadBytes: 100_000);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(partitionKey, new[] { CompressibleEntry(0, 256) }, actions);

        var entity = (AzureTableWalEntity)actions[0].Entity;
        Assert.That(entity.Compression, Is.EqualTo((byte)LatticeCompression.None));
    }

    [Test]
    public void EncodeEntriesForBatch_leaves_rows_uncompressed_when_compression_disabled()
    {
        var provider = CreateCompressingProvider(compression: LatticeCompression.None);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(partitionKey, new[] { CompressibleEntry(0, 4096) }, actions);

        var entity = (AzureTableWalEntity)actions[0].Entity;
        Assert.That(entity.Compression, Is.EqualTo((byte)LatticeCompression.None));
    }

    [Test]
    public void Constructor_throws_when_compression_enabled_without_a_matching_compressor()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
            Compression = LatticeCompression.Zstd,
        });

        Assert.That(
            () => new AzureTableWalStorageProvider(
                options, serializer, saturationSignal: null, compressors: Array.Empty<ILatticeCompressor>()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void Constructor_throws_when_a_compressor_registers_the_None_tag()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
        });

        Assert.That(
            () => new AzureTableWalStorageProvider(
                options, serializer, saturationSignal: null,
                compressors: new ILatticeCompressor[] { new NoneTagCompressor() }),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_throws_when_two_compressors_share_a_tag()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
        });

        Assert.That(
            () => new AzureTableWalStorageProvider(
                options, serializer, saturationSignal: null,
                compressors: new ILatticeCompressor[] { new ZstdLatticeCompressor(3), new ZstdLatticeCompressor(5) }),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_throws_when_a_compressor_in_the_sequence_is_null()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
        });

        Assert.That(
            () => new AzureTableWalStorageProvider(
                options, serializer, saturationSignal: null,
                compressors: new ILatticeCompressor?[] { null }!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_CompressionMinPayloadBytes_is_negative()
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalRecord>>();
        var options = Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "TCompressionPin",
            CompressionMinPayloadBytes = -1,
        });

        Assert.That(
            () => new AzureTableWalStorageProvider(options, serializer, saturationSignal: null, compressors: null),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    /// <summary>
    /// Compressor that illegally claims the reserved
    /// <see cref="LatticeCompression.None"/> tag, used to prove the
    /// provider rejects it at construction.
    /// </summary>
    private sealed class NoneTagCompressor : ILatticeCompressor
    {
        public LatticeCompression Algorithm => LatticeCompression.None;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
            => source.CopyTo(destination);
    }
}
