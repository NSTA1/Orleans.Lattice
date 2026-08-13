using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Unit tests for the zero-copy encoded append / read seams of
/// <see cref="FileWalStorageProvider"/> - the paths the WAL grain uses when
/// it has already paid the encode cost via <see cref="IWalRecordEncoder"/>.
/// Verifies the stored segments round-trip verbatim and interoperate with
/// the <see cref="WalEntry"/>-shaped seam byte-for-byte.
/// </summary>
[TestFixture]
public sealed class FileWalStorageProviderEncodedPathTests
{
    private const string TreeId = "tree-encoded";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-tests", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (System.IO.Directory.Exists(_root))
            {
                System.IO.Directory.Delete(_root, recursive: true);
            }
        }
        catch (IOException)
        {
        }
    }

    private FileWalStorageProvider CreateProvider()
    {
        var options = Options.Create(new FileWalStorageOptions { RootDirectory = _root });
        return new FileWalStorageProvider(options, _serializer);
    }

    private WalRecord Record(string key, byte tag) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { tag },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
    };

    private (ArraySegment<byte>[] Segments, long[] Offsets) Encode(params (long Offset, string Key, byte Tag)[] items)
    {
        var segments = new ArraySegment<byte>[items.Length];
        var offsets = new long[items.Length];
        for (var i = 0; i < items.Length; i++)
        {
            var writer = new System.Buffers.ArrayBufferWriter<byte>();
            var record = Record(items[i].Key, items[i].Tag);
            _encoder.Encode(in record, writer);
            segments[i] = new ArraySegment<byte>(writer.WrittenMemory.ToArray());
            offsets[i] = items[i].Offset;
        }

        return (segments, offsets);
    }

    [Test]
    public async Task AppendEncodedBatchAsync_stores_segments_that_ReadEncodedAsync_returns_verbatim()
    {
        using var sut = CreateProvider();
        var (segments, offsets) = Encode((0, "a", 1), (1, "b", 2), (2, "c", 3));

        await sut.AppendEncodedBatchAsync(
            TreeId, 0, segments, offsets, _encoder, CancellationToken.None);

        var page = await sut.ReadEncodedAsync(TreeId, 0, -1L, 1024, _encoder, CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 0L, 1L, 2L }));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(2L));
        var readSegments = page.EncodedEntries.Span;
        for (var i = 0; i < segments.Length; i++)
        {
            Assert.That(readSegments[i].AsSpan().SequenceEqual(segments[i].AsSpan()), Is.True,
                $"segment {i} must be returned byte-for-byte");
        }
    }

    [Test]
    public async Task Encoded_append_is_readable_through_the_classic_WalEntry_seam()
    {
        using var sut = CreateProvider();
        var (segments, offsets) = Encode((0, "x", 42));

        await sut.AppendEncodedBatchAsync(
            TreeId, 0, segments, offsets, _encoder, CancellationToken.None);

        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            collected.Add(entry);
        }

        Assert.That(collected, Has.Count.EqualTo(1));
        Assert.That(collected[0].Offset, Is.EqualTo(0L));
        Assert.That(collected[0].Mutation.Key, Is.EqualTo("x"));
        Assert.That(collected[0].Mutation.Value, Is.EqualTo(new byte[] { 42 }));
    }

    [Test]
    public async Task Classic_append_is_readable_through_the_encoded_seam()
    {
        using var sut = CreateProvider();
        var entry = new WalEntry
        {
            Offset = 0L,
            Mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = "k",
                Value = new byte[] { 7 },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };
        await sut.AppendBatchAsync(TreeId, 0, new[] { entry }, CancellationToken.None);

        var page = await sut.ReadEncodedAsync(TreeId, 0, -1L, 1024, _encoder, CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 0L }));
        var decoded = _encoder.Decode(page.EncodedEntries.Span[0].AsSpan(), TreeId);
        Assert.That(decoded.Key, Is.EqualTo("k"));
        Assert.That(decoded.Value, Is.EqualTo(new byte[] { 7 }));
    }

    [Test]
    public void AppendEncodedBatchAsync_rejects_mismatched_segment_and_offset_counts()
    {
        using var sut = CreateProvider();
        var (segments, _) = Encode((0, "a", 1), (1, "b", 2));
        var offsets = new long[] { 0L };

        Assert.That(
            async () => await sut.AppendEncodedBatchAsync(
                TreeId, 0, segments, offsets, _encoder, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task Encoded_batches_survive_a_provider_restart()
    {
        var (segments, offsets) = Encode((0, "a", 1), (1, "b", 2));
        using (var first = CreateProvider())
        {
            await first.AppendEncodedBatchAsync(
                TreeId, 0, segments, offsets, _encoder, CancellationToken.None);
        }

        using var second = CreateProvider();
        var page = await second.ReadEncodedAsync(TreeId, 0, -1L, 1024, _encoder, CancellationToken.None);
        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 0L, 1L }));
        for (var i = 0; i < segments.Length; i++)
        {
            Assert.That(page.EncodedEntries.Span[i].AsSpan().SequenceEqual(segments[i].AsSpan()), Is.True);
        }
    }
}
