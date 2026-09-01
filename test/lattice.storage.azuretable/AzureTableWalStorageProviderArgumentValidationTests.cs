using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Argument-validation tests for the public append / read surface of
/// <see cref="AzureTableWalStorageProvider"/>. Every case here throws (or
/// early-returns) <i>before</i> the provider touches an Azure Tables
/// endpoint - an empty batch, an over-large batch, a negative or
/// non-dense offset, a below-one read count, and use after disposal - so
/// the fixture is a pure in-process unit test with no emulator
/// dependency. It pins the misuse contract callers rely on: a rejected
/// batch fails fast and leaves observable state untouched.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderArgumentValidationTests
{
    private const string TreeId = "tree-args";
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

    private AzureTableWalStorageProvider CreateProvider() =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = "UseDevelopmentStorage=true",
                TableName = "Targs" + Guid.NewGuid().ToString("N"),
                Compression = LatticeCompression.None,
            }),
            _serializer);

    private static WalEntry Entry(long offset) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    [Test]
    public async Task AppendBatchAsync_returns_without_io_for_an_empty_batch()
    {
        await using var sut = CreateProvider();

        // An empty batch is a no-op that must return before any table I/O
        // (proven by the call completing without a reachable emulator).
        Assert.DoesNotThrowAsync(() =>
            sut.AppendBatchAsync(TreeId, ShardIndex, Array.Empty<WalEntry>(), CancellationToken.None));
    }

    [Test]
    public async Task AppendBatchAsync_rejects_a_negative_first_offset()
    {
        await using var sut = CreateProvider();

        Assert.That(
            () => sut.AppendBatchAsync(TreeId, ShardIndex, new[] { Entry(-1L) }, CancellationToken.None),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task AppendEncodedBatchAsync_returns_without_io_for_an_empty_batch()
    {
        await using var sut = CreateProvider();
        var encoder = Substitute.For<IWalRecordEncoder>();

        Assert.DoesNotThrowAsync(() => sut.AppendEncodedBatchAsync(
            TreeId,
            ShardIndex,
            ReadOnlyMemory<ArraySegment<byte>>.Empty,
            ReadOnlyMemory<long>.Empty,
            encoder,
            CancellationToken.None));
    }

    [Test]
    public async Task AppendEncodedBatchAsync_rejects_a_batch_above_the_per_call_limit()
    {
        await using var sut = CreateProvider();
        var encoder = Substitute.For<IWalRecordEncoder>();

        // One past the per-call cap of MaxEntriesPerBatch (100). The
        // segment and offset counts are parallel so the count guard, not
        // the length-mismatch guard, is the one that fires.
        const int tooMany = AzureTableWalStorageProvider.MaxEntriesPerBatch + 1;
        var segments = new ArraySegment<byte>[tooMany];
        var offsets = new long[tooMany];
        for (var i = 0; i < tooMany; i++)
        {
            segments[i] = new ArraySegment<byte>(Array.Empty<byte>());
            offsets[i] = i;
        }

        Assert.That(
            () => sut.AppendEncodedBatchAsync(TreeId, ShardIndex, segments, offsets, encoder, CancellationToken.None),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task AppendEncodedBatchAsync_rejects_a_negative_first_offset()
    {
        await using var sut = CreateProvider();
        var encoder = Substitute.For<IWalRecordEncoder>();

        var segments = new[] { new ArraySegment<byte>(new byte[] { 1 }), new ArraySegment<byte>(new byte[] { 2 }) };
        var offsets = new long[] { -1L, 0L };

        Assert.That(
            () => sut.AppendEncodedBatchAsync(TreeId, ShardIndex, segments, offsets, encoder, CancellationToken.None),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task AppendEncodedBatchAsync_rejects_non_dense_offsets()
    {
        await using var sut = CreateProvider();
        var encoder = Substitute.For<IWalRecordEncoder>();

        // First offset is valid but the sequence skips a slot (5, 7), so
        // ValidateDenseOffsets must reject it ahead of any I/O.
        var segments = new[] { new ArraySegment<byte>(new byte[] { 1 }), new ArraySegment<byte>(new byte[] { 2 }) };
        var offsets = new long[] { 5L, 7L };

        Assert.That(
            () => sut.AppendEncodedBatchAsync(TreeId, ShardIndex, segments, offsets, encoder, CancellationToken.None),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task ReadAsync_rejects_a_max_entries_below_one()
    {
        await using var sut = CreateProvider();

        // ReadAsync is an async iterator, so the guard fires only once
        // enumeration starts; drive it with an await foreach.
        Assert.That(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync(TreeId, ShardIndex, -1L, 0, CancellationToken.None))
                {
                }
            },
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task AppendBatchAsync_throws_after_disposal()
    {
        var sut = CreateProvider();
        await sut.DisposeAsync();

        Assert.That(
            () => sut.AppendBatchAsync(TreeId, ShardIndex, new[] { Entry(0L) }, CancellationToken.None),
            Throws.TypeOf<ObjectDisposedException>());
    }
}
