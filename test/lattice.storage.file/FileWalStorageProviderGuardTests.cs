using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Unit tests for the guard and short-circuit seams on
/// <see cref="FileWalStorageProvider"/>: the empty-batch no-ops that must not
/// touch the filesystem, the argument validation on the encoded read path, the
/// disposal contract, and cancellation observance. These are the boundary
/// behaviours the WAL grain relies on but which the round-trip tests never
/// reach.
/// </summary>
[TestFixture]
public sealed class FileWalStorageProviderGuardTests
{
    private const string TreeId = "tree-guard";

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
        _root = Path.Combine(
            Path.GetTempPath(),
            "lattice-file-wal-guard-tests",
            Guid.NewGuid().ToString("N"));
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
            // Best-effort cleanup; a leaked temp directory does not fail the test.
        }
    }

    private FileWalStorageProvider CreateProvider()
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
        });
        return new FileWalStorageProvider(options, _serializer);
    }

    private static bool AnyShardDirectoryUnder(string root) =>
        System.IO.Directory.EnumerateFileSystemEntries(root).Any();

    [Test]
    public async Task AppendBatchAsync_with_no_entries_is_a_no_op()
    {
        using var provider = CreateProvider();

        await provider.AppendBatchAsync(TreeId, 0, Array.Empty<WalEntry>(), CancellationToken.None);

        Assert.That(
            AnyShardDirectoryUnder(_root),
            Is.False,
            "An empty batch must short-circuit before any shard directory is created.");
    }

    [Test]
    public async Task AppendEncodedBatchAsync_with_no_entries_is_a_no_op()
    {
        using var provider = CreateProvider();

        await provider.AppendEncodedBatchAsync(
            TreeId,
            0,
            ReadOnlyMemory<ArraySegment<byte>>.Empty,
            ReadOnlyMemory<long>.Empty,
            _encoder,
            CancellationToken.None);

        Assert.That(AnyShardDirectoryUnder(_root), Is.False);
    }

    [Test]
    public void ReadEncodedAsync_rejects_a_non_positive_max_entries()
    {
        using var provider = CreateProvider();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => provider.ReadEncodedAsync(TreeId, 0, -1L, 0, _encoder, CancellationToken.None));

        Assert.That(ex!.ParamName, Is.EqualTo("maxEntries"));
    }

    [Test]
    public void ReadEncodedAsync_rejects_a_null_tree_id()
    {
        using var provider = CreateProvider();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => provider.ReadEncodedAsync(null!, 0, -1L, 1, _encoder, CancellationToken.None));
    }

    [Test]
    public void ReadEncodedAsync_rejects_a_null_encoder()
    {
        using var provider = CreateProvider();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => provider.ReadEncodedAsync(TreeId, 0, -1L, 1, null!, CancellationToken.None));
    }

    [Test]
    public async Task ReadEncodedAsync_reports_minus_one_for_an_unknown_shard()
    {
        using var provider = CreateProvider();

        var page = await provider.ReadEncodedAsync(TreeId, 7, -1L, 10, _encoder, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Offsets.Length, Is.Zero);
            Assert.That(page.EncodedEntries.Length, Is.Zero);
            Assert.That(page.HighestOffsetInclusive, Is.EqualTo(-1L));
        });
    }

    [Test]
    public void AppendBatchAsync_observes_an_already_cancelled_token()
    {
        using var provider = CreateProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            () => provider.AppendBatchAsync(TreeId, 0, Array.Empty<WalEntry>(), cts.Token));
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var provider = CreateProvider();
        provider.Dispose();

        Assert.DoesNotThrow(provider.Dispose, "A second dispose must short-circuit.");
    }

    [Test]
    public void Constructor_throws_when_the_options_value_is_null()
    {
        var options = Substitute.For<IOptions<FileWalStorageOptions>>();
        options.Value.Returns((FileWalStorageOptions)null!);

        var ex = Assert.Throws<ArgumentException>(
            () => new FileWalStorageProvider(options, _serializer));

        Assert.That(ex!.ParamName, Is.EqualTo("options"));
    }

    [Test]
    public async Task A_zero_length_payload_round_trips_as_an_empty_array()
    {
        // A tombstone-shaped entry encodes to a zero-byte payload. The read path
        // must skip the seek entirely and still hand back a distinct empty array
        // rather than null.
        using var provider = CreateProvider();
        var segments = new[] { new ArraySegment<byte>(Array.Empty<byte>()) };

        await provider.AppendEncodedBatchAsync(
            TreeId,
            0,
            segments,
            new[] { 0L },
            _encoder,
            CancellationToken.None);

        var page = await provider.ReadEncodedAsync(TreeId, 0, -1L, 10, _encoder, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Offsets.Length, Is.EqualTo(1));
            Assert.That(page.EncodedEntries.Span[0].Count, Is.Zero);
            Assert.That(page.HighestOffsetInclusive, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Operations_after_dispose_throw_ObjectDisposedException()
    {
        var provider = CreateProvider();
        provider.Dispose();

        Assert.ThrowsAsync<ObjectDisposedException>(
            () => provider.AppendBatchAsync(TreeId, 0, new[] { Entry(0) }, CancellationToken.None));
    }

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
        },
    };
}
