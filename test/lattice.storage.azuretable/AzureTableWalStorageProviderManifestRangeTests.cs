using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Pins that every read-from-offset path of
/// <see cref="AzureTableWalStorageProvider"/> seeks the manifest partition by
/// row key from the lowest batch start that can overlap the wanted offset,
/// rather than scanning the whole retained manifest prefix and filtering on
/// the non-key <c>Offset</c> column (issue #2155), and that the bounded scan
/// still returns exactly the wanted entries when the read starts mid-batch,
/// on a batch boundary, or past a full-width batch. Driven against an
/// in-memory table so it runs in the default filter; the Azurite counterpart
/// lives in <see cref="AzureTableWalStorageProviderReadTrimIntegrationTests"/>.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderManifestRangeTests
{
    private const string TreeId = "tree-range";
    private const int ShardIndex = 0;

    // Batch sizes chosen so a read can start inside a full-width batch, on the
    // first offset of a batch, and inside a short batch that follows a
    // full-width one. Batches span [0,99], [100,136], [137,236], [237,241].
    private static readonly int[] BatchSizes = [100, 37, 100, 5];
    private const long TotalEntries = 242;

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

    private async Task<(AzureTableWalStorageProvider Provider, List<string> ManifestFilters)> SeededAsync()
    {
        var table = new InMemoryWalTable();
        var provider = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ServiceClient = table.BuildServiceClient(),
                TableName = "Trange",
                Compression = LatticeCompression.None,
            }),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)]);

        var next = 0L;
        foreach (var size in BatchSizes)
        {
            await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(next, size), CancellationToken.None);
            next += size;
        }

        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var manifestPrefix = $"PartitionKey eq '{AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex)}'";
        var manifestFilters = new List<string>();
        table.BeforeQuery = filter =>
        {
            if (filter.StartsWith(manifestPrefix, StringComparison.Ordinal))
            {
                manifestFilters.Add(filter);
            }
        };

        return (provider, manifestFilters);
    }

    private static List<WalEntry> Entries(long firstOffset, int count) =>
        Enumerable.Range(0, count)
            .Select(i => new WalEntry
            {
                Offset = firstOffset + i,
                Mutation = new LatticeMutation
                {
                    Key = $"k{firstOffset + i}",
                    Value = [(byte)(firstOffset + i), 0x3C],
                },
            })
            .ToList();

    private static IEnumerable<long> Expected(long fromOffsetExclusive, int maxEntries) =>
        Enumerable.Range(0, (int)Math.Min(maxEntries, TotalEntries - (fromOffsetExclusive + 1)))
            .Select(i => fromOffsetExclusive + 1 + i);

    private static string ExpectedLowerBound(long fromOffsetExclusive) =>
        $"RowKey ge '{AzureTableWalStorageProvider.BuildManifestRowKey(Math.Max(0L, fromOffsetExclusive + 1 - AzureTableWalStorageProvider.MaxEntriesPerBatch + 1))}'";

    // 149: mid-batch inside [137,236]; 136: wants 137, the first offset of a
    // batch; 99: wants 100, the first offset past a full-width batch; 239:
    // inside the short last batch; -1: the whole log.
    [TestCase(149L)]
    [TestCase(136L)]
    [TestCase(99L)]
    [TestCase(239L)]
    [TestCase(-1L)]
    public async Task ReadAsync_seeks_the_manifest_from_the_lowest_overlapping_start(long fromOffsetExclusive)
    {
        var (provider, manifestFilters) = await SeededAsync();
        await using var _ = provider;

        var read = new List<long>();
        await foreach (var entry in provider.ReadAsync(TreeId, ShardIndex, fromOffsetExclusive, 120, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }

        Assert.That(read, Is.EqualTo(Expected(fromOffsetExclusive, 120)));
        Assert.That(manifestFilters, Is.Not.Empty);
        Assert.That(manifestFilters, Has.All.Contains(ExpectedLowerBound(fromOffsetExclusive)));
    }

    [TestCase(149L)]
    [TestCase(136L)]
    [TestCase(99L)]
    public async Task ReadEncodedAsync_seeks_the_manifest_from_the_lowest_overlapping_start(long fromOffsetExclusive)
    {
        var (provider, manifestFilters) = await SeededAsync();
        await using var _ = provider;

        var page = await provider.ReadEncodedAsync(
            TreeId, ShardIndex, fromOffsetExclusive, 60, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(Expected(fromOffsetExclusive, 60)));
        Assert.That(manifestFilters, Is.Not.Empty);
        Assert.That(manifestFilters, Has.All.Contains(ExpectedLowerBound(fromOffsetExclusive)));
    }

    [TestCase(149L)]
    [TestCase(136L)]
    public async Task ReadFilteredAsync_seeks_the_manifest_from_the_lowest_overlapping_start(long fromOffsetExclusive)
    {
        var (provider, manifestFilters) = await SeededAsync();
        await using var _ = provider;

        var read = new List<long>();
        await foreach (var entry in provider.ReadFilteredAsync(
            TreeId, ShardIndex, fromOffsetExclusive, TotalEntries - 1, 60, new WalKeyFilter(null, null), CancellationToken.None))
        {
            read.Add(entry.Offset);
        }

        Assert.That(read, Is.EqualTo(Expected(fromOffsetExclusive, 60)));
        Assert.That(manifestFilters, Is.Not.Empty);
        Assert.That(manifestFilters, Has.All.Contains(ExpectedLowerBound(fromOffsetExclusive)));
    }
}
