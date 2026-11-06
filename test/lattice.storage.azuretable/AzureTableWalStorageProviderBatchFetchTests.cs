using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Serialization.Session;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Detectors for the concurrent batch fetch behind
/// <see cref="AzureTableWalStorageProvider.ReadAsync"/> and
/// <see cref="AzureTableWalStorageProvider.ReadFilteredAsync"/>
/// (<see cref="AzureTableWalStorageOptions.ReadBatchFetchConcurrency"/>, issue #5877).
/// <para>
/// A cold-restart leaf replay reads its window one batch partition at a time,
/// and with batches averaging seven entries a 256-entry slice was about 39
/// serial table round trips. Fetching ahead must not change what a read
/// returns, so the central detector compares every concurrent read with the
/// serial read of the same window, over randomised layouts.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderBatchFetchTests
{
    private const string TreeId = "tree-batch-fetch";
    private const int ShardIndex = 0;
    private static readonly string[] Keys = ["a", "b", "k", "m", "n", "q", "z"];

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

    private (AzureTableWalStorageProvider Provider, InMemoryWalTable Table, AzureTableWalStorageOptions Options) CreateProvider(
        int concurrency)
    {
        var table = new InMemoryWalTable();
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Tbatchfetch",
            Compression = LatticeCompression.None,
            ReadBatchFetchConcurrency = concurrency,
        };
        var provider = new AzureTableWalStorageProvider(
            Options.Create(options),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)],
            routing: new WalRecordRoutingReader(_services.GetRequiredService<SerializerSessionPool>()));
        return (provider, table, options);
    }

    private static WalEntry Entry(long offset, string key) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = [(byte)offset, (byte)(offset >> 8), 0x2B],
        },
    };

    /// <summary>
    /// Appends <paramref name="sizes"/>.Length batches of the given sizes at
    /// contiguous offsets from zero and makes them readable. Returns each
    /// batch's start offset.
    /// </summary>
    private static async Task<List<long>> AppendBatchesAsync(
        AzureTableWalStorageProvider provider, IReadOnlyList<int> sizes, Random? random = null)
    {
        var starts = new List<long>();
        var offset = 0L;
        foreach (var size in sizes)
        {
            starts.Add(offset);
            var batch = new List<WalEntry>(size);
            for (var i = 0; i < size; i++, offset++)
            {
                batch.Add(Entry(offset, Keys[random?.Next(Keys.Length) ?? (int)(offset % Keys.Length)] + offset));
            }

            await provider.AppendBatchAsync(TreeId, ShardIndex, batch, CancellationToken.None);
        }

        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        return starts;
    }

    private static async Task<List<string>> ReadAllAsync(
        AzureTableWalStorageProvider provider, long fromExclusive, int maxEntries)
    {
        var read = new List<string>();
        await foreach (var entry in provider.ReadAsync(TreeId, ShardIndex, fromExclusive, maxEntries, CancellationToken.None))
        {
            read.Add(Describe(entry));
        }

        return read;
    }

    private static async Task<List<string>> ReadFilteredAsync(
        AzureTableWalStorageProvider provider, long fromExclusive, long toInclusive, int maxEntries, WalKeyFilter filter)
    {
        var read = new List<string>();
        await foreach (var entry in provider.ReadFilteredAsync(
            TreeId, ShardIndex, fromExclusive, toInclusive, maxEntries, filter, CancellationToken.None))
        {
            read.Add(Describe(entry));
        }

        return read;
    }

    private static string Describe(WalEntry entry)
        => $"{entry.Offset}:{entry.Mutation.Kind}:{entry.Mutation.Key}:{Convert.ToHexString(entry.Mutation.Value ?? [])}";

    [Test]
    public void ReadBatchFetchConcurrency_defaults_to_eight()
    {
        Assert.That(new AzureTableWalStorageOptions().ReadBatchFetchConcurrency, Is.EqualTo(8));
        Assert.That(AzureTableWalStorageOptions.DefaultReadBatchFetchConcurrency, Is.EqualTo(8));
    }

    /// <summary>
    /// Detector 1: a concurrent read returns exactly what the serial read
    /// returns - the same entries in the same order, the same entry cap and the
    /// same trailing routing-only entry - over randomised batch sizes, rows
    /// missing from inside batches, trimmed prefixes, windows, caps and filters.
    /// </summary>
    [Test]
    public async Task Concurrent_reads_are_identical_to_serial_reads_over_randomised_layouts(
        [Range(1, 12)] int seed)
    {
        var random = new Random(seed * 7919);
        var (provider, table, options) = CreateProvider(concurrency: 1);
        await using var owned = provider;

        var sizes = Enumerable.Range(0, random.Next(5, 40)).Select(_ => random.Next(1, 13)).ToList();
        var starts = await AppendBatchesAsync(provider, sizes, random);
        var head = starts[^1] + sizes[^1];

        // Rows the manifest extents claim but the table does not hold.
        for (var i = 0; i < head / 6; i++)
        {
            var offset = random.NextInt64(head);
            var batch = starts.FindLastIndex(s => s <= offset);
            table.Remove(
                AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, starts[batch]),
                AzureTableWalStorageProvider.BuildEntryRowKey(offset));
        }

        // A trimmed prefix, which can end mid-batch.
        if (random.Next(2) == 0)
        {
            await provider.TrimAsync(TreeId, ShardIndex, random.NextInt64(head / 2), CancellationToken.None);
        }

        // Queries finish out of the order they were issued, so a reader that
        // consumed batches in completion order would reorder entries. Applied to
        // the concurrent reads only; the serial read is the reference.
        var jitter = new Random(seed);
        Func<TimeSpan> outOfOrder = () => jitter.Next(6) == 0 ? TimeSpan.FromMilliseconds(15) : TimeSpan.Zero;

        for (var window = 0; window < 40; window++)
        {
            var from = random.NextInt64(-1, head);
            var to = random.Next(4) == 0 ? head + 5 : random.NextInt64(from + 1, head + 1);
            var max = random.Next(4) == 0 ? 1024 : random.Next(1, 40);
            var low = Keys[random.Next(Keys.Length)];
            var filter = random.Next(3) == 0 ? default : new WalKeyFilter(low, low + "~");

            options.ReadBatchFetchConcurrency = 1;
            table.QueryLatencySource = null;
            var serialFiltered = await ReadFilteredAsync(provider, from, to, max, filter);
            var serialAll = await ReadAllAsync(provider, from, max);

            options.ReadBatchFetchConcurrency = random.Next(2, 12);
            table.QueryLatencySource = outOfOrder;
            var concurrentFiltered = await ReadFilteredAsync(provider, from, to, max, filter);
            var concurrentAll = await ReadAllAsync(provider, from, max);

            var context = $"seed={seed} window={window} from={from} to={to} max={max} k={options.ReadBatchFetchConcurrency}";
            Assert.That(concurrentFiltered, Is.EqualTo(serialFiltered), "filtered " + context);
            Assert.That(concurrentAll, Is.EqualTo(serialAll), "unfiltered " + context);
        }
    }

    /// <summary>
    /// Detector 2, the red proof for the change: with simulated service time, a
    /// read spanning many batches keeps more than one batch query in service at
    /// once. The serial read never exceeds one.
    /// </summary>
    [Test]
    public async Task Read_spanning_many_batches_overlaps_batch_queries()
    {
        var (provider, table, _) = CreateProvider(concurrency: 8);
        await using var owned = provider;
        await AppendBatchesAsync(provider, Enumerable.Repeat(2, 24).ToList());
        table.QueryLatency = TimeSpan.FromMilliseconds(20);

        var read = await ReadFilteredAsync(provider, -1, 47, 48, new WalKeyFilter("a", "b"));

        Assert.That(read, Is.Not.Empty);
        Assert.That(table.MaxConcurrentQueries, Is.GreaterThan(1));
    }

    [Test]
    public async Task Concurrency_of_one_keeps_one_query_in_service()
    {
        var (provider, table, _) = CreateProvider(concurrency: 1);
        await using var owned = provider;
        await AppendBatchesAsync(provider, Enumerable.Repeat(2, 8).ToList());
        table.QueryLatency = TimeSpan.FromMilliseconds(5);

        await ReadAllAsync(provider, -1, 16);

        Assert.That(table.MaxConcurrentQueries, Is.EqualTo(1));
    }

    /// <summary>
    /// Detector 4: when manifest extents over-state the rows present, the
    /// read fills its window in later rounds, and queries no batch the serial
    /// read would not have queried.
    /// </summary>
    [Test]
    public async Task Extents_larger_than_their_rows_are_filled_by_follow_up_rounds()
    {
        var (provider, table, options) = CreateProvider(concurrency: 8);
        await using var owned = provider;
        var starts = await AppendBatchesAsync(provider, Enumerable.Repeat(4, 10).ToList());
        foreach (var start in starts)
        {
            for (var offset = start + 1; offset < start + 4; offset++)
            {
                table.Remove(
                    AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, start),
                    AzureTableWalStorageProvider.BuildEntryRowKey(offset));
            }
        }

        options.ReadBatchFetchConcurrency = 1;
        var queriesBefore = table.Queries;
        var serial = await ReadAllAsync(provider, -1, 6);
        var serialQueries = table.Queries - queriesBefore;

        options.ReadBatchFetchConcurrency = 8;
        queriesBefore = table.Queries;
        var concurrent = await ReadAllAsync(provider, -1, 6);
        var concurrentQueries = table.Queries - queriesBefore;

        Assert.That(concurrent, Has.Count.EqualTo(6));
        Assert.That(concurrent, Is.EqualTo(serial));
        Assert.That(concurrentQueries, Is.LessThanOrEqualTo(serialQueries));
    }

    /// <summary>
    /// Detector 5: a fault in batch i surfaces at batch i even though a later
    /// batch was fetched successfully, and nothing from the later batch is
    /// yielded.
    /// </summary>
    [Test]
    public async Task Fault_in_a_batch_yields_nothing_from_later_batches()
    {
        var (provider, table, _) = CreateProvider(concurrency: 8);
        await using var owned = provider;
        var starts = await AppendBatchesAsync(provider, Enumerable.Repeat(3, 6).ToList());
        var faulted = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, starts[2]);
        table.BeforeQuery = filter =>
        {
            if (filter.Contains(faulted, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("injected batch fault");
            }
        };

        var yielded = new List<long>();
        var fault = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var entry in provider.ReadAsync(TreeId, ShardIndex, -1, 100, CancellationToken.None))
            {
                yielded.Add(entry.Offset);
            }
        });

        Assert.That(fault!.Message, Is.EqualTo("injected batch fault"));
        Assert.That(yielded, Is.EqualTo(Enumerable.Range(0, (int)starts[2]).Select(i => (long)i)));
    }

    /// <summary>
    /// Detector 3: a cancelled read ends only once every batch fetched ahead
    /// has finished, so no query outlives it.
    /// </summary>
    [Test]
    public async Task Cancelled_read_drains_batches_fetched_ahead()
    {
        var (provider, table, _) = CreateProvider(concurrency: 8);
        await using var owned = provider;
        await AppendBatchesAsync(provider, Enumerable.Repeat(2, 24).ToList());
        table.QueryLatency = TimeSpan.FromMilliseconds(50);

        using var cancellation = new CancellationTokenSource();
        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in provider.ReadAsync(TreeId, ShardIndex, -1, 48, cancellation.Token))
            {
                cancellation.Cancel();
            }
        });

        Assert.That(table.QueriesInService, Is.Zero);
    }
}
