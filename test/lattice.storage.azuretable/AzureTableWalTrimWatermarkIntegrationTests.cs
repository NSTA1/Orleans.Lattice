using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #4621: the Azure Table provider's durable trim watermark. A trim writes
/// the watermark row before it deletes anything; a fault between the two leaves
/// the watermark above entries that still exist, which are never read back; and a
/// shard trimmed before the row existed reports the offset below its lowest entry.
/// Gated under the <c>AzureStorageEmulator</c> category, with a reachability probe
/// that falls through to <see cref="Assert.Inconclusive(string)"/> when Azurite is
/// absent.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalTrimWatermarkIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-tw";
    private const int Shard = 0;

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
            // Best-effort cleanup; a missing table is acceptable.
        }
    }

    private AzureTableWalStorageProvider CreateProvider() =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = false,
            }),
            _serializer);

    private static WalEntry Entry(long offset) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private static async Task<List<long>> ReadAllAsync(AzureTableWalStorageProvider sut)
    {
        var offsets = new List<long>();
        await foreach (var entry in sut.ReadAsync(TreeId, Shard, -1L, 1024, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }

        return offsets;
    }

    private static async Task SeedAsync(AzureTableWalStorageProvider sut)
    {
        await sut.AppendBatchAsync(TreeId, Shard, [Entry(0), Entry(1)], CancellationToken.None);
        await sut.AppendBatchAsync(TreeId, Shard, [Entry(2), Entry(3)], CancellationToken.None);
    }

    [Test]
    public async Task GetTrimWatermarkAsync_reports_the_trim_point_durably()
    {
        var sut = CreateProvider();
        Assert.That(await sut.GetTrimWatermarkAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(-1L));

        await SeedAsync(sut);
        await sut.TrimAsync(TreeId, Shard, 1, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await sut.GetTrimWatermarkAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(1L));
            Assert.That(await CreateProvider().GetTrimWatermarkAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(1L),
                "a fresh instance reads the durable row");
        });
    }

    [Test]
    public async Task A_fault_after_the_watermark_row_leaves_the_covered_entries_stored_and_never_read()
    {
        var sut = CreateProvider();
        await SeedAsync(sut);
        sut.AfterTrimWatermarkRaisedForTesting = () => throw new IOException("lost between the watermark and the delete");

        Assert.That(async () => await sut.TrimAsync(TreeId, Shard, 2, CancellationToken.None), Throws.InstanceOf<IOException>());

        var fresh = CreateProvider();
        var stored = 0;
        await foreach (var _ in _adminClient.GetTableClient(_tableName).QueryAsync<TableEntity>(filter: "RowKey ge 'E' and RowKey lt 'F'"))
        {
            stored++;
        }
        Assert.Multiple(async () =>
        {
            Assert.That(stored, Is.EqualTo(4), "nothing was deleted");
            Assert.That(await fresh.GetTrimWatermarkAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(2L),
                "the watermark is durable before the delete");
            Assert.That(await ReadAllAsync(sut), Is.EqualTo(new long[] { 3 }), "the trimming instance never reads below it");
            Assert.That(await ReadAllAsync(fresh), Is.EqualTo(new long[] { 3 }),
                "nor does another instance once it has read the watermark");
        });
    }

    [Test]
    public async Task A_trim_past_the_committed_tail_caps_the_watermark_at_the_tail()
    {
        var sut = CreateProvider();
        await SeedAsync(sut);
        await sut.TrimAsync(TreeId, Shard, long.MaxValue, CancellationToken.None);
        await sut.AppendBatchAsync(TreeId, Shard, [Entry(4)], CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await CreateProvider().GetTrimWatermarkAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(3L),
                "nothing above the committed tail was trimmed");
            Assert.That(await ReadAllAsync(sut), Is.EqualTo(new long[] { 4 }), "the next batch is readable");
        });
    }

    [Test]
    public async Task A_shard_trimmed_before_the_watermark_row_existed_reports_the_offset_below_its_lowest_entry()
    {
        var sut = CreateProvider();
        await SeedAsync(sut);
        await sut.TrimAsync(TreeId, Shard, 1, CancellationToken.None);

        // Remove the row, as a shard trimmed by an earlier build has none.
        var table = _adminClient.GetTableClient(_tableName);
        await foreach (var row in table.QueryAsync<TableEntity>(filter: $"RowKey eq '{AzureTableWalStorageProvider.TrimWatermarkRowKey}'"))
        {
            await table.DeleteEntityAsync(row.PartitionKey, row.RowKey);
        }

        Assert.That(await CreateProvider().GetTrimWatermarkAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(1L),
            "this provider keeps no readable hole, so every offset below the lowest entry was trimmed");
    }
}
