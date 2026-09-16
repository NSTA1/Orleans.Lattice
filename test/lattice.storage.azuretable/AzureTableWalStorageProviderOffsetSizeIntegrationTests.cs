using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// End-to-end tests for the retained-size and lowest-offset query
/// methods of <see cref="AzureTableWalStorageProvider"/>
/// (<see cref="AzureTableWalStorageProvider.GetRetainedByteSizeAsync"/>
/// and <see cref="AzureTableWalStorageProvider.GetLowestOffsetAsync"/>),
/// driven against a live Azure Table Storage endpoint (canonically
/// Azurite on the default development connection string). Gated under
/// the <c>AzureStorageEmulator</c> NUnit category. Each test uses a
/// fresh, GUID-named table torn down afterward, and a per-test unique
/// tree id, so the fixture is safe to run concurrently with other
/// emulator suites sharing the same Azurite instance.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalStorageProviderOffsetSizeIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private string _treeId = null!;
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
                + $"Start it or skip the AzureStorageEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _tableName = "T" + Guid.NewGuid().ToString("N");
        _treeId = "tree-os-" + Guid.NewGuid().ToString("N");
        _sut = CreateProvider(_tableName);
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

    private AzureTableWalStorageProvider CreateProvider(string tableName) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = tableName,
                Compression = LatticeCompression.None,
                // Synchronous phase-2 so a batch is fully committed
                // (manifest M-rows + TAIL present) the instant
                // AppendBatchAsync returns, which the size/offset
                // queries below rely on.
                PipelinePhaseTwoCommits = false,
            }),
            _serializer);

    private WalEntry Entry(long offset, byte tag = 1) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = _treeId,
            Kind = MutationKind.Set,
            Key = "k" + offset.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Value = new byte[] { tag, tag, tag, tag },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_for_empty_partition()
    {
        var low = await _sut.GetLowestOffsetAsync(_treeId, 0, CancellationToken.None);

        Assert.That(low, Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_lowest_committed_offset_after_append()
    {
        await _sut.AppendBatchAsync(
            _treeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var low = await _sut.GetLowestOffsetAsync(_treeId, 0, CancellationToken.None);

        Assert.That(low, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_advances_past_trimmed_entries_within_boundary_batch()
    {
        await _sut.AppendBatchAsync(
            _treeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        // Trim through offset 1: the boundary batch keeps its manifest
        // M-row but per-row deletes entries 0 and 1, so the lowest
        // extant entry offset must advance to 2 via the forward-walk.
        await _sut.TrimAsync(_treeId, 0, throughOffsetInclusive: 1L, CancellationToken.None);

        var low = await _sut.GetLowestOffsetAsync(_treeId, 0, CancellationToken.None);

        Assert.That(low, Is.EqualTo(2L));
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_returns_zero_for_empty_partition()
    {
        var size = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);

        Assert.That(size, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_returns_positive_total_after_append()
    {
        await _sut.AppendBatchAsync(
            _treeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var size = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);

        Assert.That(size, Is.GreaterThan(0L),
            "the summed manifest PayloadBytes must reflect the appended batch's encoded payload");
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_sums_across_multiple_batches()
    {
        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        var afterFirst = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);

        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        var afterSecond = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);

        Assert.That(afterSecond, Is.GreaterThan(afterFirst),
            "a second committed batch must increase the retained-byte total");
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_falls_when_a_fully_covered_batch_is_trimmed()
    {
        // The reclamation property this provider relies on: because
        // TrimAsync deletes the entry rows and the manifest row of a
        // fully-covered batch outright, the byte total it is summed
        // from must fall. A log-structured backend that only advances
        // a watermark would hold this total flat.
        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        var beforeTrim = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);

        await _sut.TrimAsync(_treeId, 0, throughOffsetInclusive: 1L, CancellationToken.None);

        var afterTrim = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);
        Assert.That(afterTrim, Is.LessThan(beforeTrim),
            "trimming a fully-covered batch must return its bytes, not merely mark them dead");
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_returns_zero_once_every_batch_is_trimmed()
    {
        // Trim-is-reclamation, stated absolutely: a fully-trimmed shard
        // retains nothing. There is no dead-byte residue awaiting a
        // later compaction pass, which is why this provider needs no
        // compaction trigger and no physical-byte accounting.
        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);
        Assert.That(
            await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None),
            Is.GreaterThan(0L),
            "sanity: the appended batches must register before the trim");

        await _sut.TrimAsync(_treeId, 0, throughOffsetInclusive: 3L, CancellationToken.None);

        var afterTrim = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);
        Assert.That(afterTrim, Is.EqualTo(0L),
            "a fully-trimmed shard must retain zero bytes, leaving no residue to compact");
    }

    [Test]
    public async Task GetRetainedByteSizeAsync_over_reports_a_partially_trimmed_boundary_batch()
    {
        // The one inaccuracy in this provider's byte accounting, pinned
        // so its direction cannot silently invert. A boundary batch
        // keeps its manifest row - and so its full PayloadBytes - until
        // it is itself fully trimmed, so the total reads HIGH by at
        // most one batch. Over-reporting is the safe direction for a
        // capacity ceiling; the file provider's defect was an
        // unbounded under-report, which is the dangerous one.
        await _sut.AppendBatchAsync(_treeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        var wholeBatch = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);

        await _sut.TrimAsync(_treeId, 0, throughOffsetInclusive: 0L, CancellationToken.None);

        var afterPartialTrim = await _sut.GetRetainedByteSizeAsync(_treeId, 0, CancellationToken.None);
        Assert.That(afterPartialTrim, Is.EqualTo(wholeBatch),
            "a partially-trimmed boundary batch keeps its manifest row, so the total is unchanged");
        Assert.That(
            await _sut.GetLowestOffsetAsync(_treeId, 0, CancellationToken.None),
            Is.EqualTo(1L),
            "the trimmed entry row is nonetheless gone, so the over-report is bounded by one batch");
    }
}
