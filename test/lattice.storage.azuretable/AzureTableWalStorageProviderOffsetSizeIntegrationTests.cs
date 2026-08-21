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
/// the <c>AzureTableEmulator</c> NUnit category. Each test uses a
/// fresh, GUID-named table torn down afterward, and a per-test unique
/// tree id, so the fixture is safe to run concurrently with other
/// emulator suites sharing the same Azurite instance.
/// </summary>
[TestFixture]
[Category("AzureTableEmulator")]
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
                + $"Start it or skip the AzureTableEmulator category. "
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
}
