using System.Reflection;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Emulator-backed tests for the residual read / trim / recovery
/// branches of <see cref="AzureTableWalStorageProvider"/> the primary
/// integration suite does not reach: the outer <c>maxEntries</c> cut-off
/// that stops a read at a batch boundary, the mid-partition transactional
/// flush inside the chunked trim delete, the lowest-offset scan falling
/// through when every batch partition is empty, the empty-payload
/// deserialisation fall-back, the pipelined phase-2 dispatch, and the
/// idempotent candidate-row rollback swallowing a 404. Gated under the
/// <c>AzureStorageEmulator</c> category exactly like the primary suite,
/// with a reachability probe that falls through to
/// <see cref="Assert.Inconclusive(string)"/> when Azurite is absent.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalStorageProviderReadTrimIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-rt";
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
                + $"Start it via 'azurite --silent --location <dir>' or skip the AzureStorageEmulator category. "
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

    private AzureTableWalStorageProvider CreateProvider(bool pipeline = false) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = pipeline,
            }),
            _serializer);

    private static WalEntry Entry(long offset, string key = "k") => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private static async Task<List<WalEntry>> ReadAllAsync(
        AzureTableWalStorageProvider sut,
        long fromOffsetExclusive = -1L,
        int maxEntries = 1024)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, Shard, fromOffsetExclusive, maxEntries, CancellationToken.None))
        {
            collected.Add(entry);
        }
        return collected;
    }

    [Test]
    public async Task ReadAsync_stops_at_maxEntries_on_a_later_batch_boundary()
    {
        // Two single-entry batches produce two manifest rows. A read
        // capped at one entry yields the first batch's entry, then the
        // outer manifest loop advances to the second manifest row with
        // the budget already spent, tripping the outer yield-break.
        await using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, Shard, new[] { Entry(0L) }, CancellationToken.None).ConfigureAwait(false);
        await sut.AppendBatchAsync(TreeId, Shard, new[] { Entry(1L) }, CancellationToken.None).ConfigureAwait(false);

        var collected = await ReadAllAsync(sut, fromOffsetExclusive: -1L, maxEntries: 1).ConfigureAwait(false);

        Assert.That(collected, Has.Count.EqualTo(1), "the read budget of one entry must halt at the second batch boundary");
        Assert.That(collected[0].Offset, Is.EqualTo(0L));
    }

    [Test]
    public async Task TrimAsync_flushes_a_full_transaction_chunk_mid_partition()
    {
        // A single 100-entry batch fills exactly one transaction chunk.
        // A full-coverage trim streams all 100 entry rows and must flush
        // the moment the pending buffer reaches the 100-action cap, then
        // find nothing left for the trailing flush.
        await using var sut = CreateProvider();
        var entries = new WalEntry[AzureTableWalStorageProvider.MaxEntriesPerBatch];
        for (var i = 0; i < entries.Length; i++)
        {
            entries[i] = Entry(i, key: "k" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        await sut.AppendBatchAsync(TreeId, Shard, entries, CancellationToken.None).ConfigureAwait(false);

        var before = await ReadAllAsync(sut).ConfigureAwait(false);
        Assert.That(before, Has.Count.EqualTo(AzureTableWalStorageProvider.MaxEntriesPerBatch),
            "sanity: the full 100-entry batch must be readable before the trim");

        await sut.TrimAsync(TreeId, Shard, throughOffsetInclusive: 99L, CancellationToken.None).ConfigureAwait(false);

        var after = await ReadAllAsync(sut).ConfigureAwait(false);
        Assert.That(after, Is.Empty, "a full-coverage trim must remove every entry and its manifest row");
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_negative_one_when_the_batch_partition_is_empty()
    {
        // Simulate a crash between the entry-row delete and the manifest
        // delete: the sole entry row is gone but its manifest row
        // remains. The lowest-offset scan finds the manifest row, walks
        // forward into an empty batch partition, and must fall through to
        // -1 rather than report a phantom offset.
        await using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, Shard, new[] { Entry(0L) }, CancellationToken.None).ConfigureAwait(false);

        var raw = new TableClient(AzuriteConnectionString, _tableName);
        await raw.DeleteEntityAsync(
            AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, Shard, 0L),
            AzureTableWalStorageProvider.BuildEntryRowKey(0L),
            ETag.All,
            CancellationToken.None).ConfigureAwait(false);

        var low = await sut.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None).ConfigureAwait(false);

        Assert.That(low, Is.EqualTo(-1L));
    }

    [Test]
    public async Task ReadAsync_yields_a_default_mutation_for_an_empty_payload_row()
    {
        // Overwrite a committed entry row with a null payload (the
        // defensive shape the deserialiser tolerates). The read must
        // still yield the row at its offset, projecting a default
        // mutation rather than throwing.
        await using var sut = CreateProvider();
        await sut.AppendBatchAsync(TreeId, Shard, new[] { Entry(0L) }, CancellationToken.None).ConfigureAwait(false);

        var raw = new TableClient(AzuriteConnectionString, _tableName);
        var emptyPayloadRow = new AzureTableWalEntity
        {
            PartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, Shard, 0L),
            RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(0L),
            Offset = 0L,
            Payload = null,
            Compression = (int)LatticeCompression.None,
        };
        await raw.UpsertEntityAsync(emptyPayloadRow, TableUpdateMode.Replace, CancellationToken.None).ConfigureAwait(false);

        var collected = await ReadAllAsync(sut).ConfigureAwait(false);

        Assert.That(collected, Has.Count.EqualTo(1));
        Assert.That(collected[0].Offset, Is.EqualTo(0L));
        Assert.That(collected[0].Mutation.TreeId, Is.Null, "an empty payload must project the default mutation");
    }

    [Test]
    public async Task AppendBatchAsync_pipelined_commits_the_previous_batch_on_the_next_append()
    {
        // In pipelined phase-2 mode, a batch's manifest commit is
        // deferred until the next append on the shard drains the slot.
        // After two appends the first batch is durably committed, so it
        // is readable; this drives the pipelined dispatch branch that the
        // synchronous integration suite never takes.
        await using var sut = CreateProvider(pipeline: true);
        await sut.AppendBatchAsync(TreeId, Shard, new[] { Entry(0L) }, CancellationToken.None).ConfigureAwait(false);
        await sut.AppendBatchAsync(TreeId, Shard, new[] { Entry(1L) }, CancellationToken.None).ConfigureAwait(false);

        var collected = await ReadAllAsync(sut, maxEntries: 10).ConfigureAwait(false);

        Assert.That(collected.Select(e => e.Offset), Does.Contain(0L),
            "the first batch's pipelined commit must land once the second append drains the slot");
    }

    [Test]
    public async Task RollBackOrphanAsync_swallows_a_missing_candidate_row()
    {
        // A legacy orphan (HasCandidateRow = true) whose candidate row is
        // already gone: rollback wipes the (empty) batch partition, then
        // issues an unconditional candidate-row delete that 404s. That
        // 404 must be swallowed so a concurrent reconciliation pass stays
        // idempotent.
        var raw = new TableClient(AzuriteConnectionString, _tableName);
        await raw.CreateIfNotExistsAsync().ConfigureAwait(false);

        var orphan = new AzureTableWalStorageProvider.OrphanBatch(
            StartOffset: 0L,
            EndOffsetInclusive: 0L,
            BatchPartitionKey: AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, Shard, 0L),
            HasCandidateRow: true);
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, Shard);

        var method = typeof(AzureTableWalStorageProvider).GetMethod(
            "RollBackOrphanAsync",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.That(method, Is.Not.Null, "RollBackOrphanAsync must be resolvable by reflection");

        Assert.DoesNotThrowAsync(async () =>
        {
            var task = (Task)method!.Invoke(
                null,
                new object[] { raw, manifestPartitionKey, orphan, CancellationToken.None })!;
            await task.ConfigureAwait(false);
        });
    }
}
