using System.Globalization;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Azurite-backed integration tests for
/// <see cref="AzureTableWalStorageProvider.ReconcileAsync"/>. The
/// tests directly synthesise "orphan" batch partitions in Azure
/// Tables, simulating a silo crash between phase 1 (entry rows
/// committed atomically into a per-batch partition) and phase 2
/// (manifest row + TAIL upsert committed via the per-shard
/// <c>PhaseTwoWorker</c>), then assert reconciliation either rolls
/// the orphan forward into the manifest or rolls it back by
/// deleting its entry rows.
/// <para>
/// Gated under the <c>AzureTableEmulator</c> category exactly like
/// the rest of the integration suite; the dev loop skips them.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureTableEmulator")]
public class AzureTableWalStorageProviderReconcileIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-recon";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private AzureTableWalStorageProvider _sut = null!;
    private TableClient _table = null!;

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
    public async Task SetUp()
    {
        _tableName = "T" + Guid.NewGuid().ToString("N");
        _sut = CreateProvider(_tableName);
        // Force the provider to create the table by issuing one cheap
        // read; this lets the orphan-synthesis helpers below open a
        // TableClient against the same table without racing the
        // provider's lazy-init.
        await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        _table = new TableClient(AzuriteConnectionString, _tableName);
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
        }
    }

    private AzureTableWalStorageProvider CreateProvider(string tableName) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = tableName,
                // Pin synchronous phase-2 commits so a committed batch's
                // TAIL and manifest rows are durable the instant
                // AppendBatchAsync returns. The reconciliation tests
                // synthesise orphans and then assert on the post-commit
                // tail directly; the throughput default
                // (PipelinePhaseTwoCommits = true) would defer a real
                // batch's phase-2 commit and race the assertions.
                PipelinePhaseTwoCommits = false,
            }),
            _serializer);

    /// <summary>
    /// Synthesises an "orphan" batch: writes entry rows directly
    /// under the per-batch partition and a phase-0 candidate-row
    /// (<c>C{startOffset:D19}</c>) directly under the shard's
    /// manifest partition, but skips the phase-2 worker so the
    /// manifest M-row and TAIL upsert never land. Models a silo
    /// crash between phase 0/1 and phase 2.
    /// </summary>
    private async Task SynthesiseOrphanBatchAsync(string treeId, int shardIndex, long startOffset, int entryCount)
    {
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(treeId, shardIndex, startOffset);
        var endOffsetInclusive = startOffset + entryCount - 1;
        var actions = new List<TableTransactionAction>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var offset = startOffset + i;
            var mutation = new LatticeMutation
            {
                TreeId = treeId,
                Kind = MutationKind.Set,
                Key = string.Create(CultureInfo.InvariantCulture, $"k{offset}"),
                Value = new byte[] { (byte)i },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            };
            var entry = new WalEntry { Offset = offset, Mutation = mutation };
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = batchPartitionKey,
                    RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(offset),
                    Offset = offset,
                    Payload = _serializer.SerializeToArray(
                        Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.ToWalRecord(
                            entry.Mutation,
                            LatticeMergeMode.LwwRegister,
                            string.Empty)),
                }));
        }
        await _table.SubmitTransactionAsync(actions, CancellationToken.None);

        // Phase 0: stamp the candidate-row that the production
        // append path writes alongside phase 1. The reconciler
        // discovers orphans by scanning these rows; without it the
        // synthesised orphan is invisible to ReconcileAsync.
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(treeId, shardIndex);
        await _table.UpsertEntityAsync(
            new AzureTableWalEntity
            {
                PartitionKey = manifestPartitionKey,
                RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(startOffset),
                Offset = endOffsetInclusive,
                Payload = null,
            },
            TableUpdateMode.Replace,
            CancellationToken.None);
    }

    private async Task<int> CountEntryRowsAsync(string treeId, int shardIndex, long startOffset)
    {
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(treeId, shardIndex, startOffset);
        var count = 0;
        await foreach (var _ in _table.QueryAsync<AzureTableWalEntity>(
            $"PartitionKey eq '{batchPartitionKey}' and RowKey ge 'E' and RowKey lt 'F'",
            cancellationToken: CancellationToken.None))
        {
            count++;
        }
        return count;
    }

    [Test]
    public async Task ReconcileAsync_no_orphans_no_committed_state_is_a_noop()
    {
        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public async Task ReconcileAsync_rolls_forward_a_single_contiguous_orphan_on_a_fresh_shard()
    {
        // Synthesise an orphan covering offsets [0, 4] with no
        // committed TAIL. Reconciliation should add the manifest
        // row and advance TAIL to 4.
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 5);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(4L));

        // The reconciled batch's entries must now be readable via
        // the normal ReadAsync path (which walks the manifest).
        var read = new List<long>();
        await foreach (var entry in _sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        Assert.That(read, Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
    }

    [Test]
    public async Task ReconcileAsync_rolls_forward_an_orphan_contiguous_with_an_existing_tail()
    {
        // Commit a batch via the normal path (TAIL = 1) then
        // synthesise an orphan at [2, 4] - reconciliation should
        // advance TAIL to 4.
        await _sut.AppendBatchAsync(TreeId, 0, new[]
        {
            Entry(0L), Entry(1L),
        }, CancellationToken.None);

        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 2L, entryCount: 3);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(4L));
    }

    [Test]
    public async Task ReconcileAsync_rolls_back_an_orphan_with_a_gap_below_it()
    {
        // Commit [0, 1] via the normal path (TAIL = 1) then
        // synthesise an orphan at [10, 14] - a gap below it (offsets
        // 2..9 missing) means rollback. Entry rows must be gone
        // and TAIL must not advance.
        await _sut.AppendBatchAsync(TreeId, 0, new[]
        {
            Entry(0L), Entry(1L),
        }, CancellationToken.None);

        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 10L, entryCount: 5);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(1L), "TAIL must not advance over a gap");

        var orphanRows = await CountEntryRowsAsync(TreeId, 0, startOffset: 10L);
        Assert.That(orphanRows, Is.Zero, "Rolled-back orphan entry rows must be deleted");
    }

    [Test]
    public async Task ReconcileAsync_rolls_back_orphan_after_first_contiguity_break()
    {
        // TAIL = 1 (from a committed batch). Orphans: [2, 4]
        // contiguous - rollforward; [20, 22] gap - rollback.
        await _sut.AppendBatchAsync(TreeId, 0, new[]
        {
            Entry(0L), Entry(1L),
        }, CancellationToken.None);

        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 2L, entryCount: 3);
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 20L, entryCount: 3);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(4L), "TAIL advances only through the rollforward prefix");

        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 2L), Is.EqualTo(3),
            "Rolled-forward orphan entry rows must be preserved");
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 20L), Is.Zero,
            "Rolled-back orphan entry rows must be deleted");
    }

    [Test]
    public async Task ReconcileAsync_is_idempotent()
    {
        // First reconcile rolls an orphan forward; second reconcile
        // observes the now-committed manifest row and must be a
        // no-op. TAIL must not change, entry rows must not be
        // touched.
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var headAfterFirst = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(headAfterFirst, Is.EqualTo(2L));

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var headAfterSecond = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(headAfterSecond, Is.EqualTo(2L), "Second reconcile must not advance TAIL further");
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 0L), Is.EqualTo(3),
            "Reconciled orphan entry rows must be preserved across reconciliation passes");
    }

    [Test]
    public async Task ReconcileAsync_chains_multiple_contiguous_orphans_into_a_single_tail_advance()
    {
        // Three orphans, all contiguous from offset 0: [0,2], [3,5],
        // [6,8]. Reconciliation should roll all three forward and
        // advance TAIL to 8.
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 3L, entryCount: 3);
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 6L, entryCount: 3);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(8L));

        var read = new List<long>();
        await foreach (var entry in _sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        Assert.That(read, Is.EqualTo(new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }));
    }

    [Test]
    public async Task ReconcileAsync_isolates_shards_within_a_tree()
    {
        // Orphan in shard 0 must not affect shard 1, and vice versa.
        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);
        await _sut.AppendBatchAsync(TreeId, 1, new[] { Entry(0L), Entry(1L) }, CancellationToken.None);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
        Assert.That(await _sut.GetHighestOffsetAsync(TreeId, 1, CancellationToken.None), Is.EqualTo(1L),
            "Reconciling shard 0 must not affect shard 1's TAIL");
    }

    [Test]
    public async Task ReconcileAsync_isolates_distinct_trees()
    {
        // Reconciling tree A must not affect tree B, even with
        // orphans in both trees on the same shard index.
        const string OtherTree = "tree-recon-other";

        await SynthesiseOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 2);
        await SynthesiseOrphanBatchAsync(OtherTree, 0, startOffset: 0L, entryCount: 4);

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(1L));
        Assert.That(await _sut.GetHighestOffsetAsync(OtherTree, 0, CancellationToken.None), Is.EqualTo(-1L),
            "Other tree must remain unreconciled");
    }

    [Test]
    public void ReconcileAsync_observes_a_pre_cancelled_token()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _sut.ReconcileAsync(TreeId, 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ReconcileAsync_rejects_null_treeId()
    {
        Assert.That(
            async () => await _sut.ReconcileAsync(null!, 0, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    private static WalEntry Entry(long offset) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = string.Create(CultureInfo.InvariantCulture, $"k{offset}"),
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };
}
