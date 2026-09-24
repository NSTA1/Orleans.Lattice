using System.Globalization;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Azurite-backed coverage for
/// <see cref="AzureTableWalStorageProvider.ReconcileAsync"/> meeting a
/// shard a concurrent committer has touched (#3348): a manifest row
/// that landed above a lowered TAIL must be treated as committed rather
/// than re-added (which 409s forever), and reconciliation must wait for
/// the provider's own in-motion writes before it scans.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalStorageProviderReconcileConcurrencyIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-recon-race";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
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
    public void SetUp()
    {
        _tableName = "T" + Guid.NewGuid().ToString("N");
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

    private async Task<AzureTableWalStorageProvider> CreateProviderAsync(bool eliminateCandidateRow)
    {
        var provider = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = false,
                EliminateCandidateRowOnHotPath = eliminateCandidateRow,
            }),
            _serializer);
        await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        return provider;
    }

    private static string ManifestPartitionKey => AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0);

    private async Task WriteEntryRowsAsync(long startOffset, int entryCount)
    {
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, 0, startOffset);
        var actions = new List<TableTransactionAction>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var offset = startOffset + i;
            var mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = string.Create(CultureInfo.InvariantCulture, $"k{offset}"),
                Value = new byte[] { (byte)i },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            };
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = batchPartitionKey,
                    RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(offset),
                    Offset = offset,
                    Payload = _serializer.SerializeToArray(
                        Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.ToWalRecord(
                            mutation,
                            LatticeMergeMode.LwwRegister,
                            string.Empty)),
                }));
        }
        await _table.SubmitTransactionAsync(actions, CancellationToken.None);
    }

    private Task UpsertManifestRowAsync(string rowKey, long offset) =>
        _table.UpsertEntityAsync(
            new AzureTableWalEntity
            {
                PartitionKey = ManifestPartitionKey,
                RowKey = rowKey,
                Offset = offset,
                Payload = null,
            },
            TableUpdateMode.Replace,
            CancellationToken.None);

    private async Task<bool> RowExistsAsync(string rowKey)
    {
        var response = await _table.GetEntityIfExistsAsync<AzureTableWalEntity>(ManifestPartitionKey, rowKey);
        return response.HasValue;
    }

    private static async Task<List<long>> ReadOffsetsAsync(AzureTableWalStorageProvider provider)
    {
        var read = new List<long>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        return read;
    }

    [Test]
    public async Task ReconcileAsync_treats_candidate_with_manifest_row_above_tail_as_committed()
    {
        // TAIL = 1 while [2,4] carries both its C-row and an M-row: the
        // shape a TAIL lowered under a landed phase 2 leaves in C-mode.
        // Re-adding the M-row would 409 on every pass.
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        await WriteEntryRowsAsync(0L, 2);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(0L), 1L);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.TailRowKey, 1L);
        await WriteEntryRowsAsync(2L, 3);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildCandidateRowKey(2L), 4L);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(2L), 4L);

        await provider.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(4L));
            Assert.That(await RowExistsAsync(AzureTableWalStorageProvider.BuildCandidateRowKey(2L)), Is.False);
            Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
        });
    }

    [Test]
    public async Task ReconcileAsync_rolls_tail_forward_over_manifest_row_with_no_candidate_row()
    {
        // Phase 2 deleted the C-row and added the M-row, then TAIL was
        // lowered beneath it. Nothing but the M-row marks the batch, and
        // left unseen its offsets would be reused by the next append.
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        await WriteEntryRowsAsync(0L, 2);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(0L), 1L);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.TailRowKey, 1L);
        await WriteEntryRowsAsync(2L, 3);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(2L), 4L);

        await provider.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(4L));
    }

    [Test]
    public async Task ReconcileAsync_d_mode_keeps_committed_batch_partition_above_tail()
    {
        // D-mode discovers [2,4] from its batch partition. It has an
        // M-row, so it must roll forward - never back, which would
        // delete entries its manifest row still references.
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: true);
        await WriteEntryRowsAsync(0L, 2);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(0L), 1L);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.TailRowKey, 1L);
        await WriteEntryRowsAsync(2L, 3);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(2L), 4L);

        await provider.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(4L));
            Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
        });
    }

    [Test]
    public async Task ReconcileAsync_committed_batch_above_gap_advances_tail_and_rolls_back_uncommitted_below()
    {
        // TAIL = 1; [2,4] is missing entirely, [5,7] is an uncommitted
        // C-row orphan above that gap, and [8,9] is committed. The
        // committed batch re-anchors TAIL; the gapped uncommitted
        // orphan still rolls back.
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        await WriteEntryRowsAsync(0L, 2);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(0L), 1L);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.TailRowKey, 1L);
        await WriteEntryRowsAsync(5L, 3);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildCandidateRowKey(5L), 7L);
        await WriteEntryRowsAsync(8L, 2);
        await UpsertManifestRowAsync(AzureTableWalStorageProvider.BuildManifestRowKey(8L), 9L);

        await provider.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(9L));
            Assert.That(await RowExistsAsync(AzureTableWalStorageProvider.BuildCandidateRowKey(5L)), Is.False);
            Assert.That(await RowExistsAsync(AzureTableWalStorageProvider.BuildManifestRowKey(5L)), Is.False);
        });
    }

    [Test]
    public async Task ReconcileAsync_waits_for_in_motion_writes_on_the_shard()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        var activity = provider.GetShardActivity(ManifestPartitionKey);
        activity.Writes.Enter();

        var reconcile = provider.ReconcileAsync(TreeId, 0, CancellationToken.None);
        await Task.Delay(200);
        Assert.That(reconcile.IsCompleted, Is.False, "reconcile must not scan while a write is in motion");

        activity.Writes.Exit();
        await reconcile.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task ReconcileAsync_wait_for_in_motion_writes_honours_cancellation()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        var activity = provider.GetShardActivity(ManifestPartitionKey);
        activity.Writes.Enter();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        Assert.That(
            async () => await provider.ReconcileAsync(TreeId, 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(activity.ReconcileGate.CurrentCount, Is.EqualTo(1), "a cancelled wait must release the reconcile gate");
        activity.Writes.Exit();
    }

    private static WalEntry[] Entries(long startOffset, int count)
    {
        var entries = new WalEntry[count];
        for (var i = 0; i < count; i++)
        {
            var offset = startOffset + i;
            entries[i] = new WalEntry
            {
                Offset = offset,
                Mutation = new LatticeMutation
                {
                    TreeId = TreeId,
                    Kind = MutationKind.Set,
                    Key = string.Create(CultureInfo.InvariantCulture, $"k{offset}"),
                    Value = new byte[] { (byte)i },
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    OriginClusterId = "site-a",
                },
            };
        }
        return entries;
    }

    /// <summary>
    /// Replaces the provider's phase-2 worker for the shard with one whose
    /// first transaction is abandoned on a short deadline and lands on the
    /// real table only when <paramref name="zombieGate"/> is released (or
    /// never lands, when the gate is faulted): a phase-2 transaction that
    /// was on the wire when the deadline fired (#3458). Cancelling the
    /// submit's token abandons only the local wait, exactly as the SDK
    /// does: the request the service already holds still lands.
    /// </summary>
    private void InjectZombieWorker(AzureTableWalStorageProvider provider, TaskCompletionSource zombieGate)
    {
        var calls = 0;
        var worker = new PhaseTwoWorker(
            (actions, ct) =>
            {
                if (Interlocked.Increment(ref calls) != 1)
                {
                    return _table.SubmitTransactionAsync(actions, ct);
                }
                var landing = LandAsync(actions);
                return landing.WaitAsync(ct);
            },
            ManifestPartitionKey,
            TimeSpan.Zero,
            commitTimeout: TimeSpan.FromMilliseconds(100));
        Assert.That(provider._phaseTwoWorkers.TryAdd(ManifestPartitionKey, worker), Is.True);

        async Task LandAsync(IReadOnlyList<TableTransactionAction> actions)
        {
            await zombieGate.Task;
            await _table.SubmitTransactionAsync(actions, CancellationToken.None);
        }
    }

    /// <summary>The grain's post-failure resync: reconcile, then read the tail.</summary>
    private static async Task<long> ResyncAsync(AzureTableWalStorageProvider provider)
    {
        await provider.ReconcileAsync(TreeId, 0, CancellationToken.None);
        return await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
    }

    [Test]
    public async Task Resync_after_abandoned_phase_two_waits_for_a_zombie_that_lands()
    {
        // [2,4] is phase-1 durable and its phase 2 was abandoned on the
        // deadline while still on the wire; [0,1] never reached phase 1.
        // Resyncing before the zombie lands would roll [2,4] back above
        // the gap and hand out offset 0, after which the zombie adds M2
        // and TAIL = 4 beneath the rewound producer - the unprovable 409
        // and overlap storm of #3458.
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: true);
        var zombieGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        InjectZombieWorker(provider, zombieGate);

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, 0, Entries(2L, 3), CancellationToken.None),
            Throws.InstanceOf<TimeoutException>());

        var resync = ResyncAsync(provider);
        await Task.Delay(300);
        var resyncRanUnderTheZombie = resync.IsCompleted;

        zombieGate.SetResult();
        var highest = await resync.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(300);
        Assert.That(resyncRanUnderTheZombie, Is.False,
            "the resync must not reconcile or read the tail while the abandoned transaction is in flight");
        Assert.That(highest, Is.EqualTo(4L), "the resync must account for what the zombie wrote");

        await provider.AppendBatchAsync(TreeId, 0, Entries(highest + 1, 2), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(6L));
            Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 2L, 3L, 4L, 5L, 6L }));
        });
    }

    [Test]
    public async Task Resync_after_abandoned_phase_two_waits_for_a_zombie_that_fails()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: true);
        var zombieGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        InjectZombieWorker(provider, zombieGate);

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, 0, Entries(2L, 3), CancellationToken.None),
            Throws.InstanceOf<TimeoutException>());

        var resync = ResyncAsync(provider);
        await Task.Delay(300);
        var resyncRanUnderTheZombie = resync.IsCompleted;

        zombieGate.SetException(new RequestFailedException(500, "zombie failed"));
        var highest = await resync.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.That(resyncRanUnderTheZombie, Is.False,
            "the resync must not reconcile or read the tail while the abandoned transaction is in flight");
        Assert.That(highest, Is.EqualTo(-1L), "a failed zombie wrote nothing, so the gapped orphan rolls back");

        await provider.AppendBatchAsync(TreeId, 0, Entries(highest + 1, 3), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
            Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L }));
        });
    }

    [Test]
    public async Task ReconcileAsync_waits_for_an_abandoned_phase_two_submit()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        var worker = provider.GetOrCreatePhaseTwoWorker(TreeId, 0);
        worker.AbandonedSubmits.Enter();

        var reconcile = provider.ReconcileAsync(TreeId, 0, CancellationToken.None);
        await Task.Delay(200);
        Assert.That(reconcile.IsCompleted, Is.False, "reconcile must not scan under a live abandoned submit");

        worker.AbandonedSubmits.Exit();
        await reconcile.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task ReconcileAsync_wait_for_an_abandoned_phase_two_submit_honours_cancellation()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        var worker = provider.GetOrCreatePhaseTwoWorker(TreeId, 0);
        var activity = provider.GetShardActivity(ManifestPartitionKey);
        worker.AbandonedSubmits.Enter();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        Assert.That(
            async () => await provider.ReconcileAsync(TreeId, 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(activity.ReconcileGate.CurrentCount, Is.EqualTo(1), "a cancelled wait must release the reconcile gate");
        worker.AbandonedSubmits.Exit();
    }

    [Test]
    public async Task GetHighestOffsetAsync_waits_for_an_abandoned_phase_two_submit()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);
        var worker = provider.GetOrCreatePhaseTwoWorker(TreeId, 0);
        worker.AbandonedSubmits.Enter();

        var read = provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        await Task.Delay(200);
        Assert.That(read.IsCompleted, Is.False, "the tail read must not run under a live abandoned submit");

        worker.AbandonedSubmits.Exit();
        Assert.That(await read.WaitAsync(TimeSpan.FromSeconds(30)), Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendBatchAsync_leaves_shard_write_tracker_idle_on_return()
    {
        await using var provider = await CreateProviderAsync(eliminateCandidateRow: false);

        await provider.AppendBatchAsync(
            TreeId,
            0,
            new[]
            {
                new WalEntry
                {
                    Offset = 0L,
                    Mutation = new LatticeMutation
                    {
                        TreeId = TreeId,
                        Kind = MutationKind.Set,
                        Key = "k0",
                        Value = new byte[] { 1 },
                        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                        OriginClusterId = "site-a",
                    },
                },
            },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetShardActivity(ManifestPartitionKey).Writes.Active, Is.Zero);
            Assert.That(provider._phaseTwoWorkers[ManifestPartitionKey].OutstandingCommits.Active, Is.Zero);
        });
    }
}