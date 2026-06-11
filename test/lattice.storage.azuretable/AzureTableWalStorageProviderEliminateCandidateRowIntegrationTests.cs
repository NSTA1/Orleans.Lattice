using System.Globalization;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Azurite-backed integration tests for the
/// <see cref="AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath"/>
/// optimisation (Variant D). The flag elides the phase-0 candidate
/// row (C-row) on the hot append path; orphans are then discovered
/// at activation time by scanning batch partitions above TAIL,
/// instead of by enumerating C-rows in the manifest partition.
/// <para>
/// These tests pin the end-to-end behaviour against Azurite:
/// happy-path appends in D-mode produce no C-rows; crash-style
/// orphans without a C-row are still discovered and recovered
/// correctly; mixed legacy + D-mode orphans on a single shard are
/// both recovered (forward-compat with a silo that ran without the
/// flag and was restarted with it on); and recovery is idempotent.
/// </para>
/// <para>
/// Gated under the <c>AzureTableEmulator</c> category exactly like
/// the rest of the integration suite; the dev loop skips them.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureTableEmulator")]
public class AzureTableWalStorageProviderEliminateCandidateRowIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-ecr";

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

    private AzureTableWalStorageProvider CreateProvider(string tableName, bool eliminateCandidateRow) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = tableName,
                EliminateCandidateRowOnHotPath = eliminateCandidateRow,
                Compression = LatticeCompression.None,
                // Pin synchronous phase-2 commits so TAIL and the
                // manifest are durable the instant AppendBatchAsync
                // returns; these tests assert read-your-writes
                // semantics, which the throughput default
                // (PipelinePhaseTwoCommits = true) defers to the next
                // append on the shard.
                PipelinePhaseTwoCommits = false,
            }),
            _serializer);

    private static WalEntry Entry(long offset, string key = "k", byte tag = 1) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = new byte[] { tag },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private async Task<int> CountCandidateRowsAsync(string treeId, int shardIndex)
    {
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(treeId, shardIndex);
        var count = 0;
        await foreach (var _ in _table.QueryAsync<AzureTableWalEntity>(
            $"PartitionKey eq '{manifestPartitionKey}' and RowKey ge 'C' and RowKey lt 'D'",
            cancellationToken: CancellationToken.None))
        {
            count++;
        }
        return count;
    }

    private async Task<int> CountManifestRowsAsync(string treeId, int shardIndex)
    {
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(treeId, shardIndex);
        var count = 0;
        await foreach (var _ in _table.QueryAsync<AzureTableWalEntity>(
            $"PartitionKey eq '{manifestPartitionKey}' and RowKey ge 'M' and RowKey lt 'N'",
            cancellationToken: CancellationToken.None))
        {
            count++;
        }
        return count;
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

    /// <summary>
    /// Synthesises a D-mode "orphan" batch: entry rows are written
    /// directly under the per-batch partition, but no candidate row
    /// is written and no manifest row / TAIL upsert is performed.
    /// Models a silo crash between phase 1 and phase 2 in a process
    /// that ran with <see cref="AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath"/>
    /// enabled.
    /// </summary>
    private async Task SynthesiseDModeOrphanBatchAsync(string treeId, int shardIndex, long startOffset, int entryCount)
    {
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(treeId, shardIndex, startOffset);
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
        // Deliberately no candidate-row, no manifest row, no TAIL.
    }

    /// <summary>
    /// Synthesises a legacy-mode "orphan" batch: entry rows under
    /// the batch partition plus a phase-0 candidate row in the
    /// manifest partition, but no manifest M-row / TAIL. Models a
    /// silo that crashed before the flag was enabled, then was
    /// restarted with the flag on - the union recovery path must
    /// still find this orphan via the legacy C-row scan.
    /// </summary>
    private async Task SynthesiseLegacyOrphanBatchAsync(string treeId, int shardIndex, long startOffset, int entryCount)
    {
        await SynthesiseDModeOrphanBatchAsync(treeId, shardIndex, startOffset, entryCount);
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(treeId, shardIndex);
        var endOffsetInclusive = startOffset + entryCount - 1;
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

    // ---------------------------------------------------------------------
    // Happy path
    // ---------------------------------------------------------------------

    [Test]
    public async Task AppendBatchAsync_in_D_mode_writes_no_candidate_row_and_still_advances_TAIL()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var head = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(2L));

        // The defining wire-shape change: no C-rows are ever
        // written, even transiently. A C-row count of zero after
        // the append has settled proves both that none were
        // written and that phase 2 produced no orphan C-rows of
        // its own.
        Assert.That(await CountCandidateRowsAsync(TreeId, 0), Is.Zero,
            "D-mode must not write phase-0 candidate rows on the hot path");
        Assert.That(await CountManifestRowsAsync(TreeId, 0), Is.EqualTo(1),
            "D-mode must still commit exactly one manifest row per batch");
    }

    [Test]
    public async Task AppendBatchAsync_with_flag_off_continues_to_write_candidate_rows_then_clean_them_up()
    {
        // Sanity check the legacy path is unchanged when the flag
        // is off: the C-row is written in phase 0 and then deleted
        // by phase 2, so the steady-state count is zero - but the
        // manifest row still lands. This guards against an
        // accidental regression where the flag's default value
        // leaks into the legacy path.
        var sut = CreateProvider(_tableName, eliminateCandidateRow: false);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        Assert.That(await CountCandidateRowsAsync(TreeId, 0), Is.Zero,
            "Legacy mode writes the C-row in phase 0 and deletes it in phase 2");
        Assert.That(await CountManifestRowsAsync(TreeId, 0), Is.EqualTo(1));
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(1L));
    }

    [Test]
    public async Task AppendBatchAsync_in_D_mode_round_trips_entries_through_ReadAsync()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0, "a"), Entry(1, "b"), Entry(2, "c") }, CancellationToken.None);

        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            collected.Add(entry);
        }

        Assert.That(collected.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
        Assert.That(collected.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task AppendBatchAsync_in_D_mode_supports_repeated_appends_into_the_same_shard()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(3L));
        Assert.That(await CountCandidateRowsAsync(TreeId, 0), Is.Zero,
            "D-mode must not leak C-rows across multiple appends");
        Assert.That(await CountManifestRowsAsync(TreeId, 0), Is.EqualTo(2),
            "Each D-mode batch must still produce exactly one manifest row");
    }

    [Test]
    public async Task AppendBatchAsync_in_D_mode_persists_across_provider_restart()
    {
        var hot = CreateProvider(_tableName, eliminateCandidateRow: true);
        await hot.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);
        await hot.DisposeAsync();

        var recovered = CreateProvider(_tableName, eliminateCandidateRow: true);
        Assert.That(await recovered.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));

        var read = new List<long>();
        await foreach (var entry in recovered.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        Assert.That(read, Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    // ---------------------------------------------------------------------
    // Failure / recovery
    // ---------------------------------------------------------------------

    [Test]
    public async Task ReconcileAsync_in_D_mode_rolls_forward_a_contiguous_orphan_with_no_candidate_row()
    {
        // Simulates a silo that ran in D-mode and crashed after
        // phase 1 (entry rows committed) but before phase 2 (no
        // manifest row, no TAIL). The reconciler must discover the
        // orphan by scanning batch partitions above TAIL.
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);
        await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None); // force table create

        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 5);
        Assert.That(await CountCandidateRowsAsync(TreeId, 0), Is.Zero,
            "precondition: synthesised D-mode orphan has no candidate row");

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(4L));

        var read = new List<long>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        Assert.That(read, Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_rolls_forward_an_orphan_contiguous_with_an_existing_TAIL()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 2L, entryCount: 3);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(4L));
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_rolls_back_an_orphan_with_a_gap_below_it()
    {
        // Failure path: a D-mode orphan that is not contiguous with
        // TAIL must be rolled back - entry rows deleted, TAIL
        // unchanged. The recovery decision must not depend on
        // whether a C-row was present.
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 10L, entryCount: 5);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(1L),
            "TAIL must not advance over a gap, irrespective of D-mode discovery");
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 10L), Is.Zero,
            "Rolled-back D-mode orphan entry rows must be deleted even when no C-row existed");
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_rolls_back_orphan_after_first_contiguity_break()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);

        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 2L, entryCount: 3);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 20L, entryCount: 3);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(4L));
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 2L), Is.EqualTo(3));
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 20L), Is.Zero);
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_recovers_a_mix_of_D_mode_and_legacy_orphans()
    {
        // Forward-compatibility scenario: a silo crashed before the
        // flag was enabled (leaving a legacy orphan with a C-row),
        // and a second crash happened after the flag was enabled
        // (leaving a D-mode orphan without one). The union recovery
        // path must find both.
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);
        await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None); // force table create

        await SynthesiseLegacyOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 3L, entryCount: 4);

        Assert.That(await CountCandidateRowsAsync(TreeId, 0), Is.EqualTo(1));

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(6L));
        Assert.That(await CountCandidateRowsAsync(TreeId, 0), Is.Zero,
            "Legacy C-row must be deleted as part of rollforward");

        var read = new List<long>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        Assert.That(read, Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L, 6L }));
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_is_idempotent()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);
        await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var headAfterFirst = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(headAfterFirst, Is.EqualTo(2L));

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var headAfterSecond = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(headAfterSecond, Is.EqualTo(2L),
            "Second reconcile must observe the now-committed manifest row and be a no-op");
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 0L), Is.EqualTo(3),
            "Reconciled entry rows must survive a repeat reconcile pass");
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_chains_multiple_contiguous_orphans_into_one_TAIL_advance()
    {
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);
        await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 3L, entryCount: 3);
        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 6L, entryCount: 3);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(8L));

        var read = new List<long>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            read.Add(entry.Offset);
        }
        Assert.That(read, Is.EqualTo(new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }));
    }

    [Test]
    public async Task ReconcileAsync_in_legacy_mode_does_not_see_a_D_mode_orphan_pins_downgrade_footgun()
    {
        // Pins the downgrade-is-not-safe property: the
        // batch-partition discovery scan is gated on
        // EliminateCandidateRowOnHotPath in ReconcileAsync, so a
        // silo running with the flag *off* recovers only legacy
        // (C-row-bearing) orphans. A D-mode orphan written by a
        // previous flag-on activation is invisible to it.
        //
        // This is the load-bearing rationale for keeping the flag
        // opt-in and for not removing the legacy code path: a
        // downgrade (flag-on -> flag-off) must be performed only
        // after a clean drain. If the gating is ever made
        // unconditional (so the partition scan always runs), flip
        // this assertion to EqualTo(2L).
        var sut = CreateProvider(_tableName, eliminateCandidateRow: false);
        await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(-1L),
            "Legacy-mode reconciler does not perform the batch-partition scan and therefore cannot see a D-mode orphan");

        // The orphan entry rows are still on disk - they have not
        // been rolled back. A future activation with the flag on
        // would still recover them.
        Assert.That(await CountEntryRowsAsync(TreeId, 0, startOffset: 0L), Is.EqualTo(3));
    }

    [Test]
    public async Task ReconcileAsync_in_D_mode_isolates_shards_and_trees()
    {
        const string OtherTree = "tree-ecr-other";
        var sut = CreateProvider(_tableName, eliminateCandidateRow: true);
        await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        await SynthesiseDModeOrphanBatchAsync(TreeId, 0, startOffset: 0L, entryCount: 3);
        await sut.AppendBatchAsync(TreeId, 1, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await SynthesiseDModeOrphanBatchAsync(OtherTree, 0, startOffset: 0L, entryCount: 4);

        await sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
        Assert.That(await sut.GetHighestOffsetAsync(TreeId, 1, CancellationToken.None), Is.EqualTo(1L),
            "Sibling shard must remain unaffected");
        Assert.That(await sut.GetHighestOffsetAsync(OtherTree, 0, CancellationToken.None), Is.EqualTo(-1L),
            "Sibling tree must remain unreconciled");
    }
}
