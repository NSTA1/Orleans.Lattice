using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Behavioural coverage for the Azure Table WAL provider's retention and
/// crash-recovery paths -
/// <see cref="AzureTableWalStorageProvider.TrimAsync"/> and
/// <see cref="AzureTableWalStorageProvider.ReconcileAsync"/>, together with
/// the chunked partition delete and the roll-forward / roll-back planner
/// behind them.
/// <para>
/// These are the paths a silo exercises after an unclean restart, and every
/// pre-existing fixture covering them carried
/// <c>[Category("AzureStorageEmulator")]</c>, so they measured at zero line
/// coverage under the repository's default filter. Driving them against an
/// in-memory table makes the orphan states deterministic - a crash between
/// phase 1 and phase 2 is simply a table whose candidate row is present and
/// whose manifest row is not - where an emulator-backed test has to
/// manufacture a real crash.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderTrimReconcileTests
{
    private const string TreeId = "tree-recover";
    private const int ShardIndex = 0;

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

    private AzureTableWalStorageProvider CreateProvider(
        InMemoryWalTable table,
        Action<AzureTableWalStorageOptions>? configure = null)
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Trecover",
            Compression = LatticeCompression.None,
        };
        configure?.Invoke(options);
        return new AzureTableWalStorageProvider(
            Options.Create(options),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)]);
    }

    private static List<WalEntry> Entries(long firstOffset, int count) =>
        Enumerable.Range(0, count)
            .Select(i => new WalEntry
            {
                Offset = firstOffset + i,
                Mutation = new LatticeMutation
                {
                    TreeId = TreeId,
                    Kind = MutationKind.Set,
                    Key = $"k{firstOffset + i}",
                    Value = [(byte)(firstOffset + i)],
                },
            })
            .ToList();

    private static async Task<List<WalEntry>> DrainAsync(IAsyncEnumerable<WalEntry> source)
    {
        var drained = new List<WalEntry>();
        await foreach (var entry in source)
        {
            drained.Add(entry);
        }

        return drained;
    }

    private static IReadOnlyList<AzureTableWalEntity> RowsWithPrefix(InMemoryWalTable table, string prefix) =>
        table.Snapshot().Where(r => r.RowKey.StartsWith(prefix, StringComparison.Ordinal)).ToList();

    /// <summary>
    /// Produces the exact table state a crash between phase 1 and phase 2
    /// leaves behind: the batch's entry rows are durable and its candidate
    /// row is present, but no manifest row exists and TAIL has not advanced
    /// over it.
    /// <para>
    /// The batches are appended and committed normally - so the entry rows
    /// carry the provider's real encoding - and the phase-2 artefacts are
    /// then rolled back by hand. That is deterministic and instant, where
    /// suppressing the real phase-2 commit would mean stalling the coalescing
    /// window and waiting on wall-clock, because an append awaits its own
    /// phase-2 task unless commit pipelining is enabled.
    /// </para>
    /// </summary>
    private async Task<InMemoryWalTable> CrashAfterPhaseOneAsync(
        params (long Start, int Count)[] batches)
    {
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using (writer)
        {
            foreach (var (start, count) in batches)
            {
                await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(start, count), CancellationToken.None);
            }

            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();

        // Undo phase 2 for each batch: drop its manifest row and restore the
        // candidate row phase 2 would have deleted.
        foreach (var (start, count) in batches)
        {
            await client.DeleteEntityAsync(
                manifestPartitionKey,
                AzureTableWalStorageProvider.BuildManifestRowKey(start),
                ETag.All,
                CancellationToken.None);

            table.Seed(new AzureTableWalEntity
            {
                PartitionKey = manifestPartitionKey,
                RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(start),
                Offset = start + count - 1,
                Payload = null,
            });
        }

        // TAIL never advanced past the crash point either.
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        return table;
    }

    [Test]
    public async Task TrimAsync_removes_every_entry_of_a_fully_covered_batch()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);

        var remaining = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(remaining.Select(e => e.Offset), Is.EqualTo(new[] { 3L, 4L, 5L }));
    }

    [Test]
    public async Task TrimAsync_deletes_the_manifest_row_of_a_fully_covered_batch()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(RowsWithPrefix(table, "M"), Has.Count.EqualTo(2));

        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);

        Assert.That(RowsWithPrefix(table, "M"), Has.Count.EqualTo(1));
        Assert.That(RowsWithPrefix(table, "M").Single().Offset, Is.EqualTo(5L));
    }

    [Test]
    public async Task TrimAsync_partially_trims_the_boundary_batch_and_keeps_its_manifest_row()
    {
        // The boundary batch straddles the trim point: entries at or below it
        // go, entries above it stay, and the manifest row must survive so the
        // batch remains discoverable. This is the arm that distinguishes a
        // correct trim from one that drops a live batch.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 6), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);

        var remaining = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(remaining.Select(e => e.Offset), Is.EqualTo(new[] { 3L, 4L, 5L }));
        Assert.That(RowsWithPrefix(table, "M"), Has.Count.EqualTo(1), "the boundary batch keeps its manifest row");
    }

    [Test]
    public async Task TrimAsync_never_moves_TAIL_backwards()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        var tailBefore = table.Snapshot().Single(r => r.RowKey == "TAIL").Offset;

        await provider.TrimAsync(TreeId, ShardIndex, 3L, CancellationToken.None);

        Assert.That(table.Snapshot().Single(r => r.RowKey == "TAIL").Offset, Is.EqualTo(tailBefore));
        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(3L),
            "trimming every entry must not rewind the append cursor");
    }

    [Test]
    public async Task TrimAsync_is_a_no_op_for_a_negative_offset()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        var before = table.Count;

        await provider.TrimAsync(TreeId, ShardIndex, -1L, CancellationToken.None);

        Assert.That(table.Count, Is.EqualTo(before));
    }

    [Test]
    public async Task TrimAsync_is_a_no_op_for_an_absent_shard()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.TrimAsync("absent", 4, 100L, CancellationToken.None);

        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task TrimAsync_is_idempotent()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 6), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);
        var afterFirst = table.Snapshot().Select(r => (r.PartitionKey, r.RowKey)).ToList();
        await provider.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);

        Assert.That(
            table.Snapshot().Select(r => (r.PartitionKey, r.RowKey)).ToList(),
            Is.EqualTo(afterFirst));
    }

    [Test]
    public async Task TrimAsync_deletes_a_large_batch_in_transactional_chunks()
    {
        // The chunked delete only loops when the match count exceeds the
        // 100-action transaction cap, so a batch of 100 plus a second batch
        // is what reaches the flush-the-remainder arm.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 100), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(100, 20), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.TrimAsync(TreeId, ShardIndex, 119L, CancellationToken.None);

        Assert.That(RowsWithPrefix(table, "E"), Is.Empty, "every entry row at or below the trim point must go");
    }

    [Test]
    public async Task TrimAsync_rejects_a_null_treeId()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        Assert.That(
            async () => await provider.TrimAsync(null!, 0, 1L, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ReconcileAsync_is_a_no_op_on_a_clean_shard()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        var before = table.Snapshot().Select(r => (r.PartitionKey, r.RowKey, r.Offset)).ToList();

        await provider.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            table.Snapshot().Select(r => (r.PartitionKey, r.RowKey, r.Offset)).ToList(),
            Is.EqualTo(before));
        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L));
    }

    [Test]
    public async Task ReconcileAsync_on_an_empty_shard_leaves_the_tail_unset()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(-1L));
    }

    [Test]
    public async Task ReconcileAsync_rolls_forward_an_orphan_contiguous_with_the_tail()
    {
        // The batch's entries are durable and only the phase-2 commit was
        // lost, so recovery must keep the data and advance TAIL over it.
        var table = await CrashAfterPhaseOneAsync((0, 3));
        Assert.That(RowsWithPrefix(table, "C"), Is.Not.Empty, "the crash state must carry a candidate row");
        Assert.That(RowsWithPrefix(table, "M"), Is.Empty, "the crash state must have no manifest row");

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L),
            "a contiguous orphan must be rolled forward, not discarded");
        Assert.That(RowsWithPrefix(table, "C"), Is.Empty, "roll-forward deletes the candidate row");
        Assert.That(RowsWithPrefix(table, "M"), Has.Count.EqualTo(1));

        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReconcileAsync_rolls_forward_several_contiguous_orphans_in_offset_order()
    {
        var table = await CrashAfterPhaseOneAsync((0, 2), (2, 2), (4, 2));

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(5L));
        Assert.That(
            RowsWithPrefix(table, "M").Select(r => r.Offset),
            Is.EqualTo(new[] { 1L, 3L, 5L }));

        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L }));
    }

    [Test]
    public async Task ReconcileAsync_rolls_back_an_orphan_above_a_gap()
    {
        // Batches [0,1] and [4,5] are durable but [2,3] never landed, so the
        // second batch sits above a gap. Rolling it forward would leave the
        // producer's cursor above offsets that were never written, so it must
        // be discarded. Paired with the roll-forward case above, this proves
        // the planner discriminates on contiguity rather than salvaging or
        // discarding everything.
        var table = await CrashAfterPhaseOneAsync((0, 2), (4, 2));

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(1L),
            "the tail may only advance across the contiguous prefix");

        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(
            readable.Select(e => e.Offset),
            Is.EqualTo(new[] { 0L, 1L }),
            "the batch above the gap must be rolled back, not readable");
        Assert.That(RowsWithPrefix(table, "C"), Is.Empty, "roll-back deletes the candidate row too");
    }

    [Test]
    public async Task ReconcileAsync_is_idempotent_across_repeated_passes()
    {
        var table = await CrashAfterPhaseOneAsync((0, 3));

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);
        var afterFirst = table.Snapshot().Select(r => (r.PartitionKey, r.RowKey, r.Offset)).ToList();

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            table.Snapshot().Select(r => (r.PartitionKey, r.RowKey, r.Offset)).ToList(),
            Is.EqualTo(afterFirst),
            "a second pass with no intervening writes must change nothing");
    }

    [Test]
    public async Task ReconcileAsync_allows_appends_to_resume_from_the_recovered_tail()
    {
        var table = await CrashAfterPhaseOneAsync((0, 3));

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);
        await recovered.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await recovered.FlushPhaseTwoAsync(CancellationToken.None);

        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
    }

    [Test]
    public async Task ReconcileAsync_discovers_an_orphan_by_partition_scan_when_candidate_rows_are_elided()
    {
        // With the C-row elided on the hot path there is no candidate row to
        // find, so recovery must fall back to enumerating batch partitions
        // above TAIL. Asserting the same roll-forward outcome as the C-row
        // case proves the fallback is equivalent rather than merely silent.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        // Roll phase 2 back without restoring a candidate row, which is the
        // crash state this mode leaves: entry rows only.
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        Assert.That(RowsWithPrefix(table, "C"), Is.Empty, "the hot path elided the candidate row");
        Assert.That(RowsWithPrefix(table, "M"), Is.Empty);

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L));
        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReconcileAsync_treats_a_candidate_whose_manifest_row_exists_as_committed()
    {
        // A batch whose M-row landed but whose C-row survived (phase 2 partly
        // applied, or TAIL later lowered by a racing writer) must not have its
        // manifest row re-added - that would fail every pass with 409 and wedge
        // activation. It is folded in as already committed and TAIL rolls over
        // it.
        var table = await CrashAfterPhaseOneAsync((0, 3));

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            Offset = 2L,
            PayloadBytes = 0L,
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L));
        Assert.That(
            RowsWithPrefix(table, "M"),
            Has.Count.EqualTo(1),
            "the committed manifest row must be kept, not duplicated");
    }

    [Test]
    public async Task ReconcileAsync_leaves_an_already_committed_batch_readable()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        var readable = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReconcileAsync_folds_in_a_committed_manifest_row_that_has_no_orphan()
    {
        // A manifest row can sit above TAIL with no candidate row and no batch
        // partition at all - the state left when phase 2 committed but a
        // racing writer later lowered TAIL. It is authoritative: TAIL must roll
        // forward over it rather than the row being ignored or re-added.
        var table = await CrashAfterPhaseOneAsync((0, 3));

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildManifestRowKey(3L),
            Offset = 5L,
            PayloadBytes = 0L,
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(5L),
            "the tail must roll forward over the unmatched committed row");
        Assert.That(
            RowsWithPrefix(table, "M").Select(r => r.Offset),
            Is.EqualTo(new[] { 2L, 5L }),
            "both manifest rows survive, with neither duplicated");
    }

    [Test]
    public async Task ReconcileAsync_anchors_its_manifest_scan_below_an_orphan_beneath_the_tail()
    {
        // An orphan can start below the persisted TAIL when an earlier batch's
        // phase 2 was undone after a later one committed. The manifest scan
        // must anchor at the orphan's start rather than at TAIL + 1, or the
        // committed row covering the orphan is never seen and the orphan is
        // wrongly rolled back.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        // Undo phase 2 for the FIRST batch only, leaving TAIL at 5. The orphan
        // at offset 0 now sits below TAIL + 1.
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(0L),
            Offset = 2L,
            Payload = null,
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(5L),
            "the committed tail must not be lowered by a sub-tail orphan");
        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(
            readable.Select(e => e.Offset),
            Is.EqualTo(new[] { 3L, 4L, 5L }),
            "the batch whose manifest row is genuinely absent is rolled back, and the committed one survives");
    }

    [Test]
    public async Task ReconcileAsync_rejects_a_null_treeId()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        Assert.That(
            async () => await provider.ReconcileAsync(null!, 0, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ReconcileAsync_throws_after_the_provider_is_disposed()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await provider.DisposeAsync();

        Assert.That(
            async () => await provider.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public async Task ReconcileAsync_retries_when_the_commit_loses_a_race_then_succeeds()
    {
        // A concurrent manifest writer makes the first commit fail with 409.
        // The pass must re-plan from fresh reads rather than propagating the
        // conflict, so the shard still activates.
        var table = await CrashAfterPhaseOneAsync((0, 3));

        var failuresInjected = 0;
        table.BeforeTransaction = actions =>
        {
            if (failuresInjected == 0
                && actions.Any(a => a.ActionType == TableTransactionActionType.Add
                    && ((AzureTableWalEntity)a.Entity).RowKey.StartsWith("M", StringComparison.Ordinal)))
            {
                failuresInjected++;
                throw new RequestFailedException(409, "EntityAlreadyExists", "EntityAlreadyExists", innerException: null);
            }
        };

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(failuresInjected, Is.EqualTo(1), "the conflict must actually have been injected");
        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L),
            "the retry must complete the roll-forward");
    }

    [Test]
    public async Task ReconcileAsync_surfaces_a_non_conflict_failure()
    {
        // A 500 is not a lost race, so it must propagate rather than be
        // retried into a wedge. Paired with the 409 case above to prove the
        // classifier discriminates.
        var table = await CrashAfterPhaseOneAsync((0, 3));

        table.BeforeTransaction = actions =>
        {
            if (actions.Any(a => a.ActionType == TableTransactionActionType.Add
                && ((AzureTableWalEntity)a.Entity).RowKey.StartsWith("M", StringComparison.Ordinal)))
            {
                throw new RequestFailedException(500, "InternalError", "InternalError", innerException: null);
            }
        };

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        Assert.That(
            async () => await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None),
            Throws.InstanceOf<RequestFailedException>().With.Property("Status").EqualTo(500));
    }

    [Test]
    public void IsConcurrentManifestConflict_classifies_only_409_and_412()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                AzureTableWalStorageProvider.IsConcurrentManifestConflict(
                    new RequestFailedException(409, "x", "x", innerException: null)),
                Is.True);
            Assert.That(
                AzureTableWalStorageProvider.IsConcurrentManifestConflict(
                    new RequestFailedException(412, "x", "x", innerException: null)),
                Is.True);
            Assert.That(
                AzureTableWalStorageProvider.IsConcurrentManifestConflict(
                    new RequestFailedException(500, "x", "x", innerException: null)),
                Is.False);
            Assert.That(
                AzureTableWalStorageProvider.IsConcurrentManifestConflict(
                    new RequestFailedException(404, "x", "x", innerException: null)),
                Is.False);
        });
    }

    [Test]
    public async Task Reconcile_then_trim_leaves_a_consistent_log()
    {
        // The two recovery paths compose: reconcile salvages the orphan, trim
        // then retires its entries, and the surviving log is still readable
        // and still appends from the right offset.
        var table = await CrashAfterPhaseOneAsync((0, 3), (3, 3));

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);
        await recovered.TrimAsync(TreeId, ShardIndex, 2L, CancellationToken.None);

        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 3L, 4L, 5L }));

        await recovered.AppendBatchAsync(TreeId, ShardIndex, Entries(6, 2), CancellationToken.None);
        await recovered.FlushPhaseTwoAsync(CancellationToken.None);

        var after = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(after.Select(e => e.Offset), Is.EqualTo(new[] { 3L, 4L, 5L, 6L, 7L }));
    }
}
