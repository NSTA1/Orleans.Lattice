using Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// The narrow arms of the Azure Table WAL provider that the read, append,
/// trim, reconcile, and filtered-read fixtures do not reach on their own:
/// the non-pipelined commit mode, the defensive empty-payload decode, the
/// malformed-key skips in the recovery discovery scan, and the cold-start
/// bound read that has to account for batches written but never committed.
/// <para>
/// These are grouped together because each is a single branch reached by one
/// specific precondition, rather than a behaviour with a suite of its own.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderEdgeArmTests
{
    private const string TreeId = "tree-edge";
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
            TableName = "Tedge",
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

    [Test]
    public async Task Append_commits_and_reads_back_with_phase_two_pipelining_disabled()
    {
        // With pipelining off, an append awaits its own phase-2 commit instead
        // of chaining onto the previous one. The observable outcome must be
        // identical to the pipelined default, which is what makes the knob an
        // operating-point choice rather than a behaviour change.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table, o => o.PipelinePhaseTwoCommits = false);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L }));
        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(5L));
    }

    [Test]
    public async Task Append_honours_a_cancellation_token_with_pipelining_disabled()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table, o => o.PipelinePhaseTwoCommits = false);
        await using var _ = provider;

        using var cts = new CancellationTokenSource();
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), cts.Token);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task ReadAsync_decodes_an_empty_payload_as_a_default_mutation()
    {
        // Defensive arm: a row with no payload must decode to a default
        // mutation rather than throwing, so one malformed row cannot make a
        // whole shard unreadable.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 0L);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = batchPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(1L),
            Offset = 1L,
            Payload = [],
        });

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
        Assert.That(read[1].Mutation.Key, Is.Null, "an empty payload decodes to a default mutation");
        Assert.That(read[0].Mutation.Key, Is.EqualTo("k0"), "the well-formed row beside it still decodes");
    }

    [Test]
    public async Task ReadFilteredAsync_never_excludes_an_empty_payload_row()
    {
        // An empty payload has no key, so no filter can prove it excluded; it
        // must be yielded rather than silently dropped.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 0L);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = batchPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(1L),
            Offset = 1L,
            Payload = [],
        });

        var collected = new List<WalEntry>();
        await foreach (var entry in provider.ReadFilteredAsync(
            TreeId, ShardIndex, -1L, 1L, 10, new WalKeyFilter("zzz", null), CancellationToken.None))
        {
            collected.Add(entry);
        }

        Assert.That(collected.Select(e => e.Offset), Contains.Item(1L));
    }

    [Test]
    public async Task ReadFilteredAsync_stops_at_maxEntries_on_a_batch_boundary()
    {
        // The cap is re-checked at the top of the manifest loop as well as in
        // the entry loop; a batch-aligned cap is what reaches the outer arm.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(4, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var collected = new List<WalEntry>();
        await foreach (var entry in provider.ReadFilteredAsync(
            TreeId, ShardIndex, -1L, 5L, 4, default, CancellationToken.None))
        {
            collected.Add(entry);
        }

        Assert.That(collected.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task ReconcileAsync_skips_rows_whose_keys_do_not_match_the_batch_layout()
    {
        // The partition-scan discovery path parses offsets out of fixed-width
        // key suffixes. Rows that do not match the layout - written by an
        // older schema, or by something else sharing the table - must be
        // skipped rather than throwing and wedging activation.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        var wellFormedPartition = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 0L);

        // A partition key with too few characters after its offset marker.
        // It sorts inside the discovery scan's range but carries no parseable
        // start offset.
        var shardPrefix = wellFormedPartition[..(wellFormedPartition.LastIndexOf('S') + 1)];
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = shardPrefix + "x",
            RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(0L),
            Offset = 0L,
            Payload = [1],
        });

        // A well-formed partition whose highest row key is too short to carry
        // an end offset. 'Z' sorts above the digits, so this row is the one the
        // scan picks as the partition's maximum.
        var truncatedRowPartition = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 50L);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = truncatedRowPartition,
            RowKey = "EZ",
            Offset = 50L,
            Payload = [1],
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L),
            "the well-formed batch must still be recovered alongside the skipped rows");
    }

    [Test]
    public async Task Cold_start_bound_accounts_for_a_batch_written_but_never_committed()
    {
        // After a crash the batch partition holds entry rows that TAIL does not
        // cover. A restarted provider's first append must read a written upper
        // bound that includes them, or it would happily re-issue those offsets
        // and write an offset twice.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        // Undo phase 2: the entry rows survive with no manifest row and no TAIL.
        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        var restarted = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = restarted;

        Assert.That(
            async () => await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(1, 2), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("overlaps"),
            "the uncommitted batch must still raise the written bound");
    }

    [Test]
    public async Task Cold_start_bound_still_admits_a_range_above_an_uncommitted_batch()
    {
        // The accepting counterpart to the refusal above: the raised bound must
        // not refuse offsets genuinely beyond the uncommitted batch.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        var restarted = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = restarted;

        await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await restarted.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(
            await restarted.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(4L));
    }

    [Test]
    public async Task Append_releases_its_claim_when_the_overlap_probe_query_fails()
    {
        // A transport fault during the overlap probe must propagate AND release
        // the claim. If the claim leaked, every later append at those offsets
        // would be refused for the life of the process, so the recovery append
        // below is the assertion that matters.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var restarted = CreateProvider(table);
        await using var _ = restarted;

        var faults = 0;
        table.BeforeQuery = filter =>
        {
            // `PartitionKey ne` appears only in the cross-partition overlap
            // probe. Anchoring on it is what makes this fault land inside
            // FindOverlappingWrittenBatchAsync rather than on the cold-start
            // bound read, which also ranges over partition keys.
            if (filter.Contains("PartitionKey ne", StringComparison.Ordinal) && faults == 0)
            {
                faults++;
                throw new RequestFailedException(503, "ServerBusy", "ServerBusy", innerException: null);
            }
        };

        Assert.That(
            async () => await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(1, 3), CancellationToken.None),
            Throws.InstanceOf<RequestFailedException>().With.Property("Status").EqualTo(503));
        Assert.That(faults, Is.EqualTo(1), "the probe fault must actually have been injected");

        table.BeforeQuery = null;
        await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await restarted.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(restarted.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(
            read.Select(e => e.Offset),
            Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }),
            "the claim must have been released so the shard still accepts appends");
    }

    [Test]
    public async Task EnsureTableAsync_creates_the_table_once_across_many_operations()
    {
        // The initialisation gate is double-checked, so the table must be
        // created exactly once no matter how many operations run through it.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        await provider.GetLowestOffsetAsync(TreeId, ShardIndex, CancellationToken.None);
        await provider.TrimAsync(TreeId, ShardIndex, 0L, CancellationToken.None);
        await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 10, CancellationToken.None));

        Assert.That(table.CreateIfNotExistsCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task FlushPhaseTwoAsync_is_a_no_op_when_no_worker_exists()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task DisposeAsync_is_idempotent()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        await provider.DisposeAsync();
        Assert.That(async () => await provider.DisposeAsync(), Throws.Nothing);
    }

    [Test]
    public async Task ReadEncodedAsync_stops_at_maxEntries_on_a_batch_boundary()
    {
        // The cap is re-checked at the top of the manifest loop as well as in
        // the entry loop; a batch-aligned cap is what reaches the outer arm.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(4, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var page = await provider.ReadEncodedAsync(
            TreeId, ShardIndex, -1L, 4, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task FlushPhaseTwoAsync_observes_a_cancellable_token()
    {
        // The flush takes a different await path when the token can be
        // cancelled. The commit must still land, so the batch is readable
        // afterwards.
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        using var cts = new CancellationTokenSource();
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(cts.Token);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReconcileAsync_skips_a_candidate_row_whose_end_offset_precedes_its_start()
    {
        // A C-row whose recorded end offset is below its start offset cannot
        // describe a real batch. It must be skipped rather than planned, and
        // the well-formed orphan beside it must still be recovered.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(0L),
            Offset = 2L,
            Payload = null,
        });

        // The inverted candidate row: start 90, end 40.
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(90L),
            Offset = 40L,
            Payload = null,
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(2L),
            "the well-formed orphan is still rolled forward");
    }

    [Test]
    public async Task ReconcileAsync_rolls_back_an_orphan_discovered_without_a_candidate_row()
    {
        // In C-row-elided mode a rolled-back orphan has no candidate row to
        // delete, so roll-back must return after wiping the batch partition
        // rather than attempting a manifest delete. The gap below the second
        // batch is what routes it to roll-back.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();

        // Undo phase 2 for the second batch and wipe the first batch entirely,
        // leaving a gap beneath the survivor.
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(2L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(
            manifestPartitionKey,
            AzureTableWalStorageProvider.BuildManifestRowKey(0L),
            ETag.All,
            CancellationToken.None);
        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        var firstBatchPartition = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 0L);
        foreach (var row in table.Partition(firstBatchPartition))
        {
            await client.DeleteEntityAsync(row.PartitionKey, row.RowKey, ETag.All, CancellationToken.None);
        }

        // Raise TAIL above the surviving batch's start so it sits above a gap.
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = "TAIL",
            Offset = 0L,
            Payload = null,
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        var secondBatchPartition = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 2L);
        Assert.That(
            table.Partition(secondBatchPartition),
            Is.Empty,
            "the orphan above the gap must have its batch partition wiped");
    }

    [Test]
    public async Task ReconcileAsync_merges_candidate_row_and_partition_scan_discoveries()
    {
        // With C-row elision on, the reconciler runs both discovery scans and
        // unions them, preferring the C-row form for a batch that has both.
        // A table carrying one of each exercises the merge.
        var table = new InMemoryWalTable();
        var writer = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = false);
        await using (writer)
        {
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
            await writer.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None);
            await writer.FlushPhaseTwoAsync(CancellationToken.None);
        }

        var manifestPartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        var client = table.BuildTableClient();
        foreach (var start in new[] { 0L, 2L })
        {
            await client.DeleteEntityAsync(
                manifestPartitionKey,
                AzureTableWalStorageProvider.BuildManifestRowKey(start),
                ETag.All,
                CancellationToken.None);
        }

        await client.DeleteEntityAsync(manifestPartitionKey, "TAIL", ETag.All, CancellationToken.None);

        // Only the first batch keeps a candidate row; the second is
        // discoverable solely by the partition scan.
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(0L),
            Offset = 1L,
            Payload = null,
        });

        var recovered = CreateProvider(table, o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = recovered;

        await recovered.ReconcileAsync(TreeId, ShardIndex, CancellationToken.None);

        Assert.That(
            await recovered.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(3L),
            "both discovery routes must contribute to the same roll-forward");
        var readable = await DrainAsync(recovered.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(readable.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task ReadEncodedAsync_returns_an_empty_segment_for_an_empty_payload_row()
    {
        var table = new InMemoryWalTable();
        var provider = CreateProvider(table);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, ShardIndex, 0L);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = batchPartitionKey,
            RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(1L),
            Offset = 1L,
            Payload = null,
        });

        var page = await provider.ReadEncodedAsync(
            TreeId, ShardIndex, -1L, 10, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        Assert.That(page.Offsets.ToArray(), Is.EqualTo(new[] { 0L, 1L }));
        Assert.That(page.EncodedEntries.ToArray()[1].Count, Is.Zero);
    }
}
