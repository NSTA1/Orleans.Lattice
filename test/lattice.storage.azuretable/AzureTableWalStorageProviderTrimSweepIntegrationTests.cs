using System.Globalization;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Azurite-backed tests for the trim sweep in
/// <see cref="AzureTableWalStorageProvider.TrimAsync"/>: entry rows at or below the
/// persisted <c>TAIL</c> are deleted by one key-range sweep of the shard's batch
/// partitions, which also reaches a partition no manifest row names - the rows a
/// refused or failed batch leaves when its writer stops before reconciling
/// (issue #5810).
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalStorageProviderTrimSweepIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-sweep";

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
    public async Task SetUp()
    {
        _tableName = "T" + Guid.NewGuid().ToString("N");
        await using (var bootstrap = CreateProvider())
        {
            await bootstrap.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        }

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

    [Test]
    public async Task A_trim_reclaims_a_refused_batch_whose_writer_stopped_before_reconciling()
    {
        // Issue #5810: the stale writer's refused [5,6] leaves partition 5 with no
        // manifest row below the successor's TAIL, and its writer stops before the
        // reconcile that would reclaim it. A trim's sweep must.
        var predecessor = CreateProvider();
        await predecessor.AppendBatchAsync(TreeId, 0, Entries(TreeId, 0, 4), CancellationToken.None);
        await using var successor = CreateProvider();
        await successor.AppendBatchAsync(TreeId, 0, Entries(TreeId, 4, 6), CancellationToken.None);
        Assert.That(
            async () => await predecessor.AppendBatchAsync(TreeId, 0, Entries(TreeId, 5, 2), CancellationToken.None),
            Throws.Exception,
            "the stale writer's batch was not refused");
        await predecessor.DisposeAsync();
        Assert.That(await CountRowsAsync(TreeId, 0, 5), Is.EqualTo(2), "the refused batch left no rows to reclaim");

        await successor.TrimAsync(TreeId, 0, 6L, CancellationToken.None);

        var refused = await CountRowsAsync(TreeId, 0, 5);
        var straddling = await CountRowsAsync(TreeId, 0, 4);
        var offsets = await ReadOffsetsAsync(successor, TreeId, 0);
        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.Zero, "the refused batch's rows outlived a trim past them");
            Assert.That(straddling, Is.EqualTo(3), "the straddling batch kept other than its rows above the trimmed offset");
            Assert.That(offsets, Is.EqualTo(new[] { 7L, 8L, 9L }));
        });
    }

    [Test]
    public async Task A_steady_trim_sweeps_from_the_previous_watermark_and_a_trim_to_tail_from_the_first_partition()
    {
        // C3: a steady trim sweeps only from the batch holding the previous
        // watermark's successor, so a leak wholly below it is left; a trim that
        // reaches TAIL - a discard is one - sweeps the whole committed prefix.
        await using var writer = CreateProvider();
        for (var start = 0L; start < 400L; start += 100L)
        {
            await writer.AppendBatchAsync(TreeId, 0, Entries(TreeId, start, 100), CancellationToken.None);
        }

        await writer.TrimAsync(TreeId, 0, 199L, CancellationToken.None);
        await SynthesiseOrphanAsync(TreeId, 0, 250L);
        await SynthesiseOrphanAsync(TreeId, 0, 10L);

        await writer.TrimAsync(TreeId, 0, 299L, CancellationToken.None);
        var inSpan = await CountRowsAsync(TreeId, 0, 250);
        var belowSpan = await CountRowsAsync(TreeId, 0, 10);

        var head = await writer.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        await writer.TrimAsync(TreeId, 0, head, CancellationToken.None);
        var afterDiscard = await CountRowsAsync(TreeId, 0, 10);

        Assert.Multiple(() =>
        {
            Assert.That(inSpan, Is.Zero, "a steady trim left a leak inside the span it trimmed");
            Assert.That(belowSpan, Is.EqualTo(1), "a steady trim swept below the previous watermark's batch");
            Assert.That(head, Is.EqualTo(399L));
            Assert.That(afterDiscard, Is.Zero, "a trim through the head left a leak below the watermark");
        });
    }

    [Test]
    public async Task A_batch_accepted_above_tail_is_untouched_by_a_trim_through_the_highest_offset()
    {
        // C1: the highest offset folds in a batch whose phase 1 has landed and whose
        // phase 2 has not committed. It starts above the persisted TAIL, and the sweep
        // stops at that TAIL, so a trim through the highest offset - a discard - never
        // deletes the rows of a batch that is about to commit.
        var gate = new PhaseTwoGate();
        await using var writer = CreateProvider(gate);
        await writer.AppendBatchAsync(TreeId, 0, Entries(TreeId, 0, 4), CancellationToken.None);
        gate.HoldNextPhaseTwo();
        var pending = writer.AppendBatchAsync(TreeId, 0, Entries(TreeId, 4, 2), CancellationToken.None);
        await gate.Held.WaitAsync(TimeSpan.FromSeconds(30));

        long head;
        int rows;
        try
        {
            head = await writer.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
            await writer.TrimAsync(TreeId, 0, head, CancellationToken.None);
            rows = await CountRowsAsync(TreeId, 0, 4);
        }
        finally
        {
            gate.Release();
        }

        await pending;
        Assert.Multiple(() =>
        {
            Assert.That(head, Is.EqualTo(5L), "the highest offset did not fold in the accepted batch");
            Assert.That(rows, Is.EqualTo(2), "a trim deleted the rows of a batch accepted above TAIL");
        });
    }

    [Test]
    public async Task A_trim_never_touches_another_shard_or_tree()
    {
        // Shard 12 shares shard 1's leading digit, and tree "tree-sweep-x" extends
        // "tree-sweep"; each holds committed rows and a leak below its own TAIL, and a
        // trim of tree-sweep shard 1 through its TAIL must leave every one of them.
        const string AdjacentTree = TreeId + "-x";
        await using var writer = CreateProvider();
        foreach (var (tree, shard) in new[] { (TreeId, 1), (TreeId, 12), (AdjacentTree, 1) })
        {
            await writer.AppendBatchAsync(tree, shard, Entries(tree, 0, 4), CancellationToken.None);
            await SynthesiseOrphanAsync(tree, shard, 2L);
        }

        await writer.TrimAsync(TreeId, 1, 3L, CancellationToken.None);

        var trimmed = await CountRowsAsync(TreeId, 1, 2);
        var otherShard = await CountRowsAsync(TreeId, 12, 0) + await CountRowsAsync(TreeId, 12, 2);
        var otherTree = await CountRowsAsync(AdjacentTree, 1, 0) + await CountRowsAsync(AdjacentTree, 1, 2);
        Assert.Multiple(() =>
        {
            Assert.That(trimmed, Is.Zero);
            Assert.That(otherShard, Is.EqualTo(5), "a trim of shard 1 deleted rows of shard 12");
            Assert.That(otherTree, Is.EqualTo(5), "a trim of one tree deleted rows of an adjacent tree");
        });
    }

    [Test]
    public async Task A_manifest_row_above_the_persisted_tail_survives_a_trim_through_tail()
    {
        // #3348: a committed batch whose manifest row sits above a TAIL a racing
        // writer lowered. A trim through TAIL does not cover it, and the sweep, capped
        // at TAIL in both partition and row, never reaches it.
        await using var writer = CreateProvider();
        await writer.AppendBatchAsync(TreeId, 0, Entries(TreeId, 0, 4), CancellationToken.None);
        await SynthesiseOrphanAsync(TreeId, 0, 4L, count: 3);
        await _table.AddEntityAsync(new AzureTableWalEntity
        {
            PartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0),
            RowKey = AzureTableWalStorageProvider.BuildManifestRowKey(4L),
            Offset = 6L,
        });

        await writer.TrimAsync(TreeId, 0, 3L, CancellationToken.None);

        var rows = await CountRowsAsync(TreeId, 0, 4);
        var manifest = await _table.GetEntityIfExistsAsync<TableEntity>(
            AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0),
            AzureTableWalStorageProvider.BuildManifestRowKey(4L));
        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo(3), "a trim through TAIL deleted a batch committed above it");
            Assert.That(manifest.HasValue, Is.True, "a trim through TAIL deleted a manifest row above it");
        });
    }

    private AzureTableWalStorageProvider CreateProvider(PhaseTwoGate? gate = null) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = false,
                ConfigureClientOptions = gate is null ? null : options => options.AddPolicy(gate, HttpPipelinePosition.PerCall),
            }),
            _serializer);

    private async Task SynthesiseOrphanAsync(string treeId, int shardIndex, long startOffset, int count = 1)
    {
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(treeId, shardIndex, startOffset);
        var actions = Entries(treeId, startOffset, count)
            .Select(entry => new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = batchPartitionKey,
                    RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(entry.Offset),
                    Offset = entry.Offset,
                    Payload = _serializer.SerializeToArray(
                        Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.ToWalRecord(
                            entry.Mutation, LatticeMergeMode.LwwRegister, string.Empty)),
                }))
            .ToList();
        await _table.SubmitTransactionAsync(actions, CancellationToken.None);
    }

    private async Task<int> CountRowsAsync(string treeId, int shardIndex, long startOffset)
    {
        var partitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(treeId, shardIndex, startOffset);
        var count = 0;
        await foreach (var _ in _table.QueryAsync<TableEntity>($"PartitionKey eq '{partitionKey}'", select: new[] { "RowKey" }))
        {
            count++;
        }

        return count;
    }

    private static async Task<long[]> ReadOffsetsAsync(AzureTableWalStorageProvider provider, string treeId, int shardIndex)
    {
        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync(treeId, shardIndex, -1L, 1024, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }

        return offsets.ToArray();
    }

    private static WalEntry[] Entries(string treeId, long startOffset, int count) =>
        Enumerable.Range(0, count)
            .Select(i => startOffset + i)
            .Select(offset => new WalEntry
            {
                Offset = offset,
                Mutation = new LatticeMutation
                {
                    TreeId = treeId,
                    Kind = MutationKind.Set,
                    Key = string.Create(CultureInfo.InvariantCulture, $"k{offset}"),
                    Value = new byte[] { 1 },
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    OriginClusterId = "site-a",
                },
            })
            .ToArray();

    /// <summary>
    /// Holds, until released, the second table transaction after it is armed: with
    /// the candidate row eliminated an append makes its phase-1 transaction and then
    /// its phase-2 one, so that is the next append's phase 2. Every other request
    /// passes.
    /// </summary>
    private sealed class PhaseTwoGate : HttpPipelinePolicy
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _countdown;

        public Task Held => _held.Task;

        public void HoldNextPhaseTwo() => Volatile.Write(ref _countdown, 2);

        public void Release() => _released.TrySetResult();

        public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            if (IsHeld(message))
            {
                _held.TrySetResult();
                await _released.Task.ConfigureAwait(false);
            }

            await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            if (IsHeld(message))
            {
                _held.TrySetResult();
                _released.Task.GetAwaiter().GetResult();
            }

            ProcessNext(message, pipeline);
        }

        private bool IsHeld(HttpMessage message) =>
            message.Request.Uri.Path.EndsWith("$batch", StringComparison.Ordinal)
            && Volatile.Read(ref _countdown) > 0
            && Interlocked.Decrement(ref _countdown) == 0;
    }
}
