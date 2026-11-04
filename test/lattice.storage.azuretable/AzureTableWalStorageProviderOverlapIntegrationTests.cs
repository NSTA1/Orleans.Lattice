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
/// Azurite-backed tests for overlap rejection in
/// <see cref="AzureTableWalStorageProvider"/>. Each batch lands in its own
/// partition keyed by its start offset, so without the provider's own
/// check a batch starting inside a written one is accepted and the shared
/// offsets read back twice.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalStorageProviderOverlapIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-overlap";

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
        _sut = CreateProvider();
        await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        _table = new TableClient(AzuriteConnectionString, _tableName);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _sut.DisposeAsync();
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
        }
    }

    [Test]
    public async Task A_batch_starting_inside_a_written_batch_is_rejected()
    {
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 3), CancellationToken.None);

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 2), CancellationToken.None),
            Throws.InvalidOperationException);

        await _sut.AppendBatchAsync(TreeId, 0, Entries(3, 2), CancellationToken.None);
        Assert.That(await ReadOffsetsAsync(_sut), Is.EqualTo(new long[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task A_batch_running_into_a_later_written_batch_is_rejected()
    {
        await _sut.AppendBatchAsync(TreeId, 0, Entries(4, 4), CancellationToken.None);

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 4), CancellationToken.None),
            Throws.InvalidOperationException);

        // Out-of-order arrival that does not overlap is still accepted.
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 4), CancellationToken.None);
        Assert.That(await ReadOffsetsAsync(_sut), Is.EqualTo(new long[] { 0, 1, 2, 3, 4, 5, 6, 7 }));
    }

    [Test]
    public async Task An_encoded_batch_overlapping_a_written_batch_is_rejected()
    {
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 3), CancellationToken.None);

        var overlapping = Entries(1, 3);
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);
        var segments = overlapping
            .Select(e =>
            {
                var record = Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.ToWalRecord(e.Mutation, LatticeMergeMode.LwwRegister, string.Empty);
                var writer = new System.Buffers.ArrayBufferWriter<byte>();
                encoder.Encode(in record, writer);
                return new ArraySegment<byte>(writer.WrittenSpan.ToArray());
            })
            .ToArray();
        var offsets = overlapping.Select(e => e.Offset).ToArray();

        Assert.That(
            async () => await _sut.AppendEncodedBatchAsync(TreeId, 0, segments, offsets, encoder, CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(await ReadOffsetsAsync(_sut), Is.EqualTo(new long[] { 0, 1, 2 }));
    }

    [Test]
    public async Task A_fresh_instance_rejects_overlap_with_a_committed_batch()
    {
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 3), CancellationToken.None);

        await using var other = CreateProvider();
        Assert.That(
            async () => await other.AppendBatchAsync(TreeId, 0, Entries(2, 2), CancellationToken.None),
            Throws.InvalidOperationException);

        await other.AppendBatchAsync(TreeId, 0, Entries(3, 2), CancellationToken.None);
        Assert.That(await ReadOffsetsAsync(other), Is.EqualTo(new long[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task A_fresh_instance_rejects_overlap_with_an_uncommitted_batch()
    {
        // Phase 1 landed but phase 2 never did, and no candidate row was
        // written: the batch is visible only through its own partition.
        await SynthesiseUncommittedBatchAsync(startOffset: 0L, entryCount: 3);

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, Entries(1, 3), CancellationToken.None),
            Throws.InvalidOperationException);

        await _sut.AppendBatchAsync(TreeId, 0, Entries(3, 2), CancellationToken.None);
    }

    [Test]
    public async Task Offsets_a_reconcile_rolled_back_can_be_written_again_at_a_different_start()
    {
        // The orphan predates this instance's first append, as one left
        // by a crashed earlier writer does.
        await SynthesiseUncommittedBatchAsync(startOffset: 10L, entryCount: 5);
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 2), CancellationToken.None);

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 10), CancellationToken.None),
            Throws.InvalidOperationException,
            "Before reconciliation the orphan's offsets are still written.");

        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 10), CancellationToken.None);

        Assert.That(await ReadOffsetsAsync(_sut), Is.EqualTo(Enumerable.Range(0, 12).Select(i => (long)i).ToArray()));
    }

    // ----- Two writers over one shard (issue #5799) -----
    //
    // A WAL shard activation on another silo cannot see this silo's abandoned
    // provider calls, so the allocator fix cannot stop two writers issuing the same
    // offsets. These pin that the storage layer fails closed when they do: entry
    // and manifest rows are insert-only and the replay proof compares content, so a
    // second batch at the same offsets is refused, never overwrites the first and
    // is never accepted as a replay of it. An acknowledged batch is never displaced.

    [Test]
    public async Task A_second_writer_reissuing_an_acknowledged_batchs_offsets_is_refused_and_changes_no_row()
    {
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 3, value: 1), CancellationToken.None);

        await using var other = CreateProvider();
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await other.AppendBatchAsync(TreeId, 0, Entries(0, 3, value: 2), CancellationToken.None),
                Throws.Exception, "same start and length, different content: not a replay");
            Assert.That(
                async () => await other.AppendBatchAsync(TreeId, 0, Entries(0, 2, value: 2), CancellationToken.None),
                Throws.Exception, "same start, shorter: not a replay");
            Assert.That(
                async () => await other.AppendBatchAsync(TreeId, 0, Entries(0, 4, value: 2), CancellationToken.None),
                Throws.Exception, "same start, longer");
            Assert.That(
                async () => await other.AppendBatchAsync(TreeId, 0, Entries(1, 3, value: 2), CancellationToken.None),
                Throws.Exception, "a later start inside the acknowledged batch");
        });

        await using var reader = CreateProvider();
        Assert.That(await ReadValuesAsync(reader), Is.EqualTo(new[] { (0L, 1), (1L, 1), (2L, 1) }),
            "the acknowledged batch is stored exactly once, unchanged");
    }

    [Test]
    public async Task A_second_writer_reissuing_the_offsets_of_a_batch_still_in_motion_is_refused()
    {
        // The first writer's phase 1 has landed and its phase 2 has not: its batch is
        // in motion on another silo, invisible to this instance's in-motion guard.
        await SynthesiseUncommittedBatchAsync(startOffset: 0L, entryCount: 3);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 3, value: 2), CancellationToken.None),
                Throws.Exception, "same start, different content");
            Assert.That(
                async () => await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 2, value: 2), CancellationToken.None),
                Throws.Exception, "a later start inside the batch in motion");
        });

        // The first writer's batch then completes; it holds its own content.
        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        await using var reader = CreateProvider();
        Assert.That(await ReadValuesAsync(reader), Is.EqualTo(new[] { (0L, 1), (1L, 1), (2L, 1) }));
    }

    [Test]
    public async Task A_late_manifest_row_from_an_abandoned_submit_never_displaces_an_acknowledged_batch()
    {
        // The residual from #5799: an abandoned phase-2 submit that the service applies
        // after the writer stopped waiting for it lands its manifest row late. Here the
        // orphan it belongs to, above a gap, was rolled back by a reconcile, the gap
        // was filled and a batch was acknowledged over the orphan's offsets.
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 2, value: 1), CancellationToken.None);
        await SynthesiseUncommittedBatchAsync(startOffset: 3L, entryCount: 3);
        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 4, value: 3), CancellationToken.None);

        // The late manifest row for the rolled-back orphan [3, 5] lands.
        var late = new AzureTableWalEntity
        {
            PartitionKey = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0),
            RowKey = AzureTableWalStorageProvider.BuildManifestRowKey(3L),
            Offset = 5L,
            Payload = null,
        };
        try
        {
            await _table.AddEntityAsync(late);
        }
        catch (RequestFailedException ex) when (ex.Status == 409)
        {
            // Already claimed by a manifest row of its own: refused, which is also fail-closed.
        }

        await using var reader = CreateProvider();
        Assert.That(await ReadValuesAsync(reader),
            Is.EqualTo(new[] { (0L, 1), (1L, 1), (2L, 3), (3L, 3), (4L, 3), (5L, 3) }),
            "every acknowledged offset still holds the write that was acknowledged there, once");
    }
    [Test]
    public async Task A_stale_writers_later_batch_never_lands_over_a_successors_acknowledged_batch()
    {
        // Issue #5810: a predecessor P (instance A) committed [0,3]. Its in-memory
        // written bound is 3, so it claims a later batch above it with no I/O. A
        // successor S (instance B, another silo) acknowledged [4,6] and then [7,9]
        // meanwhile. P's [5,6] lands in its own batch partition, keyed by start 5,
        // so nothing in storage collides with S's partition 4: offsets 5 and 6 are
        // written twice, and P's phase 2 upserts TAIL back to 6.
        await using var predecessor = CreateProvider();
        await predecessor.AppendBatchAsync(TreeId, 0, Entries(0, 4, value: 1), CancellationToken.None);

        await using var successor = CreateProvider();
        await successor.AppendBatchAsync(TreeId, 0, Entries(4, 3, value: 2), CancellationToken.None);
        await successor.AppendBatchAsync(TreeId, 0, Entries(7, 3, value: 2), CancellationToken.None);

        Exception? refused = null;
        try
        {
            await predecessor.AppendBatchAsync(TreeId, 0, Entries(5, 2, value: 9), CancellationToken.None);
        }
        catch (Exception ex)
        {
            refused = ex;
        }

        await using var reader = CreateProvider();
        var values = await ReadValuesAsync(reader);
        var tail = await reader.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.Not.Null, "the stale writer's overlapping batch was accepted");
            Assert.That(values, Is.EqualTo(new[]
            {
                (0L, 1), (1L, 1), (2L, 1), (3L, 1), (4L, 2), (5L, 2), (6L, 2), (7L, 2), (8L, 2), (9L, 2),
            }), "every offset is stored once, holding the acknowledged write");
            Assert.That(tail, Is.EqualTo(9L), "the stale writer's phase 2 regressed TAIL");
        });
    }
    [Test]
    public async Task A_regressed_tail_never_lets_a_recovered_writer_reissue_committed_offsets()
    {
        // Issue #5810, escalation: the successor S commits [4,9] as one batch, the
        // stale writer P's [5,6] then upserts TAIL back to 6, and a writer recovered
        // from storage (reconcile, then the highest offset) resumes at 7, inside S's
        // committed batch, at a start no batch partition holds.
        await using var predecessor = CreateProvider();
        await predecessor.AppendBatchAsync(TreeId, 0, Entries(0, 4, value: 1), CancellationToken.None);
        await using var successor = CreateProvider();
        await successor.AppendBatchAsync(TreeId, 0, Entries(4, 6, value: 2), CancellationToken.None);
        try
        {
            await predecessor.AppendBatchAsync(TreeId, 0, Entries(5, 2, value: 9), CancellationToken.None);
        }
        catch (Exception)
        {
            // Refused: the fence holds.
        }

        await using var recovered = CreateProvider();
        await recovered.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var next = await recovered.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None) + 1;
        Exception? reissue = null;
        try
        {
            await recovered.AppendBatchAsync(TreeId, 0, Entries(next, 1, value: 7), CancellationToken.None);
        }
        catch (Exception ex)
        {
            reissue = ex;
        }

        await using var reader = CreateProvider();
        var offsets = (await ReadValuesAsync(reader)).Select(v => v.Offset).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(next, Is.GreaterThanOrEqualTo(10L), $"recovery resumed at {next}, inside a committed batch");
            Assert.That(offsets, Is.Unique, "an offset is stored twice");
        });
    }
    [Test]
    public async Task A_steady_run_of_appends_commits_under_the_fence_with_no_extra_round_trip()
    {
        // Issue #5810, steady-path cost: the commit fence rides in the phase-2
        // transaction the append already makes and is refreshed from that
        // transaction's own response, so N appends in allocation order make exactly
        // the 2N transactions they made before (one phase 1, one phase 2 each) and
        // read nothing. An ETag the service failed to return would invalidate the
        // fence and send the next append back through a read of TAIL.
        var requests = new RequestCounter();
        await using var writer = CreateProvider(requests);
        await writer.AppendBatchAsync(TreeId, 0, Entries(0, 2), CancellationToken.None);
        var fence = writer.GetShardActivity(AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0)).Fence;
        Assert.That(fence.TryGetGeneration(out var generation), Is.True, "the first commit left the fence without an ETag");

        const int appends = 20;
        requests.Reset();
        for (var i = 0; i < appends; i++)
        {
            await writer.AppendBatchAsync(TreeId, 0, Entries(2 + (i * 2L), 2), CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(requests.Transactions, Is.EqualTo(2 * appends), "transactions per append changed");
            Assert.That(requests.Other, Is.Zero, "a steady append made a request beyond its two transactions");
            Assert.That(fence.TryGetGeneration(out var after), Is.True, "a steady commit invalidated the fence");
            Assert.That(after, Is.EqualTo(generation), "a steady commit advanced the fence generation");
        });
    }

    [Test]
    public async Task A_refused_stale_batchs_entry_rows_are_reclaimed_by_the_reconcile_its_refusal_triggers()
    {
        // Issue #5810, review nit N1: the stale writer's refused [5,6] leaves entry rows
        // in batch partition 5 with no manifest row, below the successor's TAIL, where
        // the discovery scan (above TAIL only) never looks. The resync its refusal
        // triggers must delete them, and nothing the successor committed.
        await using var predecessor = CreateProvider();
        await predecessor.AppendBatchAsync(TreeId, 0, Entries(0, 4, value: 1), CancellationToken.None);
        await using var successor = CreateProvider();
        await successor.AppendBatchAsync(TreeId, 0, Entries(4, 6, value: 2), CancellationToken.None);
        Assert.That(
            async () => await predecessor.AppendBatchAsync(TreeId, 0, Entries(5, 2, value: 9), CancellationToken.None),
            Throws.Exception,
            "the stale writer's batch was not refused");
        Assert.That(await CountBatchPartitionRowsAsync(5), Is.EqualTo(2), "the refused batch left no entry rows to reclaim");

        await predecessor.ReconcileAsync(TreeId, 0, CancellationToken.None);

        await using var reader = CreateProvider();
        var refusedRows = await CountBatchPartitionRowsAsync(5);
        var committedRows = await CountBatchPartitionRowsAsync(4);
        var values = await ReadValuesAsync(reader);
        Assert.Multiple(() =>
        {
            Assert.That(refusedRows, Is.Zero, "the refused batch's entry rows leaked");
            Assert.That(committedRows, Is.EqualTo(6), "reclaim deleted a committed batch's rows");
            Assert.That(values, Is.EqualTo(new[]
            {
                (0L, 1), (1L, 1), (2L, 1), (3L, 1), (4L, 2), (5L, 2), (6L, 2), (7L, 2), (8L, 2), (9L, 2),
            }));
        });
    }

    [Test]
    public async Task A_failed_commit_that_reconcile_rolls_forward_is_not_reclaimed()
    {
        // The reclaim deletes only a failed batch with no manifest row. A batch whose
        // commit failed but which reconciliation then rolls forward is committed, and
        // keeps its rows.
        var faults = new PhaseTwoFault();
        await using var writer = CreateProvider(requests: null, faults);
        await writer.AppendBatchAsync(TreeId, 0, Entries(0, 2, value: 1), CancellationToken.None);
        faults.FailNextPhaseTwo();
        Assert.That(
            async () => await writer.AppendBatchAsync(TreeId, 0, Entries(2, 2, value: 2), CancellationToken.None),
            Throws.Exception);

        await writer.ReconcileAsync(TreeId, 0, CancellationToken.None);

        await using var reader = CreateProvider();
        var values = await ReadValuesAsync(reader);
        var tail = await reader.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { (0L, 1), (1L, 1), (2L, 2), (3L, 2) }), "a rolled-forward batch was reclaimed");
            Assert.That(tail, Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task A_tail_moved_between_rollforward_chunks_refuses_the_next_chunk()
    {
        // Review nit N3: every rollforward chunk writes TAIL conditionally, chained on
        // the previous chunk's ETag, so once another writer moves TAIL no further chunk
        // of the pass adds a manifest row; the pass re-plans from fresh reads instead.
        // 99 single-entry orphans above TAIL roll forward in chunks of 49, 49 and 1.
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 1), CancellationToken.None);
        for (var offset = 1L; offset <= 99L; offset++)
        {
            await SynthesiseUncommittedBatchAsync(offset, 1);
        }

        await using var recovering = CreateProvider();
        var chunks = new List<int>();
        recovering.AfterRollForwardChunkForTesting = async chunk =>
        {
            chunks.Add(chunk);
            if (chunks.Count == 1)
            {
                // Another writer rewrites TAIL with the value it already holds,
                // which changes only its ETag.
                var tail = (await _table.GetEntityAsync<AzureTableWalEntity>(
                    AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0),
                    AzureTableWalStorageProvider.TailRowKey)).Value;
                await _table.UpdateEntityAsync(tail, tail.ETag, TableUpdateMode.Replace);
            }
        };

        await recovering.ReconcileAsync(TreeId, 0, CancellationToken.None);

        await using var reader = CreateProvider();
        var offsets = await ReadOffsetsAsync(reader);
        var resulting = await reader.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(chunks, Is.EqualTo(new[] { 0, 0, 1 }), "a chunk committed after another writer moved TAIL");
            Assert.That(offsets, Is.EqualTo(Enumerable.Range(0, 100).Select(o => (long)o).ToArray()));
            Assert.That(resulting, Is.EqualTo(99L));
        });
    }

    private async Task<int> CountBatchPartitionRowsAsync(long startOffset)
    {
        var partitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, 0, startOffset);
        var count = 0;
        await foreach (var _ in _table.QueryAsync<TableEntity>($"PartitionKey eq '{partitionKey}'", select: new[] { "RowKey" }))
        {
            count++;
        }

        return count;
    }

    private AzureTableWalStorageProvider CreateProvider() => CreateProvider(requests: null);

    // The default options eliminate the candidate row, so a steady append makes
    // exactly its phase-1 and phase-2 transactions.
    private AzureTableWalStorageProvider CreateProvider(RequestCounter? requests, PhaseTwoFault? faults = null) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = false,
                ConfigureClientOptions = requests is null && faults is null
                    ? null
                    : options =>
                    {
                        if (requests is not null)
                        {
                            options.AddPolicy(requests, HttpPipelinePosition.PerCall);
                        }

                        if (faults is not null)
                        {
                            options.AddPolicy(faults, HttpPipelinePosition.PerCall);
                        }
                    },
            }),
            _serializer);

    /// <summary>
    /// Fails, before it is sent, the second table transaction after it is armed: with
    /// the candidate row eliminated an append makes its phase-1 transaction and then its
    /// phase-2 one, so that is the next append's phase 2.
    /// </summary>
    private sealed class PhaseTwoFault : HttpPipelineSynchronousPolicy
    {
        private int _countdown;

        public void FailNextPhaseTwo() => Volatile.Write(ref _countdown, 2);

        public override void OnSendingRequest(HttpMessage message)
        {
            if (message.Request.Uri.Path.EndsWith("$batch", StringComparison.Ordinal)
                && Volatile.Read(ref _countdown) > 0
                && Interlocked.Decrement(ref _countdown) == 0)
            {
                throw new InvalidOperationException("Injected phase-2 transaction failure.");
            }
        }
    }

    /// <summary>Counts table transactions (<c>$batch</c>) and every other request.</summary>
    private sealed class RequestCounter : HttpPipelineSynchronousPolicy
    {
        private int _transactions;
        private int _other;

        public int Transactions => Volatile.Read(ref _transactions);

        public int Other => Volatile.Read(ref _other);

        public void Reset()
        {
            Volatile.Write(ref _transactions, 0);
            Volatile.Write(ref _other, 0);
        }

        public override void OnSendingRequest(HttpMessage message)
        {
            if (message.Request.Uri.Path.EndsWith("$batch", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref _transactions);
            }
            else
            {
                Interlocked.Increment(ref _other);
            }
        }
    }

    private async Task SynthesiseUncommittedBatchAsync(long startOffset, int entryCount)
    {
        var batchPartitionKey = AzureTableWalStorageProvider.BuildBatchPartitionKey(TreeId, 0, startOffset);
        var actions = new List<TableTransactionAction>(entryCount);
        foreach (var entry in Entries(startOffset, entryCount))
        {
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = batchPartitionKey,
                    RowKey = AzureTableWalStorageProvider.BuildEntryRowKey(entry.Offset),
                    Offset = entry.Offset,
                    Payload = _serializer.SerializeToArray(
                        Orleans.Lattice.BPlusTree.Grains.WalRecordConverter.ToWalRecord(
                            entry.Mutation, LatticeMergeMode.LwwRegister, string.Empty)),
                }));
        }

        await _table.SubmitTransactionAsync(actions, CancellationToken.None);
    }

    private static async Task<long[]> ReadOffsetsAsync(AzureTableWalStorageProvider provider)
    {
        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }

        return offsets.ToArray();
    }

    private static async Task<(long Offset, int Value)[]> ReadValuesAsync(AzureTableWalStorageProvider provider)
    {
        var values = new List<(long, int)>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            values.Add((entry.Offset, entry.Mutation.Value![0]));
        }

        return values.ToArray();
    }

    private static WalEntry[] Entries(long startOffset, int count) =>
        Entries(startOffset, count, value: 1);

    private static WalEntry[] Entries(long startOffset, int count, byte value) =>
        Enumerable.Range(0, count)
            .Select(i => startOffset + i)
            .Select(offset => new WalEntry
            {
                Offset = offset,
                Mutation = new LatticeMutation
                {
                    TreeId = TreeId,
                    Kind = MutationKind.Set,
                    Key = string.Create(CultureInfo.InvariantCulture, $"k{offset}"),
                    Value = new byte[] { value },
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    OriginClusterId = "site-a",
                },
            })
            .ToArray();
}
