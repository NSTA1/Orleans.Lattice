using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// End-to-end tests for <see cref="AzureTableWalStorageProvider"/>
/// driven against an Azure Table Storage endpoint - canonically
/// <see href="https://learn.microsoft.com/azure/storage/common/storage-use-azurite">Azurite</see>
/// listening on the default development connection string. Gated under
/// the <c>AzureTableEmulator</c> NUnit category so the default dev
/// loop (which excludes that category) skips them when no emulator is
/// running; <see cref="OneTimeSetUp"/> probes reachability and falls
/// through to <see cref="Assert.Inconclusive(string)"/> if the probe
/// fails.
/// <para>
/// Each test uses a fresh, GUID-named table created on first provider
/// use and torn down in <see cref="TearDown"/>; tests therefore
/// exercise the provider's own table-create-on-first-use codepath
/// rather than relying on an out-of-band setup step.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureTableEmulator")]
public class AzureTableWalStorageProviderIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-int";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private AzureTableWalStorageProvider _sut = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _adminClient = new TableServiceClient(AzuriteConnectionString);

        try
        {
            // Issue a cheap query to prove Azurite (or a real account
            // wired up via `UseDevelopmentStorage=true`) is reachable.
            // The enumerator is consumed with a take-1 so we don't
            // page beyond the first batch.
            await foreach (var _ in _adminClient.QueryAsync(maxPerPage: 1))
            {
                break;
            }
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Azurite is not reachable on the default development endpoint ({AzuriteConnectionString}). "
                + $"Start it via 'azurite --silent --location <dir>' or skip the AzureTableEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        // Per-test unique table name: 'T' (letter prefix required by
        // Azure) + 32 hex chars = 33 chars, alphanumeric, well within
        // the 3-63 char rule. Uniqueness avoids cross-test bleed even
        // if a teardown was missed on a prior crashed run.
        _tableName = "T" + Guid.NewGuid().ToString("N");
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
                // These end-to-end tests assert read-your-writes
                // durability: TAIL has advanced and the entries are
                // readable the instant AppendBatchAsync returns. That
                // contract only holds in synchronous phase-2 mode; the
                // throughput default (PipelinePhaseTwoCommits = true)
                // defers a batch's phase-2 commit until the next append
                // on the shard, so an immediate GetHighestOffsetAsync
                // would observe the pre-batch TAIL. The pipelined path
                // has its own white-box coverage in
                // AzureTableWalStorageProviderPhaseTwoPipeliningTests.
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

    private static async Task<List<WalEntry>> ReadAllAsync(
        AzureTableWalStorageProvider sut,
        string tree,
        int shard,
        long fromOffsetExclusive = -1L,
        int maxEntries = 1024)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(tree, shard, fromOffsetExclusive, maxEntries, CancellationToken.None))
        {
            collected.Add(entry);
        }
        return collected;
    }

    [Test]
    public async Task GetHighestOffsetAsync_returns_minus_one_for_empty_partition()
    {
        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.That(head, Is.EqualTo(-1L));
    }

    [Test]
    public async Task AppendBatchAsync_persists_entries_and_advances_head()
    {
        var batch = new[] { Entry(0), Entry(1), Entry(2) };

        await _sut.AppendBatchAsync(TreeId, 0, batch, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(2L));
    }

    [Test]
    public async Task ReadAsync_yields_entries_in_dense_offset_order()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var read = await ReadAllAsync(_sut, TreeId, 0);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReadAsync_respects_fromOffsetExclusive_lower_bound()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var read = await ReadAllAsync(_sut, TreeId, 0, fromOffsetExclusive: 1L);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L, 3L }));
    }

    [Test]
    public async Task ReadAsync_caps_yield_at_maxEntries()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        var read = await ReadAllAsync(_sut, TreeId, 0, fromOffsetExclusive: -1L, maxEntries: 2);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task ReadAsync_round_trips_every_LatticeMutation_field()
    {
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "users/42",
            Value = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },
            Timestamp = hlc,
            IsTombstone = false,
            ExpiresAtTicks = 1_700_000_000_000L,
            OriginClusterId = "site-b",
        };
        var entry = new WalEntry { Offset = 0L, Mutation = mutation };

        await _sut.AppendBatchAsync(TreeId, 0, new[] { entry }, CancellationToken.None);
        var read = await ReadAllAsync(_sut, TreeId, 0);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(read[0].Offset, Is.EqualTo(0L));
        Assert.That(read[0].Mutation.TreeId, Is.EqualTo(TreeId));
        Assert.That(read[0].Mutation.Kind, Is.EqualTo(MutationKind.Set));
        Assert.That(read[0].Mutation.Key, Is.EqualTo("users/42"));
        Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }));
        Assert.That(read[0].Mutation.Timestamp, Is.EqualTo(hlc));
        Assert.That(read[0].Mutation.IsTombstone, Is.False);
        Assert.That(read[0].Mutation.ExpiresAtTicks, Is.EqualTo(1_700_000_000_000L));
        Assert.That(read[0].Mutation.OriginClusterId, Is.EqualTo("site-b"));
    }

    [Test]
    public async Task GetHighestOffsetAsync_recovers_persisted_head_on_a_fresh_provider()
    {
        // First provider writes the partition.
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        // Spin up a new provider over the same table - simulating a
        // silo restart - and confirm the persisted head is observable.
        var recovered = CreateProvider(_tableName);
        var head = await recovered.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(recovered, TreeId, 0);

        Assert.That(head, Is.EqualTo(2L));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task AppendBatchAsync_isolates_shards_in_the_same_tree()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await _sut.AppendBatchAsync(TreeId, 1, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var headShard0 = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var headShard1 = await _sut.GetHighestOffsetAsync(TreeId, 1, CancellationToken.None);

        Assert.That(headShard0, Is.EqualTo(1L));
        Assert.That(headShard1, Is.EqualTo(2L));
    }

    [Test]
    public async Task AppendBatchAsync_isolates_distinct_trees()
    {
        const string OtherTree = "tree-int-other";

        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0) }, CancellationToken.None);
        await _sut.AppendBatchAsync(OtherTree, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        Assert.That(await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(0L));
        Assert.That(await _sut.GetHighestOffsetAsync(OtherTree, 0, CancellationToken.None), Is.EqualTo(1L));
    }

    [Test]
    public async Task AppendBatchAsync_round_trips_a_tree_id_with_disallowed_partition_characters()
    {
        // '/' is reserved in Azure Table partition keys; the provider
        // must percent-encode it for the partition key while leaving
        // the LatticeMutation.TreeId field unmodified end-to-end.
        const string AwkwardTree = "tenant/9?tag#x";
        var entry = new WalEntry
        {
            Offset = 0L,
            Mutation = new LatticeMutation
            {
                TreeId = AwkwardTree,
                Kind = MutationKind.Set,
                Key = "k",
                Value = new byte[] { 1 },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };

        await _sut.AppendBatchAsync(AwkwardTree, 0, new[] { entry }, CancellationToken.None);
        var head = await _sut.GetHighestOffsetAsync(AwkwardTree, 0, CancellationToken.None);
        var read = await ReadAllAsync(_sut, AwkwardTree, 0);

        Assert.That(head, Is.EqualTo(0L));
        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(read[0].Mutation.TreeId, Is.EqualTo(AwkwardTree));
    }

    [Test]
    public async Task AppendBatchAsync_supports_a_full_100_entry_transaction()
    {
        // The 100-action batch cap is the load-bearing batching
        // invariant for the per-batch schema (phase 1 holds entry
        // rows only; no HEAD sentinel). Exercise the cap end-to-end
        // so a future SDK-side tightening surfaces here rather than
        // at runtime in CI.
        var entries = Enumerable
            .Range(0, AzureTableWalStorageProvider.MaxEntriesPerBatch)
            .Select(i => Entry(i))
            .ToArray();

        await _sut.AppendBatchAsync(TreeId, 0, entries, CancellationToken.None);
        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(_sut, TreeId, 0, maxEntries: AzureTableWalStorageProvider.MaxEntriesPerBatch);

        Assert.That(head, Is.EqualTo(AzureTableWalStorageProvider.MaxEntriesPerBatch - 1));
        Assert.That(read, Has.Count.EqualTo(AzureTableWalStorageProvider.MaxEntriesPerBatch));
    }

    [Test]
    public async Task TrimAsync_removes_only_entries_at_or_below_the_threshold()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);

        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 1L, CancellationToken.None);

        var read = await ReadAllAsync(_sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 2L, 3L }));

        // Head sentinel is left untouched - trim does not roll the
        // monotonic head back, only the live tail.
        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(3L));
    }

    [Test]
    public async Task TrimAsync_through_negative_offset_is_a_noop()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: -1L, CancellationToken.None);

        var read = await ReadAllAsync(_sut, TreeId, 0);
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public async Task TrimAsync_past_end_removes_every_entry_and_preserves_head()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 100L, CancellationToken.None);

        var read = await ReadAllAsync(_sut, TreeId, 0);
        Assert.That(read, Is.Empty);

        // GetHighestOffsetAsync still serves the recorded head so the
        // next append continues the dense sequence rather than
        // colliding with a previously-used offset.
        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.That(head, Is.EqualTo(1L));
    }

    [Test]
    public async Task TrimAsync_isolates_shards_within_a_tree()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await _sut.AppendBatchAsync(TreeId, 1, new[] { Entry(0), Entry(1) }, CancellationToken.None);

        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 100L, CancellationToken.None);

        Assert.That(await ReadAllAsync(_sut, TreeId, 0), Is.Empty);
        var shard1 = await ReadAllAsync(_sut, TreeId, 1);
        Assert.That(shard1.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
    }

    [Test]
    public void AppendBatchAsync_rejects_a_non_dense_batch_and_writes_nothing()
    {
        var batch = new[] { Entry(0), Entry(2) };

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AppendBatchAsync_validation_failure_leaves_observable_state_untouched()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0) }, CancellationToken.None);

        var bogus = new[] { Entry(1), Entry(3) };
        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, bogus, CancellationToken.None),
            Throws.ArgumentException);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(_sut, TreeId, 0);
        Assert.That(head, Is.EqualTo(0L));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L }));
    }

    [Test]
    public void AppendBatchAsync_rejects_a_batch_that_exceeds_the_transactional_cap()
    {
        var oversized = Enumerable
            .Range(0, AzureTableWalStorageProvider.MaxEntriesPerBatch + 1)
            .Select(i => Entry(i))
            .ToArray();

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, oversized, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void AppendBatchAsync_observes_a_pre_cancelled_token()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0) }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void GetHighestOffsetAsync_observes_a_pre_cancelled_token()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _sut.GetHighestOffsetAsync(TreeId, 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_when_partition_has_never_been_written()
    {
        // Untouched partition - no entry rows in the table at all.
        var lowest = await _sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_zero_on_untrimmed_shard()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);

        var lowest = await _sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_first_surviving_offset_after_partial_trim()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2), Entry(3) }, CancellationToken.None);
        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 1L, CancellationToken.None);

        var lowest = await _sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(2L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_returns_minus_one_after_full_trim()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);
        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 2L, CancellationToken.None);

        var lowest = await _sut.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(-1L));
    }

    [Test]
    public async Task GetLowestOffsetAsync_recovers_persisted_low_water_mark_on_a_fresh_provider()
    {
        // Symmetric to GetHighestOffsetAsync_recovers_persisted_head_on_a_fresh_provider:
        // confirm the low-water-mark query reads from durable storage,
        // not from any process-local cache.
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);
        await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 0L, CancellationToken.None);

        var recovered = CreateProvider(_tableName);
        var lowest = await recovered.GetLowestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.That(lowest, Is.EqualTo(1L));
    }

    [Test]
    public void GetLowestOffsetAsync_observes_a_pre_cancelled_token()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _sut.GetLowestOffsetAsync(TreeId, 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void TrimAsync_observes_a_pre_cancelled_token()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _sut.TrimAsync(TreeId, 0, throughOffsetInclusive: 0L, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task ReadAsync_returns_empty_when_partition_has_never_been_written()
    {
        var read = await ReadAllAsync(_sut, TreeId, 7);

        Assert.That(read, Is.Empty);
    }

    [Test]
    public async Task AppendBatchAsync_supports_appending_to_the_same_partition_twice()
    {
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1) }, CancellationToken.None);
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(2), Entry(3) }, CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(_sut, TreeId, 0);

        Assert.That(head, Is.EqualTo(3L));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task AppendBatchAsync_concurrent_distinct_batches_into_one_shard_all_persist()
    {
        // Two concurrent appends into the same shard land in
        // distinct batch partitions (per-batch partition + manifest
        // schema), so Azure Tables serves them in parallel; the
        // per-shard phase-2 worker then drains them in strict
        // ascending start-offset order and produces a monotonic
        // TAIL. Both batches must round-trip and the manifest must
        // be readable in ascending order.
        var batchA = new[] { Entry(0), Entry(1) };
        var batchB = new[] { Entry(2), Entry(3), Entry(4) };

        // Kick both batches off; the phase-2 worker is responsible
        // for the strict-order TAIL, so the call order here only
        // controls which batch hits phase 1 first. Pre-await both
        // to confirm neither fails.
        var taskA = _sut.AppendBatchAsync(TreeId, 0, batchA, CancellationToken.None);
        var taskB = _sut.AppendBatchAsync(TreeId, 0, batchB, CancellationToken.None);
        await Task.WhenAll(taskA, taskB);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(_sut, TreeId, 0);

        Assert.That(head, Is.EqualTo(4L), "TAIL must advance to the highest end offset across both batches");
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
    }

    [Test]
    public async Task AppendEncodedBatchAsync_round_trips_pre_encoded_payload_bytes()
    {
        // The zero-copy append path hands the WAL grain's
        // already-encoded payload bytes straight to the row's
        // Payload column - no second encode. The provider overrides
        // the default interface implementation; this test exercises
        // the override end-to-end.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var recordOne = new WalRecord
        {
            TreeId = TreeId,
            Op = MutationKind.Set,
            Key = "encoded-zero",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
        };
        var recordTwo = new WalRecord
        {
            TreeId = TreeId,
            Op = MutationKind.Delete,
            Key = "encoded-one",
            Value = Array.Empty<byte>(),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            OriginClusterId = "site-b",
            Mode = LatticeMergeMode.LwwRegister,
        };

        // Encode each record through the encoder. The provider
        // accepts ArraySegment<byte> rentals; an owned byte[] is
        // fine for the test surface.
        var writerOne = new System.Buffers.ArrayBufferWriter<byte>();
        encoder.Encode(recordOne, writerOne);
        var writerTwo = new System.Buffers.ArrayBufferWriter<byte>();
        encoder.Encode(recordTwo, writerTwo);
        var segments = new[]
        {
            new ArraySegment<byte>(writerOne.WrittenSpan.ToArray()),
            new ArraySegment<byte>(writerTwo.WrittenSpan.ToArray()),
        };
        var offsets = new long[] { 0L, 1L };

        await _sut.AppendEncodedBatchAsync(
            TreeId,
            0,
            new ReadOnlyMemory<ArraySegment<byte>>(segments),
            new ReadOnlyMemory<long>(offsets),
            encoder,
            CancellationToken.None);

        var head = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(_sut, TreeId, 0);

        Assert.That(head, Is.EqualTo(1L));
        Assert.That(read, Has.Count.EqualTo(2));
        Assert.That(read[0].Mutation.Key, Is.EqualTo("encoded-zero"));
        Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(read[1].Mutation.Key, Is.EqualTo("encoded-one"));
        Assert.That(read[1].Mutation.Kind, Is.EqualTo(MutationKind.Delete));
    }

    [Test]
    public void AppendEncodedBatchAsync_rejects_offset_segment_length_mismatch()
    {
        // ReadOnlyMemory<ArraySegment<byte>> and ReadOnlyMemory<long>
        // are parallel sequences; the provider must reject a length
        // mismatch synchronously without any I/O.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var segments = new ArraySegment<byte>[] { new(new byte[] { 1 }) };
        var offsets = new long[] { 0L, 1L };

        Assert.That(
            async () => await _sut.AppendEncodedBatchAsync(
                TreeId,
                0,
                new ReadOnlyMemory<ArraySegment<byte>>(segments),
                new ReadOnlyMemory<long>(offsets),
                encoder,
                CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AppendBatchAsync_persists_after_provider_dispose_and_reactivation()
    {
        // The phase-2 worker is awaited synchronously inside
        // AppendBatchAsync, so the visible TAIL is durable by the
        // time the call returns even if the provider is disposed
        // immediately afterwards. A fresh provider on the same
        // table must observe the persisted state.
        await _sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0), Entry(1), Entry(2) }, CancellationToken.None);
        await _sut.DisposeAsync();

        var recovered = CreateProvider(_tableName);
        var head = await recovered.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        var read = await ReadAllAsync(recovered, TreeId, 0);

        Assert.That(head, Is.EqualTo(2L));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task AppendBatchAsync_concurrent_appends_across_distinct_shards_do_not_serialise()
    {
        // Pins the cross-shard parallelism characteristic at the
        // behavioural level: distinct shards must accept concurrent
        // appends and each shard's TAIL must reflect only that
        // shard's commits. The structural invariant (one
        // PhaseTwoWorker per (treeId, shardIndex), no cross-shard
        // lock) is pinned by the white-box unit tests in
        // AzureTableWalStorageProviderTests; this test exercises
        // the same property end-to-end against Azurite.
        const int shardCount = 8;
        const int entriesPerShard = 4;

        var tasks = new Task[shardCount];
        for (var shard = 0; shard < shardCount; shard++)
        {
            var capturedShard = shard;
            var batch = Enumerable
                .Range(0, entriesPerShard)
                .Select(i => Entry(i, key: $"shard-{capturedShard}-{i}"))
                .ToArray();
            tasks[shard] = _sut.AppendBatchAsync(TreeId, capturedShard, batch, CancellationToken.None);
        }
        await Task.WhenAll(tasks);

        // Every shard's TAIL must reflect its own batch independently.
        for (var shard = 0; shard < shardCount; shard++)
        {
            var head = await _sut.GetHighestOffsetAsync(TreeId, shard, CancellationToken.None);
            Assert.That(head, Is.EqualTo((long)(entriesPerShard - 1)),
                $"shard {shard}: every shard's TAIL is independent of every other shard's commits");

            var read = await ReadAllAsync(_sut, TreeId, shard);
            Assert.That(read.Select(e => e.Offset),
                Is.EqualTo(Enumerable.Range(0, entriesPerShard).Select(i => (long)i)),
                $"shard {shard}: per-shard read must yield only this shard's entries");
        }
    }

    [Test]
    public async Task AppendBatchAsync_concurrent_appends_across_distinct_trees_all_persist()
    {
        // Distinct tree ids land in distinct manifest partition keys
        // (`_m_|<tree>|<shard>`), so the per-tree workers are also
        // independent. This is the cross-tree counterpart of the
        // cross-shard test: a tree-id-keyed silo bottleneck is one
        // of the easier mistakes to make in a refactor, so the
        // characteristic is worth pinning explicitly.
        const int treeCount = 4;
        var treeIds = Enumerable.Range(0, treeCount).Select(i => $"tree-conc-{i}").ToArray();

        var tasks = new Task[treeCount];
        for (var i = 0; i < treeCount; i++)
        {
            var tree = treeIds[i];
            tasks[i] = _sut.AppendBatchAsync(
                tree,
                shardIndex: 0,
                new[] { Entry(0, key: $"{tree}-k0"), Entry(1, key: $"{tree}-k1") },
                CancellationToken.None);
        }
        await Task.WhenAll(tasks);

        for (var i = 0; i < treeCount; i++)
        {
            var tree = treeIds[i];
            var head = await _sut.GetHighestOffsetAsync(tree, 0, CancellationToken.None);
            Assert.That(head, Is.EqualTo(1L), $"tree {tree}: independent TAIL");

            var read = await ReadAllAsync(_sut, tree, 0);
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
            Assert.That(read.Select(e => e.Mutation.Key),
                Is.EqualTo(new[] { $"{tree}-k0", $"{tree}-k1" }),
                $"tree {tree}: per-tree partitioning means no key bleed across trees");
        }
    }

    [Test]
    public async Task ReadEncodedAsync_returns_row_payload_bytes_verbatim()
    {
        // Pin the override behaviour: ReadEncodedAsync must return the
        // exact byte sequences AppendEncodedBatchAsync was handed, so
        // a future shipper one-encode fast path can stream them
        // straight into the outbound framing encoder without an
        // intermediate strongly-typed materialisation.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var record = new WalRecord
        {
            TreeId = TreeId,
            Op = MutationKind.Set,
            Key = "encoded-read",
            Value = new byte[] { 0x10, 0x20, 0x30 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
        };
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        encoder.Encode(record, writer);
        var producedBytes = writer.WrittenSpan.ToArray();

        await _sut.AppendEncodedBatchAsync(
            TreeId,
            0,
            new ReadOnlyMemory<ArraySegment<byte>>(new[] { new ArraySegment<byte>(producedBytes) }),
            new ReadOnlyMemory<long>(new[] { 0L }),
            encoder,
            CancellationToken.None);

        var page = await _sut.ReadEncodedAsync(TreeId, 0, -1L, 64, encoder, CancellationToken.None);

        Assert.That(page.EncodedEntries.Length, Is.EqualTo(1));
        Assert.That(page.Offsets.Length, Is.EqualTo(1));
        Assert.That(page.Offsets.Span[0], Is.EqualTo(0L));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(0L));
        Assert.That(
            page.EncodedEntries.Span[0].ToArray(),
            Is.EqualTo(producedBytes),
            "ReadEncodedAsync must hand back the exact bytes AppendEncodedBatchAsync was given - no re-encode round-trip");

        // And the segment must still decode through the same encoder
        // to the same record.
        var decoded = encoder.Decode(page.EncodedEntries.Span[0].AsSpan());
        Assert.That(decoded.Key, Is.EqualTo("encoded-read"));
        Assert.That(decoded.Value, Is.EqualTo(new byte[] { 0x10, 0x20, 0x30 }));
    }

    [Test]
    public async Task ReadEncodedAsync_respects_fromOffsetExclusive_and_maxEntries()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        // Append four entries via the legacy AppendBatchAsync path so
        // the rows are populated via the production BuildEntryEntity
        // (which now persists WalRecord bytes). The bytes the override
        // returns are the row payload bytes, which round-trip through
        // the same encoder.
        var entries = new[] { Entry(0, "a"), Entry(1, "b"), Entry(2, "c"), Entry(3, "d") };
        await _sut.AppendBatchAsync(TreeId, 0, entries, CancellationToken.None);

        var page = await _sut.ReadEncodedAsync(TreeId, 0, fromOffsetExclusive: 0L, maxEntries: 2, encoder, CancellationToken.None);

        Assert.That(page.Offsets.Span.ToArray(), Is.EqualTo(new[] { 1L, 2L }));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(2L));
        var decoded = encoder.Decode(page.EncodedEntries.Span[0].AsSpan());
        Assert.That(decoded.Key, Is.EqualTo("b"));
    }

    [Test]
    public async Task ReadEncodedAsync_1024_entries_round_trip_through_decode_match_ReadAsync()
    {
        // The acceptance row pins 1024-entry round-trip
        // equivalence between ReadAsync and ReadEncodedAsync.
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        const int N = 1024;
        // Azure Table Storage caps a single transactional batch at 100
        // actions; chunk the 1024 append into 100-entry segments so
        // each call stays inside the per-batch limit. The provider
        // sees them as 11 distinct batches but the manifest threads
        // them into one contiguous offset range.
        const int ChunkSize = 100;
        var entries = new WalEntry[N];
        for (var i = 0; i < N; i++)
        {
            entries[i] = Entry(i, "k-" + i.ToString("D4"), (byte)(i & 0xFF));
        }
        for (var start = 0; start < N; start += ChunkSize)
        {
            var len = Math.Min(ChunkSize, N - start);
            var chunk = new WalEntry[len];
            Array.Copy(entries, start, chunk, 0, len);
            await _sut.AppendBatchAsync(TreeId, 0, chunk, CancellationToken.None);
        }

        var classicEntries = await ReadAllAsync(_sut, TreeId, 0);
        var page = await _sut.ReadEncodedAsync(TreeId, 0, -1L, N, encoder, CancellationToken.None);

        Assert.That(classicEntries, Has.Count.EqualTo(N));
        Assert.That(page.EncodedEntries.Length, Is.EqualTo(N));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(N - 1L));

        var segments = page.EncodedEntries.Span;
        var offsets = page.Offsets.Span;
        for (var i = 0; i < N; i++)
        {
            Assert.That(offsets[i], Is.EqualTo(classicEntries[i].Offset), $"offset mismatch at {i}");
            var decoded = encoder.Decode(segments[i].AsSpan());
            Assert.Multiple(() =>
            {
                Assert.That(decoded.Key, Is.EqualTo(classicEntries[i].Mutation.Key), $"key[{i}]");
                Assert.That(decoded.Value, Is.EqualTo(classicEntries[i].Mutation.Value), $"value[{i}]");
                Assert.That(decoded.Op, Is.EqualTo(classicEntries[i].Mutation.Kind), $"op[{i}]");
            });
        }
    }

    [Test]
    public async Task ReadEncodedAsync_returns_empty_page_for_unwritten_shard()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var page = await _sut.ReadEncodedAsync(TreeId, 7, -1L, 64, encoder, CancellationToken.None);

        Assert.That(page.EncodedEntries.Length, Is.EqualTo(0));
        Assert.That(page.Offsets.Length, Is.EqualTo(0));
        Assert.That(page.HighestOffsetInclusive, Is.EqualTo(-1L));
    }

    [Test]
    public void ReadEncodedAsync_throws_on_null_treeId()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        Assert.That(
            async () => await _sut.ReadEncodedAsync(null!, 0, -1L, 1, encoder, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReadEncodedAsync_throws_on_null_encoder()
    {
        Assert.That(
            async () => await _sut.ReadEncodedAsync(TreeId, 0, -1L, 1, null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReadEncodedAsync_throws_on_zero_max_entries()
    {
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        Assert.That(
            async () => await _sut.ReadEncodedAsync(TreeId, 0, -1L, 0, encoder, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
