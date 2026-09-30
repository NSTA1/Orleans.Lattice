using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Behavioural coverage for the Azure Table WAL provider's three-phase append
/// pipeline - the candidate row (phase 0), the per-batch entry transaction
/// (phase 1), and the manifest commit plus TAIL upsert (phase 2) - together
/// with the overlap guard that fences a batch against re-writing a live
/// offset.
/// <para>
/// As with the read path, every pre-existing fixture covering these methods
/// end-to-end carried <c>[Category("AzureStorageEmulator")]</c> and so was
/// excluded from the default filter, leaving
/// <see cref="AzureTableWalStorageProvider.AppendBatchAsync"/>,
/// <see cref="AzureTableWalStorageProvider.AppendEncodedBatchAsync"/>, and the
/// overlap-claim path at zero coverage. These fixtures drive the same code
/// against an in-memory table and therefore carry no slow-suite category.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderAppendPipelineTests
{
    private const string TreeId = "tree-append";
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

    private (AzureTableWalStorageProvider Provider, InMemoryWalTable Table) CreateProvider(
        Action<AzureTableWalStorageOptions>? configure = null)
    {
        var table = new InMemoryWalTable();
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Tappend",
            Compression = LatticeCompression.None,
        };
        configure?.Invoke(options);
        var provider = new AzureTableWalStorageProvider(
            Options.Create(options),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)]);
        return (provider, table);
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
                    Value = [(byte)(firstOffset + i), 0x11],
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

    private static IReadOnlyList<AzureTableWalEntity> EntryRows(InMemoryWalTable table) =>
        table.Snapshot().Where(r => r.RowKey.StartsWith("E", StringComparison.Ordinal)).ToList();

    private static IReadOnlyList<AzureTableWalEntity> ManifestRows(InMemoryWalTable table) =>
        table.Snapshot().Where(r => r.RowKey.StartsWith("M", StringComparison.Ordinal)).ToList();

    private static IReadOnlyList<AzureTableWalEntity> CandidateRows(InMemoryWalTable table) =>
        table.Snapshot().Where(r => r.RowKey.StartsWith("C", StringComparison.Ordinal)).ToList();

    [Test]
    public async Task AppendBatchAsync_writes_one_entry_row_per_entry()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 5), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var rows = EntryRows(table);
        Assert.That(rows, Has.Count.EqualTo(5));
        Assert.That(rows.Select(r => r.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
        Assert.That(rows.Select(r => r.Payload), Has.All.Not.Null);
    }

    [Test]
    public async Task AppendBatchAsync_commits_a_manifest_row_and_moves_TAIL()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var manifest = ManifestRows(table);
        Assert.That(manifest, Has.Count.EqualTo(1));
        Assert.That(manifest[0].Offset, Is.EqualTo(2L), "the manifest row records endOffsetInclusive");

        var tail = table.Snapshot().Single(r => r.RowKey == "TAIL");
        Assert.That(tail.Offset, Is.EqualTo(2L));
    }

    [Test]
    public async Task AppendBatchAsync_deletes_the_candidate_row_once_phase_two_commits()
    {
        // The C-row is the crash-recovery signal: present means phase 2 has
        // not run. Its presence before the flush and absence afterwards is
        // what proves the phase-2 transaction deleted it atomically with the
        // manifest insert, rather than the row never having been written.
        var (provider, table) = CreateProvider(o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        Assert.That(CandidateRows(table), Is.Not.Empty, "phase 0 should stamp a candidate row");

        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(CandidateRows(table), Is.Empty, "phase 2 should delete the candidate row");
    }

    [Test]
    public async Task AppendBatchAsync_writes_no_candidate_row_when_the_hot_path_elides_it()
    {
        var (provider, table) = CreateProvider(o => o.EliminateCandidateRowOnHotPath = true);
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);

        Assert.That(CandidateRows(table), Is.Empty);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(ManifestRows(table), Has.Count.EqualTo(1), "the batch still commits without a C-row");
    }

    [Test]
    public async Task AppendBatchAsync_places_each_batch_in_its_own_partition()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var partitions = EntryRows(table).Select(r => r.PartitionKey).Distinct().ToList();
        Assert.That(partitions, Has.Count.EqualTo(2), "concurrent batches must not share a partition server");
    }

    [Test]
    public async Task AppendBatchAsync_stamps_the_batch_sentinel_on_the_first_row_only()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var rows = EntryRows(table).OrderBy(r => r.Offset).ToList();
        Assert.That(rows[0].BatchHash, Is.Not.Null, "the first row carries the idempotency sentinel");
        Assert.That(rows[0].BatchEntryCount, Is.EqualTo(4));
        Assert.That(rows.Skip(1).Select(r => r.BatchHash), Has.All.Null);
        Assert.That(rows.Skip(1).Select(r => r.BatchEntryCount), Has.All.Zero);
    }

    [Test]
    public async Task AppendBatchAsync_accounts_payload_bytes_on_the_manifest_row()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var storedBytes = EntryRows(table).Sum(r => (long)(r.Payload?.Length ?? 0));
        Assert.That(ManifestRows(table).Single().PayloadBytes, Is.EqualTo(storedBytes));
    }

    [Test]
    public async Task AppendBatchAsync_is_a_no_op_for_an_empty_batch()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, [], CancellationToken.None);

        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task AppendBatchAsync_rejects_a_non_dense_batch()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        var sparse = new List<WalEntry>
        {
            Entries(0, 1)[0],
            Entries(5, 1)[0],
        };

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, sparse, CancellationToken.None),
            Throws.ArgumentException);
        Assert.That(table.Count, Is.Zero, "a rejected batch must write nothing");
    }

    [Test]
    public async Task AppendBatchAsync_rejects_an_overlapping_offset_range()
    {
        // The overlap guard is the WAL's single-writer invariant. Pairing the
        // refusal with the accepting append above proves the guard
        // discriminates rather than refusing everything.
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var rowsBefore = table.Count;
        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 4), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("overlaps"));
        Assert.That(table.Count, Is.EqualTo(rowsBefore), "a refused append must leave the log untouched");
    }

    [Test]
    public async Task AppendBatchAsync_accepts_a_contiguous_range_after_a_refused_overlap()
    {
        var (provider, _) = CreateProvider();
        await using var _p = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(4, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L }));
    }

    [Test]
    public async Task AppendBatchAsync_treats_a_byte_identical_replay_as_success()
    {
        // Models the lost-response retry: the rows committed server-side but
        // the caller saw a transport failure, so the SDK resends and the
        // service answers 409. The provider must resolve that as success
        // rather than failing a durable batch.
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var fresh = CreateProviderOver(table);
        await using var _f = fresh;

        Assert.That(
            async () => await fresh.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None),
            Throws.Nothing);
    }

    [Test]
    public async Task AppendBatchAsync_surfaces_a_phase_one_transaction_failure()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        table.BeforeTransaction = actions =>
        {
            if (actions.Any(a => a.ActionType == TableTransactionActionType.Add))
            {
                throw new Azure.RequestFailedException(500, "InternalError", "InternalError", innerException: null);
            }
        };

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None),
            Throws.InstanceOf<Azure.RequestFailedException>());
    }

    [Test]
    public async Task AppendEncodedBatchAsync_stores_the_supplied_bytes_verbatim()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        var payloads = new[]
        {
            new byte[] { 1, 2, 3 },
            new byte[] { 4, 5 },
            new byte[] { 6, 7, 8, 9 },
        };
        var segments = payloads.Select(p => new ArraySegment<byte>(p)).ToArray();
        var offsets = new[] { 0L, 1L, 2L };

        await provider.AppendEncodedBatchAsync(
            TreeId, ShardIndex, segments, offsets, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var stored = EntryRows(table).OrderBy(r => r.Offset).Select(r => r.Payload!).ToList();
        Assert.That(stored, Has.Count.EqualTo(3));
        for (var i = 0; i < payloads.Length; i++)
        {
            Assert.That(stored[i], Is.EqualTo(payloads[i]));
        }
    }

    [Test]
    public async Task AppendEncodedBatchAsync_commits_its_manifest_row_and_TAIL()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        var segments = new[] { new ArraySegment<byte>([1, 2]), new ArraySegment<byte>([3, 4]) };

        await provider.AppendEncodedBatchAsync(
            TreeId, ShardIndex, segments, new[] { 0L, 1L }, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(ManifestRows(table).Single().Offset, Is.EqualTo(1L));
        Assert.That(table.Snapshot().Single(r => r.RowKey == "TAIL").Offset, Is.EqualTo(1L));
    }

    [Test]
    public async Task AppendEncodedBatchAsync_is_a_no_op_for_an_empty_batch()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendEncodedBatchAsync(
            TreeId,
            ShardIndex,
            ReadOnlyMemory<ArraySegment<byte>>.Empty,
            ReadOnlyMemory<long>.Empty,
            Substitute.For<IWalRecordEncoder>(),
            CancellationToken.None);

        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task AppendEncodedBatchAsync_rejects_mismatched_segment_and_offset_counts()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        Assert.That(
            async () => await provider.AppendEncodedBatchAsync(
                TreeId,
                ShardIndex,
                new[] { new ArraySegment<byte>([1]) },
                new[] { 0L, 1L },
                Substitute.For<IWalRecordEncoder>(),
                CancellationToken.None),
            Throws.ArgumentException);
        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task AppendEncodedBatchAsync_rejects_an_overlapping_range()
    {
        var (provider, _) = CreateProvider();
        await using var _p = provider;

        var segments = new[] { new ArraySegment<byte>([1]), new ArraySegment<byte>([2]) };
        await provider.AppendEncodedBatchAsync(
            TreeId, ShardIndex, segments, new[] { 0L, 1L }, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(
            async () => await provider.AppendEncodedBatchAsync(
                TreeId, ShardIndex, segments, new[] { 1L, 2L }, Substitute.For<IWalRecordEncoder>(), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task Append_paths_interleave_into_one_contiguous_log()
    {
        var (provider, _) = CreateProvider();
        await using var _p = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var page = await provider.ReadEncodedAsync(
            TreeId, ShardIndex, -1L, 10, Substitute.For<IWalRecordEncoder>(), CancellationToken.None);

        await provider.AppendEncodedBatchAsync(
            TreeId,
            ShardIndex,
            page.EncodedEntries,
            new[] { 2L, 3L },
            Substitute.For<IWalRecordEncoder>(),
            CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(provider.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));

        // The re-appended entries are the first two re-encoded, so their
        // mutations must round-trip identically.
        Assert.That(read[2].Mutation.Key, Is.EqualTo("k0"));
        Assert.That(read[3].Mutation.Key, Is.EqualTo("k1"));
    }

    [Test]
    public async Task GetHighestOffsetAsync_folds_the_workers_accepted_range_over_the_persisted_tail()
    {
        // The worker folds its accepted ranges over the persisted TAIL, so the
        // answer is the same whether or not phase 2 has flushed. Asserting
        // both sides of the flush proves the fold is additive rather than
        // replacing TAIL.
        var (provider, _) = CreateProvider();
        await using var _p = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 4), CancellationToken.None);

        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(3L));

        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(
            await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None),
            Is.EqualTo(3L));
    }

    [Test]
    public async Task Append_paths_reject_a_null_treeId()
    {
        var (provider, _) = CreateProvider();
        await using var _p = provider;

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await provider.AppendBatchAsync(null!, 0, Entries(0, 1), CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await provider.AppendEncodedBatchAsync(
                    null!,
                    0,
                    new[] { new ArraySegment<byte>([1]) },
                    new[] { 0L },
                    Substitute.For<IWalRecordEncoder>(),
                    CancellationToken.None),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    [Test]
    public async Task AppendBatchAsync_throws_after_the_provider_is_disposed()
    {
        var (provider, _) = CreateProvider();
        await provider.DisposeAsync();

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 1), CancellationToken.None),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public async Task AppendBatchAsync_rejects_a_batch_above_the_transaction_cap()
    {
        // Azure Tables caps a transaction at 100 actions, so a larger batch
        // must be refused before any I/O rather than truncated.
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 101), CancellationToken.None),
            Throws.ArgumentException.With.Message.Contains("exceeds the per-call limit"));
        Assert.That(table.Count, Is.Zero, "the batch must be refused before it writes anything");
    }

    [Test]
    public async Task AppendBatchAsync_accepts_a_batch_at_exactly_the_transaction_cap()
    {
        // The accepting counterpart to the refusal above: the boundary value
        // itself must succeed, so the guard is an upper bound rather than an
        // off-by-one that rejects a legal batch.
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 100), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(EntryRows(table), Has.Count.EqualTo(100));
    }

    [Test]
    public async Task AppendEncodedBatchAsync_rejects_a_batch_above_the_transaction_cap()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        var segments = Enumerable.Range(0, 101).Select(i => new ArraySegment<byte>([(byte)i])).ToArray();
        var offsets = Enumerable.Range(0, 101).Select(i => (long)i).ToArray();

        Assert.That(
            async () => await provider.AppendEncodedBatchAsync(
                TreeId, ShardIndex, segments, offsets, Substitute.For<IWalRecordEncoder>(), CancellationToken.None),
            Throws.ArgumentException);
        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task AppendBatchAsync_rejects_a_negative_starting_offset()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        var negative = new List<WalEntry>
        {
            new()
            {
                Offset = -1L,
                Mutation = new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = "k", Value = [1] },
            },
        };

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, negative, CancellationToken.None),
            Throws.ArgumentException);
        Assert.That(table.Count, Is.Zero);
    }

    [Test]
    public async Task AppendBatchAsync_surfaces_a_candidate_row_failure()
    {
        // Phase 0 and phase 1 run in parallel and are joined before phase 2 is
        // dispatched, so a phase-0 failure must surface to the caller and must
        // not leave a batch queued for commit.
        var (provider, table) = CreateProvider(o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = provider;

        table.BeforeUpsert = entity =>
        {
            if (entity.RowKey.StartsWith("C", StringComparison.Ordinal))
            {
                throw new Azure.RequestFailedException(503, "ServerBusy", "ServerBusy", innerException: null);
            }
        };

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None),
            Throws.InstanceOf<Azure.RequestFailedException>().With.Property("Status").EqualTo(503));

        table.BeforeUpsert = null;
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(ManifestRows(table), Is.Empty, "a failed phase 0 must not commit a manifest row");
    }

    [Test]
    public async Task AppendBatchAsync_surfaces_the_candidate_failure_when_phase_one_also_fails()
    {
        // When both halves fault the candidate-row fault is the one surfaced,
        // and the phase-1 fault must still be observed rather than left
        // unobserved on the task.
        var (provider, table) = CreateProvider(o => o.EliminateCandidateRowOnHotPath = false);
        await using var _ = provider;

        table.BeforeUpsert = entity =>
        {
            if (entity.RowKey.StartsWith("C", StringComparison.Ordinal))
            {
                throw new Azure.RequestFailedException(503, "ServerBusy", "ServerBusy", innerException: null);
            }
        };
        table.BeforeTransaction = actions =>
        {
            if (actions.Any(a => a.ActionType == TableTransactionActionType.Add))
            {
                throw new Azure.RequestFailedException(500, "InternalError", "InternalError", innerException: null);
            }
        };

        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None),
            Throws.InstanceOf<Azure.RequestFailedException>().With.Property("Status").EqualTo(503),
            "the candidate-row fault is the one surfaced");
    }

    [Test]
    public async Task AppendBatchAsync_on_a_fresh_provider_detects_an_overlap_written_by_a_previous_instance()
    {
        // A restarted provider's overlap guard starts unbounded, so its first
        // append must read a written upper bound from the table - including
        // uncommitted batch partitions sitting above TAIL - before it can
        // decide. This is the path a silo takes after a crash.
        var table = new InMemoryWalTable();
        var first = CreateProviderOver(table);
        await first.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await first.FlushPhaseTwoAsync(CancellationToken.None);
        await first.DisposeAsync();

        var restarted = CreateProviderOver(table);
        await using var _ = restarted;

        Assert.That(
            async () => await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(1, 3), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("overlaps"));
    }

    [Test]
    public async Task AppendBatchAsync_on_a_fresh_provider_accepts_a_range_above_the_written_bound()
    {
        // The accepting counterpart: the same cold-start bound read must let a
        // genuinely new range through, so the guard is not simply refusing
        // every append after a restart.
        var table = new InMemoryWalTable();
        var first = CreateProviderOver(table);
        await first.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await first.FlushPhaseTwoAsync(CancellationToken.None);
        await first.DisposeAsync();

        var restarted = CreateProviderOver(table);
        await using var _ = restarted;

        await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await restarted.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(restarted.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
    }

    [Test]
    public async Task AppendBatchAsync_releases_its_claim_when_the_overlap_probe_fails()
    {
        // The claim must be released on a failed probe, otherwise the shard's
        // guard would hold a phantom range forever and every later append at
        // those offsets would be refused. Asserting a subsequent append
        // succeeds is what proves the release happened.
        var table = new InMemoryWalTable();
        var first = CreateProviderOver(table);
        await first.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await first.FlushPhaseTwoAsync(CancellationToken.None);
        await first.DisposeAsync();

        var restarted = CreateProviderOver(table);
        await using var _ = restarted;

        Assert.That(
            async () => await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(2, 2), CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        await restarted.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await restarted.FlushPhaseTwoAsync(CancellationToken.None);

        var read = await DrainAsync(restarted.ReadAsync(TreeId, ShardIndex, -1L, 100, CancellationToken.None));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
    }

    private AzureTableWalStorageProvider CreateProviderOver(InMemoryWalTable table)
    {
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Tappend",
            Compression = LatticeCompression.None,
        };
        return new AzureTableWalStorageProvider(
            Options.Create(options),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)]);
    }
}
