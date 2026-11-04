using System.Globalization;
using Azure;
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
    private AzureTableWalStorageProvider CreateProvider() =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = false,
            }),
            _serializer);

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
