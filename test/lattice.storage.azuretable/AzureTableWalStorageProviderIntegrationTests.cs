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
    private Serializer<LatticeMutation> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private AzureTableWalStorageProvider _sut = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeMutation>>();
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
    public async Task AppendBatchAsync_supports_a_full_99_entry_transaction()
    {
        // The 100-action / 99-entry cap is the load-bearing batching
        // invariant. Exercise it end-to-end so a future SDK-side
        // tightening surfaces here rather than at runtime in CI.
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
}
