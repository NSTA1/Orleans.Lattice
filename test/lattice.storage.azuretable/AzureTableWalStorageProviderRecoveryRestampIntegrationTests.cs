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
/// Azurite-backed tests for issue #5825: a WAL partition's predecessor writer, still
/// running after its successor recovered the partition, must not commit a batch below
/// the successor's <c>TAIL</c> once the successor may have let readers pass it as a
/// hole. The phase-2 worker commits whatever has arrived, so a predecessor's lower
/// batch whose entry write was slow can commit after a higher one; a successor that
/// only read <c>TAIL</c> leaves the predecessor's commit fence (issue #5810) current.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalStorageProviderRecoveryRestampIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-restamp";

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

    [Test]
    public async Task A_predecessors_late_lower_commit_is_refused_once_a_successor_has_recovered()
    {
        // Predecessor P holds [0] before its entry write and commits [1] past it, so
        // TAIL is 1 with no batch at 0. Successor S recovers - reconcile, then the
        // highest offset - and its readers pass offset 0 as a hole. P's [0] then lands.
        // Its commit must be refused: a record appearing at 0 now would be applied by a
        // replay from the start but never by a reader that already passed it.
        var gate = new TransactionGate();
        await using var predecessor = CreateProvider(gate);
        gate.HoldNextTransaction();
        var lower = predecessor.AppendBatchAsync(TreeId, 0, Entries(0, 1, value: 9), CancellationToken.None);
        await gate.Held.WaitAsync(TimeSpan.FromSeconds(30));

        long head;
        (long Offset, int Value)[] seenBySuccessor;
        try
        {
            await predecessor.AppendBatchAsync(TreeId, 0, Entries(1, 1, value: 1), CancellationToken.None);

            await using var successor = CreateProvider();
            await successor.ReconcileAsync(TreeId, 0, CancellationToken.None);
            head = await successor.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
            seenBySuccessor = await ReadValuesAsync(successor);
        }
        finally
        {
            gate.Release();
        }

        Exception? refused = null;
        try
        {
            await lower;
        }
        catch (Exception ex)
        {
            refused = ex;
        }

        await using var reader = CreateProvider();
        var values = await ReadValuesAsync(reader);
        var manifestRow = await _table.GetEntityIfExistsAsync<TableEntity>(
            AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0),
            AzureTableWalStorageProvider.BuildManifestRowKey(0L));
        Assert.Multiple(() =>
        {
            Assert.That(head, Is.EqualTo(1L));
            Assert.That(seenBySuccessor, Is.EqualTo(new[] { (1L, 1) }), "the successor's reader did not pass offset 0 as a hole");
            Assert.That(refused, Is.Not.Null, "the predecessor's late lower commit was accepted");
            Assert.That(manifestRow.HasValue, Is.False, "a batch committed below a recovered successor's readers");
            Assert.That(values, Is.EqualTo(new[] { (1L, 1) }), "a record appeared below a position readers had passed");
        });
    }

    [Test]
    public async Task A_recovery_changes_tails_etag_and_keeps_its_value()
    {
        // The refusal above works by the recovery rewriting TAIL at the same value, so
        // that any commit conditioned on the ETag a predecessor holds fails.
        await using var writer = CreateProvider();
        await writer.AppendBatchAsync(TreeId, 0, Entries(0, 2, value: 1), CancellationToken.None);
        var before = await ReadTailAsync();

        await using var successor = CreateProvider();
        await successor.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var after = await ReadTailAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after.Offset, Is.EqualTo(before.Offset), "recovery moved TAIL");
            Assert.That(after.ETag, Is.Not.EqualTo(before.ETag), "recovery left TAIL's ETag, so a predecessor's fence stays current");
        });
    }

    private async Task<(long Offset, ETag ETag)> ReadTailAsync()
    {
        var tail = (await _table.GetEntityAsync<AzureTableWalEntity>(
            AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, 0),
            AzureTableWalStorageProvider.TailRowKey)).Value;
        return (tail.Offset, tail.ETag);
    }

    private AzureTableWalStorageProvider CreateProvider(TransactionGate? gate = null) =>
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

    private static async Task<(long Offset, int Value)[]> ReadValuesAsync(AzureTableWalStorageProvider provider)
    {
        var values = new List<(long, int)>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            values.Add((entry.Offset, entry.Mutation.Value![0]));
        }

        return values.ToArray();
    }

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

    /// <summary>
    /// Holds, until released, the next table transaction after it is armed: with the
    /// candidate row eliminated, that is the next append's entry write. Every other
    /// request passes.
    /// </summary>
    private sealed class TransactionGate : HttpPipelinePolicy
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public Task Held => _held.Task;

        public void HoldNextTransaction() => Volatile.Write(ref _armed, 1);

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
            && Interlocked.Exchange(ref _armed, 0) == 1;
    }
}
