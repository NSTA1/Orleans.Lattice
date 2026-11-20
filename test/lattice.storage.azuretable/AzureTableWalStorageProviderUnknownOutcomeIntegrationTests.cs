using System.Globalization;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #6116 end to end against Azurite: a phase-1 transaction whose fault
/// surfaces while the request itself still reaches the service late. The surfaced
/// fault carries an unknown outcome whose settle signal resolves on the real late
/// landing, a batch allocated above the window is accepted, and the stray rows the
/// late landing left are reclaimed by the next reconciliation.
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public sealed class AzureTableWalStorageProviderUnknownOutcomeIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-unknown-outcome";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private StrayTransaction _stray = null!;
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
        _stray = new StrayTransaction();
        _sut = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = false,
                PhaseOneTransientRetryMaxAttempts = 0,
                RetryMaxAttempts = 0,
                ConfigureClientOptions = options => options.AddPolicy(_stray, HttpPipelinePosition.BeforeTransport),
            }),
            _serializer)
        {
            PhaseOneUnknownOutcomeFirstProbeDelay = TimeSpan.FromMilliseconds(20),
        };
        await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        _table = new TableClient(AzuriteConnectionString, _tableName);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _sut.DisposeAsync();
        _stray.Dispose();
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
        }
    }

    [Test]
    public async Task A_phase_one_attempt_that_lands_after_its_fault_is_fenced_and_then_reclaimed()
    {
        await _sut.AppendBatchAsync(TreeId, 0, Entries(0, 2), CancellationToken.None);

        _stray.ArmNextTransaction();
        Exception? fault = null;
        try
        {
            await _sut.AppendBatchAsync(TreeId, 0, Entries(2, 2), CancellationToken.None);
        }
        catch (Exception ex)
        {
            fault = ex;
        }

        var unknown = WalAppendOutcomeUnknown.Find(fault);
        Assert.That(unknown, Is.Not.Null, "the faulted attempt's request may still reach the service");
        Assert.That(unknown!.Settled.IsCompleted, Is.False, "PRECONDITION: the window is fenced while the batch is absent");

        // The shard's resync reconciles and reads the tail before the stray lands.
        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);
        var highest = await _sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        await _stray.LandAsync();
        await unknown.Settled.WaitAsync(TimeSpan.FromSeconds(30));

        // Re-seeded above the window, as the shard now does, the next batch is accepted.
        await _sut.AppendBatchAsync(TreeId, 0, Entries(4, 2, value: 3), CancellationToken.None);
        var strayRowsBeforeReconcile = await CountBatchPartitionRowsAsync(2);
        await _sut.ReconcileAsync(TreeId, 0, CancellationToken.None);

        var stored = await ReadValuesAsync(_sut);
        var strayRowsAfterReconcile = await CountBatchPartitionRowsAsync(2);
        Assert.Multiple(() =>
        {
            Assert.That(highest, Is.EqualTo(1L), "PRECONDITION: the tail read missed the late landing");
            Assert.That(strayRowsBeforeReconcile, Is.EqualTo(2), "PRECONDITION: the stray transaction landed late");
            Assert.That(stored, Is.EqualTo(new[] { (0L, 1), (1L, 1), (4L, 3), (5L, 3) }),
                "every acknowledged append reads back exactly once");
            Assert.That(strayRowsAfterReconcile, Is.Zero,
                "the stray rows below TAIL with no manifest row are reclaimed");
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

    private static async Task<(long Offset, int Value)[]> ReadValuesAsync(AzureTableWalStorageProvider provider)
    {
        var values = new List<(long, int)>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            values.Add((entry.Offset, entry.Mutation.Value![0]));
        }

        return values.ToArray();
    }

    private static WalEntry[] Entries(long startOffset, int count, byte value = 1) =>
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
    /// Captures the next signed table transaction instead of sending it and fails
    /// the attempt with a transport fault, then lets the test send the captured
    /// request later: an attempt the client gave up on that the service executes late.
    /// </summary>
    private sealed class StrayTransaction : HttpPipelinePolicy, IDisposable
    {
        private readonly HttpClient _http = new();
        private int _armed;
        private HttpRequestMessage? _captured;

        public void ArmNextTransaction() => Volatile.Write(ref _armed, 1);

        public async Task LandAsync()
        {
            using var response = await _http.SendAsync(_captured!);
            Assert.That((int)response.StatusCode, Is.EqualTo(202), "PRECONDITION: the late transaction was accepted");
        }

        public void Dispose()
        {
            _captured?.Dispose();
            _http.Dispose();
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            => ProcessAsync(message, pipeline).AsTask().GetAwaiter().GetResult();

        public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            if (message.Request.Uri.Path.EndsWith("$batch", StringComparison.Ordinal)
                && Interlocked.Exchange(ref _armed, 0) == 1)
            {
                using var body = new MemoryStream();
                await message.Request.Content!.WriteToAsync(body, message.CancellationToken);
                var captured = new HttpRequestMessage(HttpMethod.Post, message.Request.Uri.ToUri())
                {
                    Content = new ByteArrayContent(body.ToArray()),
                };
                foreach (var header in message.Request.Headers)
                {
                    if (header.Name.Equals("Content-Length", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (header.Name.StartsWith("Content-", StringComparison.OrdinalIgnoreCase))
                    {
                        captured.Content.Headers.Remove(header.Name);
                        captured.Content.Headers.TryAddWithoutValidation(header.Name, header.Value);
                    }
                    else
                    {
                        captured.Headers.TryAddWithoutValidation(header.Name, header.Value);
                    }
                }

                _captured = captured;
                throw new IOException("The connection was reset after the transaction was sent.");
            }

            await ProcessNextAsync(message, pipeline);
        }
    }
}
