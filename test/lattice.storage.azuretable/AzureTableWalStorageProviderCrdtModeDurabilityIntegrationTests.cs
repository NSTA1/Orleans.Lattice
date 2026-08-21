using System.Buffers;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// End-to-end regression coverage for issue #926 against a real Azure
/// Table Storage endpoint (canonically
/// <see href="https://learn.microsoft.com/azure/storage/common/storage-use-azurite">Azurite</see>
/// on the default development connection string). The bug was that a
/// CRDT write's durable merge mode was dropped on the WAL, so a cold
/// replay folded it as a plain Last-Writer-Wins register and silently
/// emptied the key. These tests append a typed-CRDT (<see
/// cref="LatticeMergeMode.OrSet"/>) entry through both of the
/// provider's write seams - the materialised
/// <see cref="AzureTableWalStorageProvider.AppendBatchAsync"/> path and
/// the zero-copy
/// <see cref="AzureTableWalStorageProvider.AppendEncodedBatchAsync"/>
/// hot path - then read it back through a fresh provider over the same
/// table (a stand-in for a silo restart) and assert the mode survived.
/// <para>
/// Gated under the <c>AzureStorageEmulator</c> NUnit category so the
/// default dev loop (which excludes that category) skips them when no
/// emulator is running; <see cref="OneTimeSetUp"/> probes reachability
/// and falls through to <see cref="Assert.Inconclusive(string)"/> if the
/// probe fails.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalStorageProviderCrdtModeDurabilityIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-crdt-mode";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private AzureTableWalStorageProvider _sut = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
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
                + $"Start it via 'azurite --silent --location <dir>' or skip the AzureStorageEmulator category. "
                + $"Underlying error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
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
                // Read-your-writes durability requires synchronous
                // phase-2; see AzureTableWalStorageProviderIntegrationTests.
                PipelinePhaseTwoCommits = false,
            }),
            _serializer);

    private static async Task<List<WalEntry>> ReadAllAsync(
        AzureTableWalStorageProvider sut,
        int shard = 0)
    {
        var collected = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, shard, -1L, 1024, CancellationToken.None))
        {
            collected.Add(entry);
        }
        return collected;
    }

    [Test]
    public async Task AppendBatchAsync_round_trips_a_CRDT_merge_mode_across_a_fresh_provider()
    {
        // The materialised write seam: the legacy BuildEntryEntity path
        // used to hardcode LwwRegister here, dropping the authored mode.
        var entry = new WalEntry
        {
            Offset = 0L,
            Mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = "labels/orset",
                Value = new byte[] { 0x01, 0x02 },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
                Mode = LatticeMergeMode.OrSet,
            },
        };

        await _sut.AppendBatchAsync(TreeId, 0, new[] { entry }, CancellationToken.None);

        // Fresh provider over the same table stands in for a silo restart
        // so the read goes through the durable bytes, not in-memory state.
        var recovered = CreateProvider(_tableName);
        var read = await ReadAllAsync(recovered);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(
            read[0].Mutation.Mode,
            Is.EqualTo(LatticeMergeMode.OrSet),
            "The CRDT merge mode must survive the materialised AppendBatchAsync write seam.");
    }

    [Test]
    public async Task AppendEncodedBatchAsync_round_trips_a_CRDT_merge_mode_across_a_fresh_provider()
    {
        // The zero-copy hot path stores the encoder's bytes verbatim, so
        // the durable mode (wire id 26) must round-trip from those bytes
        // with no per-batch framing header on the storage replay path.
        var record = new WalRecord
        {
            TreeId = TreeId,
            Op = MutationKind.Set,
            Key = "labels/orset",
            Value = null,
            Delta = new byte[] { 0xAA, 0xBB, 0xCC },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.OrSet,
            ShardIndex = 0,
        };
        var writer = new ArrayBufferWriter<byte>();
        _encoder.Encode(in record, writer);
        var segment = new ArraySegment<byte>(writer.WrittenSpan.ToArray());

        await _sut.AppendEncodedBatchAsync(
            TreeId,
            0,
            new ReadOnlyMemory<ArraySegment<byte>>(new[] { segment }),
            new ReadOnlyMemory<long>(new[] { 0L }),
            _encoder,
            CancellationToken.None);

        var recovered = CreateProvider(_tableName);
        var read = await ReadAllAsync(recovered);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(
            read[0].Mutation.Mode,
            Is.EqualTo(LatticeMergeMode.OrSet),
            "The CRDT merge mode must survive the zero-copy AppendEncodedBatchAsync hot path.");
    }
}
