using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #6118: a phase-2 manifest transaction whose outcome is unknown to the
/// caller. A <c>500 OperationTimedOut</c> (or any other fault that leaves open
/// whether the service applied the transaction) used to fail the commit without
/// looking, and a transient fault on the replay proof's own reads failed a commit
/// that had landed. Either way the shard latched a sticky failure for a durable
/// write. Driven against the in-memory table, so it runs under the default filter.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderPhaseTwoUnknownOutcomeTests
{
    private const string TreeId = "tree-phase2-unknown";
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

    private (AzureTableWalStorageProvider Provider, InMemoryWalTable Table) CreateProvider()
    {
        var table = new InMemoryWalTable();
        var provider = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ServiceClient = table.BuildServiceClient(),
                TableName = "Tphase2unknown",
                Compression = LatticeCompression.None,
            }),
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
                    Value = [(byte)(firstOffset + i), 0x33],
                },
            })
            .ToList();

    private static bool IsManifestCommit(IReadOnlyList<TableTransactionAction> actions) =>
        actions.Any(a => a.ActionType == TableTransactionActionType.Add
            && ((AzureTableWalEntity)a.Entity).RowKey.StartsWith(
                AzureTableWalStorageProvider.ManifestRowKeyPrefix, StringComparison.Ordinal));

    private static RequestFailedException OperationTimedOut() =>
        new(500, "Operation could not be completed within the specified time.", "OperationTimedOut", innerException: null);

    [Test]
    public async Task A_phase_two_commit_that_landed_behind_a_500_OperationTimedOut_succeeds()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;
        var faulted = 0;
        table.FaultAfterCommit = actions =>
            IsManifestCommit(actions) && Interlocked.CompareExchange(ref faulted, 1, 0) == 0 ? OperationTimedOut() : null;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        Assert.That(async () => await provider.FlushPhaseTwoAsync(CancellationToken.None), Throws.Nothing,
            "a commit the service applied was failed because its response was a 500");
        Assert.That(faulted, Is.EqualTo(1), "PRECONDITION: the manifest commit landed and answered 500");

        // The shard keeps appending past the commit, under the same fence.
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None), Is.EqualTo(4L));
    }

    [Test]
    public async Task A_resent_commit_is_proven_when_a_proof_read_faults_transiently()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;
        var resends = 0;
        table.ResendAfterCommit = actions =>
            IsManifestCommit(actions) && Interlocked.CompareExchange(ref resends, 1, 0) == 0;
        var readFaults = 0;
        table.BeforeGet = (_, rowKey) =>
        {
            if (rowKey.StartsWith(AzureTableWalStorageProvider.ManifestRowKeyPrefix, StringComparison.Ordinal)
                && Volatile.Read(ref resends) == 1
                && Interlocked.CompareExchange(ref readFaults, 1, 0) == 0)
            {
                throw OperationTimedOut();
            }
        };

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        Assert.That(async () => await provider.FlushPhaseTwoAsync(CancellationToken.None), Throws.Nothing,
            "a transient fault on the proof's own read failed a commit that had landed");
        Assert.Multiple(() =>
        {
            Assert.That(resends, Is.EqualTo(1), "PRECONDITION: the manifest commit was resent and answered 409");
            Assert.That(readFaults, Is.EqualTo(1), "PRECONDITION: the proof's first read faulted");
        });
    }

    [Test]
    public async Task A_500_OperationTimedOut_for_a_commit_that_did_not_land_still_fails()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;
        table.BeforeTransaction = actions =>
        {
            if (IsManifestCommit(actions))
            {
                throw OperationTimedOut();
            }
        };

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        Assert.That(
            async () => await provider.FlushPhaseTwoAsync(CancellationToken.None),
            Throws.InstanceOf<RequestFailedException>().With.Property(nameof(RequestFailedException.Status)).EqualTo(500));
    }
}
