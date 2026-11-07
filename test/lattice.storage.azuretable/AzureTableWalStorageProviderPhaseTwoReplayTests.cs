using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Regression coverage for issue #5520: a phase-2 manifest transaction whose
/// first attempt committed but whose response was lost is resent by the Azure
/// Tables SDK retry pipeline, and the resend answers <c>409 EntityAlreadyExists</c>
/// on the manifest row the first attempt already wrote. That write is durable, so
/// the append must succeed; a manifest row that records a different batch at the
/// same start offset must still fail loudly and must never be overwritten.
/// Driven against the in-memory table, so it runs under the default filter.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderPhaseTwoReplayTests
{
    private const string TreeId = "tree-phase2-replay";
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
        var options = new AzureTableWalStorageOptions
        {
            ServiceClient = table.BuildServiceClient(),
            TableName = "Tphase2replay",
            Compression = LatticeCompression.None,
        };
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
                    Value = [(byte)(firstOffset + i), 0x22],
                },
            })
            .ToList();

    private static bool IsManifestCommit(IReadOnlyList<TableTransactionAction> actions) =>
        actions.Any(a => a.ActionType == TableTransactionActionType.Add
            && ((AzureTableWalEntity)a.Entity).RowKey.StartsWith(
                AzureTableWalStorageProvider.ManifestRowKeyPrefix, StringComparison.Ordinal));

    private static IReadOnlyList<AzureTableWalEntity> ManifestRows(InMemoryWalTable table) =>
        table.Partition(AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex))
            .Where(r => r.RowKey.StartsWith(AzureTableWalStorageProvider.ManifestRowKeyPrefix, StringComparison.Ordinal))
            .ToList();

    [Test]
    public async Task Append_succeeds_when_an_SDK_resend_of_a_committed_phase_two_transaction_answers_409()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        // Resend exactly the first manifest commit, as the SDK does after a
        // lost response: the resend collides with the manifest row it wrote.
        var resends = 0;
        table.ResendAfterCommit = actions =>
            IsManifestCommit(actions) && Interlocked.CompareExchange(ref resends, 1, 0) == 0;

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(resends, Is.EqualTo(1), "the first manifest commit was resent");
        var manifest = ManifestRows(table);
        Assert.That(manifest.Select(r => (r.RowKey, r.Offset)), Is.EqualTo(new[]
        {
            (AzureTableWalStorageProvider.BuildManifestRowKey(0), 2L),
        }));

        // The shard keeps appending past the replayed commit.
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.That(await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None), Is.EqualTo(4L));
        Assert.That(ManifestRows(table), Has.Count.EqualTo(2));
    }

    [Test]
    public async Task A_batch_claimed_while_an_earlier_commit_is_resent_commits_once_that_commit_is_proven_durable()
    {
        // #5605 W7: a proven replay is a commit that landed. The commit fence (#5810)
        // must treat it as one: a batch claimed while it was in flight, under the same
        // generation, is not a stale claim, and refusing it fails a healthy append with
        // a "must resync" error the caller cannot retry.
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        Task? second = null;
        var resends = 0;
        table.ResendAfterCommit = actions =>
        {
            if (!IsManifestCommit(actions) || Interlocked.CompareExchange(ref resends, 1, 0) != 0)
            {
                return false;
            }

            // The next batch is claimed while the first commit's response is lost.
            second = provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);
            return true;
        };

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(second, Is.Not.Null, "precondition: the second batch was claimed during the resend");
        Assert.That(resends, Is.EqualTo(1), "precondition: the first manifest commit was resent");

        Assert.That(async () =>
        {
            await second!;
            await provider.FlushPhaseTwoAsync(CancellationToken.None);
        }, Throws.Nothing, "a batch claimed during a commit that landed was refused by the commit fence");
        Assert.That(await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None), Is.EqualTo(4L));
        Assert.That(ManifestRows(table), Has.Count.EqualTo(2));
    }

    [Test]
    public async Task A_batch_claimed_during_a_resend_is_refused_when_a_successor_restamped_TAIL_at_the_same_value()
    {
        // a5e2e2ef's B1 on #5963: a successor's reconcile re-stamps TAIL at the value this
        // instance's lost-response commit wrote, precisely to fence this instance out. The
        // replay proof reads that same value; adopting the re-stamp's ETag would let this
        // instance's batches commit against the successor's fence. The commit nonce tells
        // the two rows apart, so the fence is invalidated and the batch is refused.
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        var manifestPartition = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        Task? second = null;
        var resends = 0;
        table.ResendAfterCommit = actions =>
        {
            if (!IsManifestCommit(actions) || Interlocked.CompareExchange(ref resends, 1, 0) != 0)
            {
                return false;
            }

            second = provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 2), CancellationToken.None);

            // The successor's re-stamp: the same value, rewritten without this commit's nonce.
            var written = table.Partition(manifestPartition).Single(r => r.RowKey == AzureTableWalStorageProvider.TailRowKey);
            table.Seed(new AzureTableWalEntity { PartitionKey = manifestPartition, RowKey = written.RowKey, Offset = written.Offset });
            return true;
        };

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await provider.FlushPhaseTwoAsync(CancellationToken.None);
        Assert.That(second, Is.Not.Null, "precondition: the second batch was claimed during the resend");

        Assert.That(async () =>
        {
            await second!;
            await provider.FlushPhaseTwoAsync(CancellationToken.None);
        }, Throws.InstanceOf<TimeoutException>(),
            "a batch claimed before a successor re-stamped TAIL committed against the successor's fence");
        Assert.That(ManifestRows(table), Has.Count.EqualTo(1), "the stale batch's manifest row landed");
    }

    [Test]
    public async Task Append_fails_loudly_and_overwrites_nothing_when_the_manifest_row_records_another_batch()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;

        // A manifest row at the same start offset that records a different
        // batch (end offset 7, not 2): a genuine conflict, never a replay.
        var manifestPartition = AzureTableWalStorageProvider.BuildManifestPartitionKey(TreeId, ShardIndex);
        table.Seed(new AzureTableWalEntity
        {
            PartitionKey = manifestPartition,
            RowKey = AzureTableWalStorageProvider.BuildManifestRowKey(0),
            Offset = 7L,
            PayloadBytes = 11L,
        });

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);

        Assert.That(
            async () => await provider.FlushPhaseTwoAsync(CancellationToken.None),
            Throws.InstanceOf<RequestFailedException>()
                .With.Property(nameof(RequestFailedException.Status)).EqualTo(409));

        var manifest = ManifestRows(table);
        Assert.That(manifest, Has.Count.EqualTo(1));
        Assert.That((manifest[0].Offset, manifest[0].PayloadBytes), Is.EqualTo((7L, 11L)), "the conflicting row is left exactly as it was");
    }
}

