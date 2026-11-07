using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Storage.AzureTable.Tests.Fakes;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #5861: a single writer's pipelined phase-2 commits, claimed under the commit
/// fence's generation while an earlier commit is still in flight and committed after it,
/// both land. Azure returns the TAIL row's new ETag on that earlier commit, and the fence
/// conditions the next commit on it (issue #5810); only a commit whose landed ETag is
/// unknown may fence the claims made before it.
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderPipelinedCommitTests
{
    private const string TreeId = "tree-pipelined";
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
                TableName = "Tpipelined",
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
                Mutation = new LatticeMutation { Key = $"k{firstOffset + i}", Value = [(byte)(firstOffset + i), 0x5A] },
            })
            .ToList();

    [Test]
    public async Task A_commit_claimed_while_the_previous_one_is_in_flight_lands_after_it()
    {
        var (provider, table) = CreateProvider();
        await using var _ = provider;
        using var inFlight = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var held = 0;
        table.BeforeTransaction = actions =>
        {
            // Hold the first phase-2 commit - the first transaction that writes TAIL - in flight.
            if (actions.Any(a => ((AzureTableWalEntity)a.Entity).RowKey == AzureTableWalStorageProvider.TailRowKey)
                && Interlocked.Exchange(ref held, 1) == 0)
            {
                inFlight.Set();
                release.Wait(TimeSpan.FromSeconds(30));
            }
        };

        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        Assert.That(inFlight.Wait(TimeSpan.FromSeconds(30)), Is.True, "precondition: the first phase-2 commit is in flight");

        // Claimed now, under the generation the in-flight commit carries, and committed
        // after it in a transaction of its own.
        await provider.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
        release.Set();

        Assert.That(async () => await provider.FlushPhaseTwoAsync(CancellationToken.None), Throws.Nothing,
            "a pipelined commit claimed before the previous one landed was refused by the commit fence");
        Assert.That(await provider.GetHighestOffsetAsync(TreeId, ShardIndex, CancellationToken.None), Is.EqualTo(5L));
    }

    [Test]
    public async Task A_commit_conditioned_on_a_tail_another_writer_moved_is_refused()
    {
        // The other direction: with real ETags the fence still bites. Writer A knows TAIL's
        // ETag from its own commit; writer B then moves TAIL; A's next commit, conditioned
        // on the ETag B replaced, must not land over B's.
        var (a, table) = CreateProvider();
        await using var _ = a;
        await a.AppendBatchAsync(TreeId, ShardIndex, Entries(0, 3), CancellationToken.None);
        await a.FlushPhaseTwoAsync(CancellationToken.None);

        var b = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ServiceClient = table.BuildServiceClient(),
                TableName = "Tpipelined",
                Compression = LatticeCompression.None,
            }),
            _serializer,
            saturationSignal: null,
            compressors: [new ZstdLatticeCompressor(3)]);
        await using (b)
        {
            await b.AppendBatchAsync(TreeId, ShardIndex, Entries(3, 3), CancellationToken.None);
            await b.FlushPhaseTwoAsync(CancellationToken.None);
        }

        long TailOffset() => table.Snapshot().Single(r => r.RowKey == AzureTableWalStorageProvider.TailRowKey).Offset;
        var movedTail = TailOffset();

        Assert.That(async () =>
            {
                await a.AppendBatchAsync(TreeId, ShardIndex, Entries(6, 3), CancellationToken.None);
                await a.FlushPhaseTwoAsync(CancellationToken.None);
            },
            Throws.Exception,
            "a commit conditioned on a TAIL ETag another writer replaced was admitted");
        Assert.Multiple(() =>
        {
            Assert.That(movedTail, Is.EqualTo(5L), "precondition: the other writer moved TAIL");
            Assert.That(TailOffset(), Is.EqualTo(movedTail), "the stale writer's commit moved TAIL");
        });
    }
}
