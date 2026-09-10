using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Azurite-backed regression coverage for the pipelined phase-2
/// visibility contract and the
/// <see cref="AzureTableWalStorageProvider.FlushPhaseTwoAsync"/>
/// barrier that closes it (issue #2509).
/// <para>
/// <b>What went wrong.</b> With the shipping default
/// <see cref="AzureTableWalStorageOptions.PipelinePhaseTwoCommits"/>
/// = <see langword="true"/>, <c>AppendBatchAsync</c> returns once the
/// batch's phase 0+1 rows are durable and the <i>previous</i> batch's
/// phase-2 commit has landed. The returning batch's own manifest row
/// and <c>TAIL</c> upsert are still in flight, and both
/// <c>ReadAsync</c> and <c>GetHighestOffsetAsync</c> are derived from
/// those manifest rows - so the trailing batch on a shard is durable
/// but not yet <i>visible</i>. A test that appended and then read
/// immediately therefore asserted read-after-write that the default
/// does not promise, and failed roughly one run in twenty with
/// "read returned 36 entries; expected 40" - exactly one un-landed
/// four-entry batch.
/// </para>
/// <para>
/// <b>Why this reproduces deterministically where the chaos test did
/// not.</b> The failure is a race between the read and the per-shard
/// phase-2 worker, so repeat runs on a fast machine mostly lose it.
/// These tests force the worst case instead of hunting for it: a
/// deliberately long <see cref="AzureTableWalStorageOptions.PhaseTwoCoalescingWindow"/>
/// makes the worker wait a bounded interval after the first arrival
/// before committing, so an un-flushed post-append read is
/// guaranteed to observe the pre-append manifest. The assertions are
/// then exact equalities in both directions - invisible before the
/// flush, fully visible after it - with no sleep, no polling, and no
/// dependence on machine speed.
/// </para>
/// <para>
/// Gated under the <c>AzureStorageEmulator</c> category exactly like
/// the rest of the integration suite; the dev loop skips them.
/// </para>
/// </summary>
[TestFixture]
[Category("AzureStorageEmulator")]
public class AzureTableWalStorageProviderPipelinedVisibilityIntegrationTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-pipeline-visibility";
    private const int EntriesPerBatch = 4;

    /// <summary>
    /// Coalescing window used to make the phase-2 lag deterministic.
    /// The worker delays this long after the first arrival before it
    /// commits, so a post-append read issued without a flush cannot
    /// race ahead of the assertion by finishing late. It is a
    /// generous upper bound on a local Azurite round-trip, not a
    /// sleep the tests wait out: every test that needs the commit
    /// awaits <see cref="AzureTableWalStorageProvider.FlushPhaseTwoAsync"/>
    /// rather than the window.
    /// </summary>
    private static readonly TimeSpan LongCoalescingWindow = TimeSpan.FromSeconds(3);

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;

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
    public void SetUp() => _tableName = "T" + Guid.NewGuid().ToString("N");

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch (RequestFailedException)
        {
            // Best-effort cleanup.
        }
    }

    private AzureTableWalStorageProvider CreateProvider(
        bool pipeline = true,
        TimeSpan? coalescingWindow = null) =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
                PipelinePhaseTwoCommits = pipeline,
                PhaseTwoCoalescingWindow = coalescingWindow ?? LongCoalescingWindow,
            }),
            _serializer);

    private static WalEntry[] Batch(int shardIndex, long firstOffset)
    {
        var batch = new WalEntry[EntriesPerBatch];
        for (var i = 0; i < EntriesPerBatch; i++)
        {
            batch[i] = new WalEntry
            {
                Offset = firstOffset + i,
                Mutation = new LatticeMutation
                {
                    TreeId = TreeId,
                    Kind = MutationKind.Set,
                    Key = $"k-{shardIndex:D2}-{firstOffset + i:D4}",
                    Value = System.Text.Encoding.UTF8.GetBytes($"v-{shardIndex}-{firstOffset + i}"),
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    OriginClusterId = "site-a",
                },
            };
        }
        return batch;
    }

    private static async Task<List<long>> ReadOffsetsAsync(
        AzureTableWalStorageProvider provider,
        int shardIndex)
    {
        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync(
            TreeId, shardIndex, fromOffsetExclusive: -1L, maxEntries: int.MaxValue, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }
        return offsets;
    }

    [Test]
    public async Task Pipelined_append_returns_before_its_own_batch_is_readable()
    {
        // Characterisation of the documented lag, and the exact
        // mechanism behind the flaky chaos failure. The batch is
        // durable when the append returns (phase 0+1 committed), but
        // its manifest row has not been written yet, so neither the
        // manifest scan behind ReadAsync nor the TAIL point-read
        // behind GetHighestOffsetAsync can see it.
        await using var sut = CreateProvider();

        await sut.AppendBatchAsync(TreeId, 0, Batch(0, 0L), CancellationToken.None);

        var offsets = await ReadOffsetsAsync(sut, 0);
        var highest = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.Empty,
                "the trailing batch has no manifest row until its phase-2 commit lands, so a manifest scan must not see it");
            Assert.That(highest, Is.EqualTo(-1L),
                "TAIL is upserted by the phase-2 commit, so it must still read as the empty-log sentinel");
        });

        // Leave the shard quiesced so teardown does not race the
        // worker's still-pending commit.
        await sut.FlushPhaseTwoAsync(CancellationToken.None);
    }

    [Test]
    public async Task FlushPhaseTwoAsync_makes_the_trailing_pipelined_batch_readable()
    {
        // The regression assertion for issue #2509, reproducing the
        // chaos failure's exact symptom - "read returned fewer
        // entries than expected", short by precisely one batch - and
        // then closing it with the barrier.
        //
        // The first batch is flushed so the shard's worker is back at
        // rest before the second batch is appended. The second batch
        // is therefore a fresh arrival that must sit out the whole
        // coalescing window, rather than being folded into a commit
        // that is already in flight, which is what makes the trailing
        // batch's invisibility deterministic rather than racy.
        await using var sut = CreateProvider();

        await sut.AppendBatchAsync(TreeId, 0, Batch(0, 0L), CancellationToken.None);
        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        await sut.AppendBatchAsync(TreeId, 0, Batch(0, EntriesPerBatch), CancellationToken.None);

        var beforeBarrier = await ReadOffsetsAsync(sut, 0);
        Assert.That(beforeBarrier, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
            "the trailing batch is durable but has no manifest row yet, so an unbarriered read is short by exactly one batch");

        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        var offsets = await ReadOffsetsAsync(sut, 0);
        var highest = await sut.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L }),
                "every appended entry must be readable in dense ascending order once the flush barrier returns");
            Assert.That(highest, Is.EqualTo((long)(2 * EntriesPerBatch) - 1),
                "TAIL must have converged to the highest appended offset once the flush barrier returns");
        });
    }

    [Test]
    public async Task FlushPhaseTwoAsync_drains_every_shard_the_provider_has_appended_to()
    {
        // The chaos workload fans one writer per shard, so the
        // barrier has to cover every per-shard slot, not just the
        // one the calling thread last touched.
        const int shardCount = 4;
        await using var sut = CreateProvider();

        for (var shard = 0; shard < shardCount; shard++)
        {
            await sut.AppendBatchAsync(TreeId, shard, Batch(shard, 0L), CancellationToken.None);
        }

        await sut.FlushPhaseTwoAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            for (var shard = 0; shard < shardCount; shard++)
            {
                var offsets = ReadOffsetsAsync(sut, shard).GetAwaiter().GetResult();
                var highest = sut.GetHighestOffsetAsync(TreeId, shard, CancellationToken.None)
                    .GetAwaiter().GetResult();

                Assert.That(offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
                    $"shard {shard}: every appended entry must be readable once the flush barrier returns");
                Assert.That(highest, Is.EqualTo((long)EntriesPerBatch - 1),
                    $"shard {shard}: TAIL must have converged once the flush barrier returns");
            }
        });
    }

    [Test]
    public async Task FlushPhaseTwoAsync_is_a_no_op_when_phase_two_commits_are_synchronous()
    {
        // With pipelining off every append already awaited its own
        // phase-2 commit, so no slot is ever occupied: the barrier
        // must be free rather than merely harmless, and the data must
        // already be visible before it is called.
        await using var sut = CreateProvider(pipeline: false, coalescingWindow: TimeSpan.Zero);

        await sut.AppendBatchAsync(TreeId, 0, Batch(0, 0L), CancellationToken.None);

        var beforeFlush = await ReadOffsetsAsync(sut, 0);
        await sut.FlushPhaseTwoAsync(CancellationToken.None);
        var afterFlush = await ReadOffsetsAsync(sut, 0);

        Assert.Multiple(() =>
        {
            Assert.That(beforeFlush, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
                "synchronous phase-2 commits promise read-after-write without any barrier");
            Assert.That(afterFlush, Is.EqualTo(beforeFlush),
                "the barrier must not perturb an already-visible shard");
        });
    }
}
