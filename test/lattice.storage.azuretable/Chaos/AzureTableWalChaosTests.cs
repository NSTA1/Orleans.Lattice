using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Storage.AzureTable.Tests.Chaos;

/// <summary>
/// Chaos coverage of <see cref="AzureTableWalStorageProvider"/> under
/// sustained concurrent write load across multiple shards against a real
/// Azure Table Storage endpoint (canonically Azurite). The provider's
/// contract is single-writer per <c>(tree, shard)</c>: each shard is
/// driven by exactly one writer task that allocates monotonically-dense
/// offsets, but the test fans many such writers across distinct shards
/// in parallel so the provider's internal phase-two pipeline is exercised
/// at scale. After the chaos window closes, each shard's stored entries
/// must satisfy the per-shard density / no-gap / no-duplicate invariants
/// the WAL relies on for materialiser recovery.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this test pins.</b> The Azure Table WAL provider's
/// <see cref="AzureTableWalStorageProvider.PhaseTwoWorker"/> coalesces
/// per-shard batches into bounded Azure Table transactions; the chaos
/// workload runs many shards in parallel so the pipeline is genuinely
/// under load, and validates post-window that every shard's dense
/// offset namespace is intact and that
/// <see cref="AzureTableWalStorageProvider.GetHighestOffsetAsync(string, int, System.Threading.CancellationToken)"/>
/// converges to the expected highest offset on every shard.
/// </para>
/// <para>
/// <b>Why this is the chaos shape and not a fault-injection shape.</b>
/// Azurite does not expose a programmatic throttle / fault-injection
/// surface, and constructing a fault-injecting HTTP message handler
/// against the Azure SDK's pipeline is materially more invasive than
/// the test's value justifies on this branch. The sustained-write-across-
/// shards shape exercises the same critical path (per-shard append
/// serialisation, per-batch retry policy, dense offset allocation) under
/// realistic concurrent load and any regression that broke the per-shard
/// pipeline invariants would surface here as a gap / duplicate /
/// missing-entry failure on the post-window read.
/// </para>
/// <para>
/// Gated under the <c>AzureStorageEmulator</c> category and falls through
/// to <see cref="Assert.Inconclusive(string)"/> when Azurite is not
/// reachable, matching every other test under
/// <c>test/lattice.storage.azuretable</c>.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
[Category("AzureStorageEmulator")]
public class AzureTableWalChaosTests
{
    private const string AzuriteConnectionString = "UseDevelopmentStorage=true";
    private const string TreeId = "tree-chaos";
    private const int ShardCount = 6;
    private const int BatchesPerShard = 10;
    private const int EntriesPerBatch = 4;

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private TableServiceClient _adminClient = null!;
    private string _tableName = null!;
    private AzureTableWalStorageProvider _sut = null!;

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
        _sut = new AzureTableWalStorageProvider(
            Options.Create(new AzureTableWalStorageOptions
            {
                ConnectionString = AzuriteConnectionString,
                TableName = _tableName,
                Compression = LatticeCompression.None,
            }),
            _serializer);
    }

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await _adminClient.DeleteTableAsync(_tableName);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    [Test]
    public async Task Sustained_concurrent_appends_across_shards_preserve_per_shard_dense_offset_invariant()
    {
        var failures = new ConcurrentBag<string>();

        // One writer task per shard - matches the provider's
        // single-writer-per-(tree, shard) contract. All shards run
        // concurrently so the internal phase-two pipeline is genuinely
        // under load.
        var workers = new Task[ShardCount];
        for (var s = 0; s < ShardCount; s++)
        {
            var shardIdx = s;
            workers[shardIdx] = Task.Run(async () =>
            {
                try
                {
                    long nextOffset = 0;
                    for (var b = 0; b < BatchesPerShard; b++)
                    {
                        var batch = new WalEntry[EntriesPerBatch];
                        for (var i = 0; i < EntriesPerBatch; i++)
                        {
                            batch[i] = new WalEntry
                            {
                                Offset = nextOffset++,
                                Mutation = new LatticeMutation
                                {
                                    TreeId = TreeId,
                                    Kind = MutationKind.Set,
                                    Key = $"k-{shardIdx:D2}-{b:D2}-{i}",
                                    Value = System.Text.Encoding.UTF8.GetBytes($"v-{shardIdx}-{b}-{i}"),
                                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                                    OriginClusterId = "site-a",
                                },
                            };
                        }
                        await _sut.AppendBatchAsync(TreeId, shardIdx, batch, CancellationToken.None);
                    }
                }
                catch (Exception ex)
                {
                    failures.Add($"shard{shardIdx} threw: {ex.GetType().Name}: {ex.Message}");
                }
            });
        }

        await Task.WhenAll(workers);

        Assert.That(failures, Is.Empty,
            "Per-shard writer exceptions: " + string.Join("\n  ", failures));

        var expectedPerShard = BatchesPerShard * EntriesPerBatch;

        // Post-window read invariants, per shard.
        Assert.Multiple(() =>
        {
            for (var s = 0; s < ShardCount; s++)
            {
                var shardIdx = s;
                var allRead = new List<WalEntry>();
                var enumerator = _sut.ReadAsync(TreeId, shardIdx, fromOffsetExclusive: -1L, maxEntries: int.MaxValue, CancellationToken.None);
                var collectTask = Task.Run(async () =>
                {
                    await foreach (var entry in enumerator)
                    {
                        allRead.Add(entry);
                    }
                });
                collectTask.GetAwaiter().GetResult();

                var observedOffsets = allRead.Select(e => e.Offset).ToList();
                var distinct = new HashSet<long>(observedOffsets);

                Assert.That(observedOffsets.Count, Is.EqualTo(expectedPerShard),
                    $"Shard {shardIdx}: read returned {observedOffsets.Count} entries; expected {expectedPerShard}.");
                Assert.That(distinct.Count, Is.EqualTo(expectedPerShard),
                    $"Shard {shardIdx}: read returned {observedOffsets.Count - distinct.Count} duplicate offsets.");

                var gaps = new List<long>();
                for (long i = 0; i < expectedPerShard; i++)
                {
                    if (!distinct.Contains(i)) gaps.Add(i);
                }
                Assert.That(gaps, Is.Empty,
                    $"Shard {shardIdx}: read had gaps in the dense offset namespace at: " +
                    string.Join(",", gaps.Take(10)));

                for (var i = 1; i < observedOffsets.Count; i++)
                {
                    if (observedOffsets[i] <= observedOffsets[i - 1])
                    {
                        Assert.Fail(
                            $"Shard {shardIdx}: read returned non-monotonic offsets at index {i}: " +
                            $"{observedOffsets[i - 1]} >= {observedOffsets[i]}");
                    }
                }

                var highest = _sut.GetHighestOffsetAsync(TreeId, shardIdx, CancellationToken.None).GetAwaiter().GetResult();
                Assert.That(highest, Is.EqualTo((long)(expectedPerShard - 1)),
                    $"Shard {shardIdx}: GetHighestOffsetAsync reported {highest}; expected {expectedPerShard - 1}.");
            }
        });

        TestContext.Out.WriteLine($"Azure Table WAL chaos: shards={ShardCount}, batches/shard={BatchesPerShard}, " +
            $"entries/batch={EntriesPerBatch}, total entries={ShardCount * expectedPerShard}");
    }
}
