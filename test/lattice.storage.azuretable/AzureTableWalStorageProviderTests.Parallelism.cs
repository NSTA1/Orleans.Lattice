using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box structural pins for the cross-shard / cross-tree
/// parallelism characteristic of
/// <see cref="AzureTableWalStorageProvider"/>. The provider achieves
/// maximum write parallelism by giving every <c>(treeId,
/// shardIndex)</c> pair its own <c>PhaseTwoWorker</c> - independent
/// channel, independent drain loop, independent manifest partition -
/// so there is no shared lock or shared queue between distinct
/// shards or distinct trees. These tests pin that invariant
/// structurally (one worker instance per logical shard, distinct
/// pairs get distinct instances) so a refactor that accidentally
/// introduces a shared bottleneck fails locally without needing an
/// Azurite-backed wall-clock test.
/// <para>
/// The tests never call any I/O method on the provider; they only
/// exercise <see cref="AzureTableWalStorageProvider.GetOrCreatePhaseTwoWorker"/>,
/// which is pure in-memory dictionary work. No Azurite endpoint is
/// required.
/// </para>
/// </summary>
[TestFixture]
public class AzureTableWalStorageProviderParallelismTests
{
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

    private AzureTableWalStorageProvider CreateProvider() =>
        new(
            Options.Create(new AzureTableWalStorageOptions
            {
                // Connection string is never used by these tests; the
                // provider only resolves a TableClient on first I/O,
                // and these tests exercise only the in-memory worker
                // dictionary. The literal must still parse as a valid
                // connection-string shape.
                ConnectionString = "UseDevelopmentStorage=true",
                TableName = "Tparallelism" + Guid.NewGuid().ToString("N"),
                Compression = LatticeCompression.None,
            }),
            _serializer);

    [Test]
    public async Task GetOrCreatePhaseTwoWorker_returns_the_same_instance_for_repeated_calls_with_the_same_shard()
    {
        // Idempotence: the worker is the per-shard serialisation
        // point for manifest commits, so repeated lookups for the
        // same (treeId, shardIndex) must always return the same
        // instance. A regression here (e.g. a TryAdd race that
        // discards the cached winner) would silently spin up
        // multiple workers and break strict offset ordering.
        await using var sut = CreateProvider();

        var a = sut.GetOrCreatePhaseTwoWorker("tree", 0);
        var b = sut.GetOrCreatePhaseTwoWorker("tree", 0);

        Assert.That(b, Is.SameAs(a),
            "repeated GetOrCreatePhaseTwoWorker calls for the same (treeId, shardIndex) must return the same instance");
    }

    [Test]
    public async Task GetOrCreatePhaseTwoWorker_returns_distinct_instances_for_distinct_shards_in_the_same_tree()
    {
        // Cross-shard parallelism: distinct shards in the same tree
        // must each get their own worker so concurrent appends do
        // not serialise behind a shared queue. The dictionary key
        // includes the shard index via the manifest partition key
        // (`_m_|tree|shard`), so distinct shard indices land on
        // distinct entries.
        await using var sut = CreateProvider();

        var workers = Enumerable.Range(0, 16)
            .Select(shard => sut.GetOrCreatePhaseTwoWorker("tree", shard))
            .ToArray();

        Assert.That(
            workers.Distinct().Count(),
            Is.EqualTo(workers.Length),
            "every shard in the same tree must own a distinct PhaseTwoWorker instance; "
            + "a single shared worker would serialise cross-shard appends behind one channel");
    }

    [Test]
    public async Task GetOrCreatePhaseTwoWorker_returns_distinct_instances_for_distinct_trees_at_the_same_shard()
    {
        // Cross-tree parallelism: distinct tree ids land in distinct
        // manifest partition keys, so the worker dictionary keys
        // are distinct and each tree gets its own worker. This is
        // the easier direction to accidentally break - a future
        // refactor that keys workers on `shardIndex` alone (ignoring
        // the tree) would silently funnel every tree's commits
        // through the same drain loop.
        await using var sut = CreateProvider();

        var workers = Enumerable.Range(0, 8)
            .Select(i => sut.GetOrCreatePhaseTwoWorker($"tree-{i}", shardIndex: 0))
            .ToArray();

        Assert.That(
            workers.Distinct().Count(),
            Is.EqualTo(workers.Length),
            "every tree at the same shard index must own a distinct PhaseTwoWorker instance; "
            + "a shard-only key would funnel every tree's commits through one worker");
    }

    [Test]
    public async Task GetOrCreatePhaseTwoWorker_grows_dictionary_one_entry_per_distinct_shard()
    {
        // Defends the steady-state size invariant: the worker
        // dictionary holds exactly one entry per active
        // (treeId, shardIndex). Repeated calls for the same shard
        // must not grow the dictionary, and distinct shards must
        // each contribute exactly one entry.
        await using var sut = CreateProvider();

        // Warm up four distinct shards with repeated calls each.
        for (var repeat = 0; repeat < 3; repeat++)
        {
            sut.GetOrCreatePhaseTwoWorker("tree-a", 0);
            sut.GetOrCreatePhaseTwoWorker("tree-a", 1);
            sut.GetOrCreatePhaseTwoWorker("tree-b", 0);
            sut.GetOrCreatePhaseTwoWorker("tree-b", 1);
        }

        Assert.That(sut._phaseTwoWorkers.Count, Is.EqualTo(4),
            "the worker dictionary holds exactly one entry per (treeId, shardIndex); "
            + "repeated calls for the same pair must not create duplicate entries");
    }

    [Test]
    public async Task GetOrCreatePhaseTwoWorker_concurrent_lookups_for_the_same_shard_return_one_winner()
    {
        // Race condition: many threads simultaneously calling
        // GetOrCreatePhaseTwoWorker for the same (treeId,
        // shardIndex) must converge on a single instance. The
        // method uses ConcurrentDictionary.TryAdd to elect a winner
        // and disposes the loser; if the race were lost, two
        // workers would coexist for one shard and break strict
        // offset ordering.
        //
        // The participants run on dedicated threads, not Task.Run: 64
        // thread-pool work items all blocked in SignalAndWait deadlock
        // a cold pool smaller than 64 threads (issue #4034). Dedicated
        // threads keep the genuine 64-way simultaneous release, and
        // every wait is bounded so a regression fails fast instead of
        // hanging the run.
        await using var sut = CreateProvider();

        const int concurrency = 64;
        var budget = TimeSpan.FromSeconds(30);
        var barrier = new Barrier(concurrency);
        var observed = new PhaseTwoWorker?[concurrency];
        var failures = new ConcurrentQueue<Exception>();

        var threads = new Thread[concurrency];
        for (var i = 0; i < concurrency; i++)
        {
            var index = i;
            threads[i] = new Thread(() =>
            {
                try
                {
                    if (barrier.SignalAndWait(budget))
                    {
                        observed[index] = sut.GetOrCreatePhaseTwoWorker("racing-tree", 0);
                    }
                }
                catch (Exception ex)
                {
                    failures.Enqueue(ex);
                }
            })
            {
                IsBackground = true,
                Name = $"phase-two-race-{index}",
            };
        }

        foreach (var thread in threads)
        {
            thread.Start();
        }

        var deadline = Environment.TickCount64 + (long)budget.TotalMilliseconds;
        var unfinished = threads.Count(thread =>
        {
            var remaining = TimeSpan.FromMilliseconds(deadline - Environment.TickCount64);
            return !thread.Join(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
        });

        Assert.That(unfinished, Is.Zero,
            $"every racing thread must finish within {budget.TotalSeconds}s; a hang here is a harness or election defect, not a pass");

        // Disposed only once every participant has left the barrier:
        // disposing a Barrier with threads still inside it is undefined.
        barrier.Dispose();

        Assert.That(failures, Is.Empty,
            "no racing thread may throw from the barrier or from GetOrCreatePhaseTwoWorker");
        Assert.That(observed, Has.All.Not.Null,
            "every participant must pass the barrier and observe a worker; a null means SignalAndWait timed out");
        Assert.That(observed.Distinct().Count(), Is.EqualTo(1),
            "concurrent GetOrCreatePhaseTwoWorker calls for the same shard must converge on one winner");
        Assert.That(sut._phaseTwoWorkers.Count, Is.EqualTo(1),
            "the losing throwaway workers must not leak into the dictionary");
    }
}
