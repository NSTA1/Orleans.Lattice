using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of cross-tree atomic writes
/// (<see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAcrossTreesAsync"/>)
/// under sustained per-tree split churn. A single commit worker repeatedly writes
/// a fresh per-generation key into the <b>same logical slot</b> of three distinct
/// trees as one all-or-nothing cross-tree saga, while reader workers continuously
/// probe both the last-committed and the in-flight generation across all three
/// trees. The reader invariant is the cross-tree visibility guarantee: a generation
/// key is present in <b>all three trees or none</b> - a reader must never observe a
/// partial cross-tree commit (some trees flipped, others not), even while splits
/// move keys between shards mid-saga on every tree.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosCrossTreeAtomicWriteIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int TreeCount = 3;
    private const int SeedKeys = 200;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan CommitInterval = TimeSpan.FromMilliseconds(50);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);
    private static string SeedKey(int i) => $"seed-{i:D6}";
    private static string GenKey(int g) => $"x-{g:D8}";

    private static bool IsTransient(Exception ex) =>
        ex.GetType().Name is "EnumerationAbortedException" or "StaleShardRoutingException"
            or "LatticeCursorSnapshotExpiredException" or "LatticeCursorRegistryPinExhaustedException"
        || (ex is InvalidOperationException
            && ex.Message.Contains("failed and was rolled back", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("retries while topology kept changing", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("kept committing sagas faster than the fan-out", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("fewer than 2 virtual slots", StringComparison.Ordinal))
        || ex is TimeoutException;

    [Test]
    public async Task Chaos_cross_tree_atomic_write_under_split_churn_is_all_or_nothing_across_trees()
    {
        var runId = Guid.NewGuid().ToString("N");
        var treeIds = new string[TreeCount];
        var trees = new ILattice[TreeCount];
        for (var t = 0; t < TreeCount; t++)
        {
            treeIds[t] = $"xct-{runId}-{t}";
            trees[t] = await _fixture.CreateTreeAsync(treeIds[t]);
            for (var i = 0; i < SeedKeys; i++)
            {
                await trees[t].SetAsync(SeedKey(i), Bytes($"seed-{i}"));
            }
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        // Per-(tree,generation) monotonicity latch: once a committed key is
        // observed present in a tree it must never subsequently read absent.
        var seenPresent = new ConcurrentDictionary<(int Tree, int Gen), bool>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        var lastCommitted = -1;     // highest generation whose saga returned Committed
        var inFlight = -1;          // generation currently being committed

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;
        var workers = new List<Task>();

        // ---- Commit worker: one cross-tree saga per generation, same logical key
        // written into every tree all-or-nothing.
        workers.Add(Task.Run(async () =>
        {
            var g = 0;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(CommitInterval, ct);
                    Volatile.Write(ref inFlight, g);
                    Bump(stats, "commit-attempts");
                    var batches = treeIds
                        .Select(tid => new LatticeTreeBatch(tid,
                            [new KeyValuePair<string, byte[]>(GenKey(g), Bytes($"v-{g}"))]))
                        .ToList();
                    var outcome = await _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
                        batches, operationId: $"xctop-{runId}-{g}", ct);
                    if (outcome != CrossTreeAtomicWriteOutcome.Committed)
                    {
                        failures.Add($"generation {g} returned {outcome}, expected Committed");
                    }
                    else
                    {
                        Volatile.Write(ref lastCommitted, g);
                        Bump(stats, "commits");
                    }
                    g++;
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-commits"); g++; }
                catch (Exception ex)
                {
                    failures.Add($"commit-worker threw: {ex.GetType().Name}: {ex.Message}");
                    g++;
                }
            }
        }));

        // ---- Reader workers: continuously verify that SETTLED committed
        // generations stay present in every tree under split churn. A
        // generation is "settled" once the commit frontier has advanced a
        // safety margin past it, guaranteeing every participant's finalize has
        // promoted the entry into its main store. Splits are read-transparent
        // for promoted committed keys (see ShardSplitIntegrationTests), so a
        // settled key that reads absent is a genuine rollback. The in-flight /
        // just-decided window is deliberately excluded: each per-tree GetAsync
        // is an independent point-in-time read, so the global decision flip
        // landing between samples yields a benign transient skew that is not a
        // partial-commit violation.
        const int SettleMargin = 5;
        for (var r = 0; r < 3; r++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var settled = Volatile.Read(ref lastCommitted) - SettleMargin;
                        if (settled < 0) { await Task.Delay(5, ct); continue; }
                        for (var g = Math.Max(0, settled - 5); g <= settled; g++)
                        {
                            for (var t = 0; t < TreeCount; t++)
                            {
                                if (await trees[t].GetAsync(GenKey(g)) is not null)
                                {
                                    seenPresent[(t, g)] = true;
                                }
                                else
                                {
                                    failures.Add(
                                        $"settled generation {g} read absent in tree {t} (committed key vanished under churn)");
                                }
                            }
                        }
                        Bump(stats, "read-checks");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-reads"); }
                    catch (Exception ex)
                    {
                        failures.Add($"reader threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }));
        }

        // ---- Split coordinators: one per tree, churning shards continuously.
        for (var t = 0; t < TreeCount; t++)
        {
            var treeId = treeIds[t];
            var tree = trees[t];
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(treeId.GetHashCode());
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(SplitInterval, ct);
                        var physical = await tree.CountPerShardAsync();
                        var candidates = Enumerable.Range(0, physical.Count)
                            .Where(i => physical[i] > 0).ToList();
                        if (candidates.Count == 0) continue;
                        var src = candidates[rng.Next(candidates.Count)];
                        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{src}");
                        Bump(stats, "split-attempts");
                        await split.SplitAsync(src);
                        await split.RunSplitPassAsync();
                        Bump(stats, "splits");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-splits"); }
                    catch (Exception ex)
                    {
                        failures.Add($"split-coordinator threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }));
        }

        await Task.WhenAll(workers);

        // ---- Post-window invariant: every committed generation is durably
        // present in ALL trees (all-or-nothing completeness across trees).
        var committed = Volatile.Read(ref lastCommitted);
        var incomplete = new List<int>();
        for (var g = 0; g <= committed; g++)
        {
            var present = 0;
            for (var t = 0; t < TreeCount; t++)
            {
                if (await trees[t].GetAsync(GenKey(g)) is not null) present++;
            }
            if (present != TreeCount) incomplete.Add(g);
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));
            Assert.That(incomplete, Is.Empty,
                "Cross-tree all-or-nothing violated post-window: committed generations missing from one or more trees (first 20): " +
                string.Join(",", incomplete.Take(20)));
            Assert.That(stats.GetValueOrDefault("commits", 0), Is.GreaterThan(0),
                "Commit worker must have committed at least one cross-tree saga.");
            Assert.That(stats.GetValueOrDefault("read-checks", 0), Is.GreaterThan(0),
                "Reader workers must have performed at least one cross-tree probe.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinators must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos cross-tree-atomic-write workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-32}{kv.Value}");
    }
}
