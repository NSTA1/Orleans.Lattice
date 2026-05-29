using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the public <see cref="ILattice.SetIfVersionAsync(string, byte[], HybridLogicalClock, CancellationToken)"/>
/// surface under sustained split churn. Stale routing + CAS retry is a
/// non-trivial composition: a stale-routing exception from the routing
/// layer must surface as a transient failure the caller's CAS loop can
/// retry from a fresh <c>GetWithVersionAsync</c>; the CAS result itself
/// (<c>true</c> applied vs <c>false</c> version-mismatch) must remain
/// the sole source of truth for whether the write took effect.
/// </summary>
/// <remarks>
/// Workload: <c>WriterCount</c> writers each repeatedly increment one
/// of a small key universe via the read-then-CAS dance:
/// <code>
/// while (true) {
///     var v = await tree.GetWithVersionAsync(key);
///     var next = ParseCounter(v.Value) + 1;
///     if (await tree.SetIfVersionAsync(key, EncodeCounter(next), v.Version)) {
///         localSuccessCount[writerId, key]++;
///         break;
///     }
///     // CAS lost; refresh and retry.
/// }
/// </code>
/// Split coordinator churns topology in parallel. Invariants:
/// <list type="bullet">
///   <item><description>Per key, the stored counter at post-window time equals the sum of <c>localSuccessCount</c> across every writer. A "lost update" would make the stored counter strictly less than the sum.</description></item>
///   <item><description>Every successful CAS for a given key carries a strictly newer <see cref="HybridLogicalClock"/> than the version that was observed by the caller (validated by the runtime - if it ever stamped an HLC less than the expected version, the CAS would have returned <c>false</c>).</description></item>
///   <item><description>Zero caller-visible exceptions outside the documented transient class.</description></item>
/// </list>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CompareAndSwapChaosTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int KeyUniverseSize = 8;
    private const int WriterCount = 4;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"cas-{i:D3}";

    private static byte[] EncodeCounter(long n) => Encoding.UTF8.GetBytes(n.ToString());

    private static long ParseCounter(byte[]? v)
    {
        if (v is null || v.Length == 0) return 0;
        return long.Parse(Encoding.UTF8.GetString(v));
    }

    private static bool IsTransient(Exception ex) =>
        ex.GetType().Name is "EnumerationAbortedException" or "StaleShardRoutingException"
        || (ex is InvalidOperationException
            && ex.Message.Contains("failed and was rolled back", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("retries while topology kept changing", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("kept committing sagas faster than the fan-out", StringComparison.Ordinal))
        || ex is TimeoutException;

    [Test]
    public async Task Chaos_concurrent_compare_and_swap_under_split_churn_preserves_counter_invariant()
    {
        var treeId = $"cas-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed every counter to 0.
        for (int i = 0; i < KeyUniverseSize; i++)
        {
            await tree.SetAsync(KeyOf(i), EncodeCounter(0));
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, long>();
        static void Bump(ConcurrentDictionary<string, long> s, string k, long delta = 1) =>
            s.AddOrUpdate(k, delta, (_, v) => v + delta);

        // Per-key per-writer success counters. Indexed [keyIdx, writerId].
        var perKeySuccess = new long[KeyUniverseSize, WriterCount];

        // Warm cold-activation paths.
        _ = await tree.CountPerShardAsync();

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- CAS writers.
        for (int w = 0; w < WriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 17);
                while (!ct.IsCancellationRequested)
                {
                    var idx = rng.Next(KeyUniverseSize);
                    var key = KeyOf(idx);
                    bool committed = false;
                    int casAttempts = 0;
                    while (!committed && !ct.IsCancellationRequested)
                    {
                        try
                        {
                            var vv = await tree.GetWithVersionAsync(key);
                            var current = ParseCounter(vv.Value);
                            var next = EncodeCounter(current + 1);
                            casAttempts++;
                            var ok = await tree.SetIfVersionAsync(key, next, vv.Version);
                            if (ok)
                            {
                                Interlocked.Increment(ref perKeySuccess[idx, writerId]);
                                Bump(stats, "cas-commits");
                                committed = true;
                            }
                            else
                            {
                                Bump(stats, "cas-version-mismatches");
                            }
                        }
                        catch (OperationCanceledException) { break; }
                        catch (Exception ex) when (IsTransient(ex))
                        {
                            Bump(stats, "transient-cas");
                        }
                        catch (Exception ex)
                        {
                            failures.Add($"writer{writerId}/{key} threw: {ex.GetType().Name}: {ex.Message}");
                            break;
                        }
                    }
                    Bump(stats, "cas-attempts", casAttempts);
                }
            }, ct));
        }

        // ---- Split coordinator.
        workers.Add(Task.Run(async () =>
        {
            var rng = new Random(421);
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
        }, ct));

        await Task.WhenAll(workers);

        // ---- Post-window invariant: for every key, the stored counter
        // value must equal the sum of all writers' local success counts
        // for that key. A lost update would make the stored value
        // strictly less than the sum.
        var counterMismatches = new List<string>();
        for (int idx = 0; idx < KeyUniverseSize; idx++)
        {
            var expected = 0L;
            for (int w = 0; w < WriterCount; w++) expected += perKeySuccess[idx, w];

            var observed = ParseCounter(await tree.GetAsync(KeyOf(idx)));
            if (observed != expected)
            {
                counterMismatches.Add(
                    $"key={KeyOf(idx)} stored={observed} expected={expected} " +
                    $"(per-writer: {string.Join(",", Enumerable.Range(0, WriterCount).Select(w => perKeySuccess[idx, w]))})");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} non-transient exceptions (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(counterMismatches, Is.Empty,
                "Lost-update detected on at least one key (stored counter < sum of writer successes):\n " +
                string.Join("\n ", counterMismatches));

            Assert.That(stats.GetValueOrDefault("cas-commits", 0L), Is.GreaterThan(0L),
                "At least one CAS must have committed.");
            Assert.That(stats.GetValueOrDefault("cas-version-mismatches", 0L), Is.GreaterThan(0L),
                "At least one CAS must have lost the race (otherwise the contention level is unrealistically low).");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0L), Is.GreaterThan(0L),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos CAS workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }
}
