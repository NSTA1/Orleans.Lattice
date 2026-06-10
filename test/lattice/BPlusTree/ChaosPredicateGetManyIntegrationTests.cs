using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the typed predicate push-down overload
/// <see cref="TypedLatticeExtensions.GetManyAsync{T}(ILattice, List{string}, System.Linq.Expressions.Expression{System.Func{T, bool}}, CancellationToken)"/>
/// under sustained split churn and concurrent conflicting writes. The
/// predicate is lowered to a serializable IR and evaluated server-side on the
/// owning leaf, so this test proves the filter is <b>sound</b> (every returned
/// value satisfies the predicate under any interleaving) and <b>complete</b>
/// over a steady-state band whose values never change while keys migrate
/// between shards.
/// </summary>
/// <remarks>
/// Universe layout: 600 keys partitioned into two bands, each value a JSON
/// <c>ChaosScored</c> document carrying its index, a band tag, and a score.
/// <list type="bullet">
///   <item><description><c>pg-000000..pg-000199</c> - <b>stable band</b>: written once with <c>Score == Index</c> and never rewritten. A completeness worker issues <c>GetManyAsync(stableKeys, d =&gt; d.Score &lt; 100)</c> and asserts the result is exactly indices <c>[0, 100)</c> - the predicate selects the same deterministic set on every pass regardless of split churn moving the keys around.</description></item>
///   <item><description><c>pg-000200..pg-000599</c> - <b>churn band</b>: point writers continuously rewrite these keys with a fresh random score. A soundness worker queries a random subset with <c>d =&gt; d.Score &gt;= Threshold</c> and asserts every returned document actually satisfies the predicate - the server must never surface a value its IR rejects, no matter how writes and splits race the read.</description></item>
/// </list>
/// Post-window invariants:
/// <list type="bullet">
///   <item><description>No soundness violation: every value the predicate-GetMany returned satisfied the predicate.</description></item>
///   <item><description>No completeness violation: every stable-band predicate-GetMany returned exactly the deterministic matching set.</description></item>
///   <item><description>A final stable-band predicate-GetMany returns exactly indices <c>[0, 100)</c>.</description></item>
///   <item><description>Each worker (point writer, soundness, completeness, split coordinator) made progress.</description></item>
/// </list>
/// The churn-band live set is intentionally NOT pinned: scores race continuously, so which keys match is non-deterministic by construction - only soundness of what is returned is asserted there.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosPredicateGetManyIntegrationTests
{
    private sealed record ChaosScored(int Index, string Band, int Score);

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int StableStart = 0;
    private const int ChurnStart = 200;
    private const int UniverseEnd = 600;
    private const int StableMatchThreshold = 100;
    private const int ChurnThreshold = 150;
    private const int ScoreSpread = 300;
    private const int PointWriterCount = 3;
    private const int SoundnessWorkerCount = 2;
    private const int CompletenessWorkerCount = 1;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
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

    private static string KeyOf(int i) => $"pg-{i:D6}";

    private static int IndexOfKey(string key) =>
        key.StartsWith("pg-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan(3), out var idx)
            ? idx
            : -1;

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
    public async Task Chaos_predicate_getmany_under_split_churn_is_sound_and_complete()
    {
        var treeId = $"pg-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed the whole universe. Stable band: Score == Index (deterministic
        // match set). Churn band: an initial score that writers will churn.
        for (int i = StableStart; i < UniverseEnd; i++)
        {
            var band = i < ChurnStart ? "stable" : "churn";
            var score = i < ChurnStart ? i : i % ScoreSpread;
            await tree.SetAsync(KeyOf(i), new ChaosScored(i, band, score));
        }

        var stableKeys = Enumerable.Range(StableStart, ChurnStart - StableStart)
            .Select(KeyOf).ToList();
        var expectedStableMatch = Enumerable.Range(StableStart, ChurnStart - StableStart)
            .Where(i => i < StableMatchThreshold)
            .Select(KeyOf)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Warm cold-activation paths outside the chaos window.
        _ = await tree.CountPerShardAsync();
        _ = await tree.GetManyAsync<ChaosScored>(stableKeys, d => d.Score < StableMatchThreshold);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Point writers: continuously rewrite churn-band keys with a
        // fresh random score so the matching set churns under the readers.
        for (int w = 0; w < PointWriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 11);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(ChurnStart, UniverseEnd);
                        var score = rng.Next(0, ScoreSpread);
                        await tree.SetAsync(KeyOf(idx), new ChaosScored(idx, "churn", score));
                        Bump(stats, "point-writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-writes"); }
                    catch (Exception ex)
                    {
                        failures.Add($"writer{writerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }));
        }

        // ---- Soundness workers: query a random churn-band subset with a
        // value predicate and assert every returned document actually
        // satisfies it. This is the differential check that holds under any
        // write/split interleaving: the server must never surface a value its
        // IR rejects.
        for (int s = 0; s < SoundnessWorkerCount; s++)
        {
            var workerId = s;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(workerId * 104729 + 17);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var keys = new List<string>(32);
                        for (int k = 0; k < 32; k++)
                            keys.Add(KeyOf(rng.Next(ChurnStart, UniverseEnd)));

                        Bump(stats, "soundness-attempts");
                        var result = await tree.GetManyAsync<ChaosScored>(
                            keys, d => d.Score >= ChurnThreshold, ct);

                        foreach (var (key, doc) in result)
                        {
                            if (!keys.Contains(key))
                                failures.Add($"soundness{workerId}: returned unrequested key '{key}'");
                            if (doc.Score < ChurnThreshold)
                                failures.Add($"soundness{workerId}: key '{key}' returned score {doc.Score} < {ChurnThreshold}");
                        }
                        Bump(stats, "soundness");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-soundness"); }
                    catch (Exception ex)
                    {
                        failures.Add($"soundness{workerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }));
        }

        // ---- Completeness worker: the stable band never mutates, so a
        // predicate-GetMany over it must return exactly the deterministic
        // matching set on every pass even while splits migrate the keys
        // between shards.
        for (int c = 0; c < CompletenessWorkerCount; c++)
        {
            var workerId = c;
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        Bump(stats, "completeness-attempts");
                        var result = await tree.GetManyAsync<ChaosScored>(
                            stableKeys, d => d.Score < StableMatchThreshold, ct);

                        var got = result.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();
                        if (!got.SequenceEqual(expectedStableMatch, StringComparer.Ordinal))
                        {
                            var missing = expectedStableMatch.Except(got, StringComparer.Ordinal).Take(10);
                            var extra = got.Except(expectedStableMatch, StringComparer.Ordinal).Take(10);
                            failures.Add($"completeness{workerId}: stable match set drift. " +
                                $"missing=[{string.Join(",", missing)}] extra=[{string.Join(",", extra)}]");
                        }

                        foreach (var (key, doc) in result)
                        {
                            var idx = IndexOfKey(key);
                            if (doc.Score != idx)
                                failures.Add($"completeness{workerId}: stable key '{key}' score drifted to {doc.Score}");
                        }
                        Bump(stats, "completeness");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-completeness"); }
                    catch (Exception ex)
                    {
                        failures.Add($"completeness{workerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }));
        }

        // ---- Split coordinator: identical to the main chaos suite.
        workers.Add(Task.Run(async () =>
        {
            var rng = new Random(173);
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

        await Task.WhenAll(workers);

        // ---- Post-window invariants.
        await Task.Delay(100);

        var finalStable = await tree.GetManyAsync<ChaosScored>(
            stableKeys, d => d.Score < StableMatchThreshold);
        var finalStableKeys = finalStable.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(finalStableKeys, Is.EqualTo(expectedStableMatch),
                "Final stable-band predicate-GetMany did not return exactly the deterministic match set.");

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("soundness-attempts", 0), Is.GreaterThan(0),
                "Soundness workers must have issued at least one predicate-GetMany.");
            Assert.That(stats.GetValueOrDefault("completeness-attempts", 0), Is.GreaterThan(0),
                "Completeness worker must have issued at least one stable-band predicate-GetMany.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos predicate-GetMany workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }
}
