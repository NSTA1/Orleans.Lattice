using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the conditional range-delete push-down overload
/// (<see cref="TypedLatticeExtensions.DeleteRangeAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string, string, CancellationToken)"/>)
/// under sustained split churn and concurrent conflicting writes. Unlike an
/// unconditional range delete, the conditional variant must tombstone
/// <b>only</b> the in-range keys whose value satisfies the predicate, evaluated
/// once at write time on the owning leaf and shipped as an explicit matched-key
/// set so replay / replication reproduce exactly that closure. This test proves
/// the conditional delete is <b>sound</b> (a non-matching key is never
/// tombstoned), <b>complete</b> (a matching key is tombstoned), and
/// <b>range-bounded</b> (keys outside <c>[200, 400)</c> are never touched), even
/// while splits move keys between shards mid-delete and point writers rewrite
/// values concurrently.
/// </summary>
/// <remarks>
/// Universe: 600 keys, each value a JSON <c>ChaosScored</c> document. The
/// predicate-delete worker issues <c>DeleteRangeAsync&lt;ChaosScored&gt;(d =&gt; d.Score &lt; DeleteThreshold, "pr-000200", "pr-000400")</c>.
/// <list type="bullet">
///   <item><description><c>pr-000000..pr-000199</c> - <b>lower protected band</b>: outside the range, never touched, must stay present and envelope-valid.</description></item>
///   <item><description><c>pr-000200..pr-000279</c> - <b>sticky-keep</b>: written once with <c>Score = KeepScore</c> (never matches the predicate) and never rewritten. <b>Soundness</b>: a conditional delete must never tombstone these.</description></item>
///   <item><description><c>pr-000280..pr-000359</c> - <b>sticky-delete</b>: written once with <c>Score = DeleteScore</c> (always matches) and never rewritten. <b>Completeness</b>: after a delete pass these must be gone and stay gone.</description></item>
///   <item><description><c>pr-000360..pr-000399</c> - <b>churn</b>: point writers continuously rewrite with random scores; races the delete worker, not asserted for liveness.</description></item>
///   <item><description><c>pr-000400..pr-000599</c> - <b>upper protected band</b>: outside the range, never touched, must stay present and envelope-valid.</description></item>
/// </list>
/// Post-window invariants (after a final deterministic delete pass to remove the
/// race against the last in-flight churn write): every sticky-keep key present
/// with <c>Score == KeepScore</c>; every sticky-delete key absent; both
/// protected bands fully present and envelope-valid; no scanner ever observed a
/// value whose <c>Index</c> disagrees with its key; every worker made progress.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosPredicateRangeDeleteIntegrationTests
{
    private sealed record ChaosScored(int Index, string Band, int Score);

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int LowerProtectedStart = 0;
    private const int DeleteBandStart = 200;
    private const int StickyKeepEnd = 280;
    private const int StickyDeleteEnd = 360;
    private const int DeleteBandEnd = 400;
    private const int UpperProtectedStart = 400;
    private const int UniverseEnd = 600;

    private const int DeleteThreshold = 500;
    private const int KeepScore = 1000;   // never < DeleteThreshold
    private const int DeleteScore = 0;    // always < DeleteThreshold
    private const int ChurnSpread = 1000;

    private const int PointWriterCount = 3;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan DeleteInterval = TimeSpan.FromMilliseconds(400);

    private static readonly string DeleteBandStartKey = KeyOf(DeleteBandStart);
    private static readonly string DeleteBandEndKey = KeyOf(DeleteBandEnd);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"pr-{i:D6}";

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
    public async Task Chaos_predicate_range_delete_under_split_churn_is_sound_complete_and_bounded()
    {
        var treeId = $"pr-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed the whole universe.
        for (int i = LowerProtectedStart; i < UniverseEnd; i++)
        {
            var (band, score) = ClassifySeed(i);
            await tree.SetAsync(KeyOf(i), new ChaosScored(i, band, score));
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Warm cold-activation paths outside the chaos window.
        _ = await tree.CountPerShardAsync();
        _ = await tree.GetAsync<ChaosScored>(KeyOf(0));

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Point writers: rewrite churn-band keys with fresh random scores
        // that straddle the threshold, racing the predicate-delete worker.
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
                        var idx = rng.Next(StickyDeleteEnd, DeleteBandEnd);
                        var score = rng.Next(0, ChurnSpread);
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
            }, ct));
        }

        // ---- Predicate-delete worker: periodically issues the conditional
        // range delete over the whole delete band.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(DeleteInterval, ct);
                    Bump(stats, "predicate-delete-attempts");
                    _ = await tree.DeleteRangeAsync<ChaosScored>(
                        d => d.Score < DeleteThreshold,
                        DeleteBandStartKey,
                        DeleteBandEndKey,
                        ct);
                    Bump(stats, "predicate-deletes");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-deletes"); }
                catch (Exception ex)
                {
                    failures.Add($"predicate-delete-worker threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Soundness scanner: walk the whole universe; every observed key's
        // value must carry an Index that matches its key (no cross-key bleed
        // under split churn), and any observed sticky-keep key must still carry
        // KeepScore - the conditional delete must never have rewritten or
        // tombstoned a non-matching key.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    Bump(stats, "scan-attempts");
                    await foreach (var kv in tree.ScanEntriesAsync<ChaosScored>().WithCancellation(ct))
                    {
                        var idx = kv.Value.Index;
                        if ($"pr-{idx:D6}" != kv.Key)
                            failures.Add($"scan: key '{kv.Key}' carried Index {idx}");
                        if (idx >= DeleteBandStart && idx < StickyKeepEnd && kv.Value.Score != KeepScore)
                            failures.Add($"scan: sticky-keep '{kv.Key}' surfaced score {kv.Value.Score} != {KeepScore}");
                    }
                    Bump(stats, "scans");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-scans"); }
                catch (Exception ex)
                {
                    failures.Add($"scan threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

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
        }, ct));

        await Task.WhenAll(workers);

        // ---- Final deterministic delete pass: removes the race against the
        // last in-flight churn write so completeness is deterministic.
        await Task.Delay(100);
        await RetryUntilAsync(() => tree.DeleteRangeAsync<ChaosScored>(
            d => d.Score < DeleteThreshold, DeleteBandStartKey, DeleteBandEndKey));

        // ---- Post-window invariants.
        var keepMissing = new List<int>();
        var keepWrongScore = new List<int>();
        for (int i = DeleteBandStart; i < StickyKeepEnd; i++)
        {
            var v = await tree.GetAsync<ChaosScored>(KeyOf(i));
            if (v is null) keepMissing.Add(i);
            else if (v.Score != KeepScore) keepWrongScore.Add(i);
        }

        var delPresent = new List<int>();
        for (int i = StickyKeepEnd; i < StickyDeleteEnd; i++)
        {
            if (await tree.GetAsync<ChaosScored>(KeyOf(i)) is not null) delPresent.Add(i);
        }

        var lowerMissing = new List<int>();
        for (int i = LowerProtectedStart; i < DeleteBandStart; i++)
            if (await tree.GetAsync<ChaosScored>(KeyOf(i)) is null) lowerMissing.Add(i);

        var upperMissing = new List<int>();
        for (int i = UpperProtectedStart; i < UniverseEnd; i++)
            if (await tree.GetAsync<ChaosScored>(KeyOf(i)) is null) upperMissing.Add(i);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(keepMissing, Is.Empty,
                "Soundness violated: conditional delete tombstoned non-matching sticky-keep keys (first 20): " +
                string.Join(",", keepMissing.Take(20)));
            Assert.That(keepWrongScore, Is.Empty,
                "sticky-keep keys mutated away from KeepScore (first 20): " +
                string.Join(",", keepWrongScore.Take(20)));

            Assert.That(delPresent, Is.Empty,
                "Completeness violated: matching sticky-delete keys survived the conditional delete (first 20): " +
                string.Join(",", delPresent.Take(20)));

            Assert.That(lowerMissing, Is.Empty,
                "Range bound violated: lower protected band leaked into the conditional delete (first 20): " +
                string.Join(",", lowerMissing.Take(20)));
            Assert.That(upperMissing, Is.Empty,
                "Range bound violated: upper protected band leaked into the conditional delete (first 20): " +
                string.Join(",", upperMissing.Take(20)));

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("predicate-delete-attempts", 0), Is.GreaterThan(0),
                "Predicate-delete worker must have attempted at least one delete.");
            Assert.That(stats.GetValueOrDefault("scan-attempts", 0), Is.GreaterThan(0),
                "Soundness scanner must have started at least one scan.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos predicate-range-delete workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-28}{kv.Value}");
    }

    private static (string Band, int Score) ClassifySeed(int i)
    {
        if (i < DeleteBandStart) return ("lower", KeepScore);
        if (i < StickyKeepEnd) return ("sticky-keep", KeepScore);
        if (i < StickyDeleteEnd) return ("sticky-delete", DeleteScore);
        if (i < DeleteBandEnd) return ("churn", DeleteScore);
        return ("upper", KeepScore);
    }

    private static async Task RetryUntilAsync(Func<Task> action)
    {
        for (int attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await Task.Delay(100);
            }
        }
        await action();
    }
}
