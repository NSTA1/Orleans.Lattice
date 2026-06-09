using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the conditional bulk write
/// (<see cref="TypedLatticeExtensions.SetManyAsync{T}(ILattice, System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, T}}, System.Linq.Expressions.Expression{System.Func{T, bool}}, CancellationToken)"/>)
/// under sustained split churn and concurrent conflicting point writes. A
/// conditional-write worker repeatedly attempts to stamp a marker score onto a
/// band of keys, guarded by <c>Score &gt;= Guard</c>; the guard is evaluated
/// server-side against each key's <b>current</b> value at write time. This
/// proves the conditional write is <b>sound</b> (a key whose current value does
/// not satisfy the guard is never written - it never receives the marker, and
/// never appears in the returned written set), <b>complete</b> (a key that
/// satisfies the guard receives the marker), and <b>range-bounded</b> (only the
/// keys actually submitted are ever considered), even while splits move keys
/// between shards mid-write and point writers rewrite values concurrently.
/// </summary>
/// <remarks>
/// The marker score (<see cref="MarkerScore"/>) is itself <c>&gt;= Guard</c>, so
/// the conditional write is idempotent on the sticky-match band: once stamped a
/// key keeps matching and keeps the marker. Universe / band layout mirrors
/// <see cref="ChaosPredicateRangeDeleteIntegrationTests"/>.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosPredicateConditionalSetManyIntegrationTests
{
    private sealed record ChaosDoc(int Index, string Band, int Score);

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int LowerProtectedStart = 0;
    private const int WriteBandStart = 200;
    private const int StickyMatchEnd = 280;
    private const int StickyNoMatchEnd = 360;
    private const int WriteBandEnd = 400;
    private const int UpperProtectedStart = 400;
    private const int UniverseEnd = 600;

    private const int Guard = 500;
    private const int MarkerScore = 7777;  // >= Guard => idempotent once stamped
    private const int MatchSeedScore = 1000; // >= Guard => sticky-match
    private const int NoMatchSeedScore = 0;  // <  Guard => sticky-no-match
    private const int ProtectedScore = 1000;

    private const int PointWriterCount = 3;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan WriteInterval = TimeSpan.FromMilliseconds(350);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"cs-{i:D6}";

    private static bool IsTransient(Exception ex) =>
        ex.GetType().Name is "EnumerationAbortedException" or "StaleShardRoutingException"
            or "LatticeCursorSnapshotExpiredException" or "LatticeCursorRegistryPinExhaustedException"
        || (ex is InvalidOperationException
            && ex.Message.Contains("failed and was rolled back", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("retries while topology kept changing", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("kept committing sagas faster than the fan-out", StringComparison.Ordinal))
        || ex is TimeoutException;

    private static List<KeyValuePair<string, ChaosDoc>> BuildBandWrite()
    {
        var entries = new List<KeyValuePair<string, ChaosDoc>>(WriteBandEnd - WriteBandStart);
        for (int i = WriteBandStart; i < WriteBandEnd; i++)
            entries.Add(new(KeyOf(i), new ChaosDoc(i, "marked", MarkerScore)));
        return entries;
    }

    [Test]
    public async Task Chaos_conditional_set_many_under_split_churn_is_sound_complete_and_bounded()
    {
        var treeId = $"cs-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        for (int i = LowerProtectedStart; i < UniverseEnd; i++)
        {
            var (band, score) = ClassifySeed(i);
            await tree.SetAsync(KeyOf(i), new ChaosDoc(i, band, score));
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Keys that must never be written by the conditional writer: the
        // sticky-no-match band (guard always false) plus everything outside the
        // submitted range. Used to validate both the stored state and the
        // returned written set.
        static bool MustNeverBeWritten(int idx) =>
            idx < WriteBandStart || idx >= WriteBandEnd
            || (idx >= StickyMatchEnd && idx < StickyNoMatchEnd);

        _ = await tree.CountPerShardAsync();
        _ = await tree.GetAsync<ChaosDoc>(KeyOf(0));

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;
        var workers = new List<Task>();

        // ---- Point writers: rewrite churn-band keys with fresh random scores.
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
                        var idx = rng.Next(StickyNoMatchEnd, WriteBandEnd);
                        var score = rng.Next(0, 1000);
                        await tree.SetAsync(KeyOf(idx), new ChaosDoc(idx, "churn", score));
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

        // ---- Conditional-write worker: stamps the marker onto guard-matching
        // band keys and validates the returned written set on the spot.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(WriteInterval, ct);
                    Bump(stats, "conditional-write-attempts");
                    var written = await tree.SetManyAsync<ChaosDoc>(BuildBandWrite(), d => d.Score >= Guard);
                    Bump(stats, "conditional-writes");
                    foreach (var key in written)
                    {
                        var idx = int.Parse(key.AsSpan(3));
                        if (MustNeverBeWritten(idx))
                            failures.Add($"written-set: guarded-out / out-of-range key '{key}' was reported written");
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-conditional-writes"); }
                catch (Exception ex)
                {
                    failures.Add($"conditional-write-worker threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Soundness scanner: no guarded-out key may ever surface the marker.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    Bump(stats, "scan-attempts");
                    await foreach (var kv in tree.ScanEntriesAsync<ChaosDoc>().WithCancellation(ct))
                    {
                        var idx = kv.Value.Index;
                        if ($"cs-{idx:D6}" != kv.Key)
                            failures.Add($"scan: key '{kv.Key}' carried Index {idx}");
                        if (MustNeverBeWritten(idx) && kv.Value.Score == MarkerScore)
                            failures.Add($"scan: guarded-out key '{kv.Key}' surfaced the marker score");
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

        // ---- Split coordinator.
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

        // ---- Final deterministic conditional-write pass for completeness.
        await Task.Delay(100);
        await RetryUntilAsync(() => tree.SetManyAsync<ChaosDoc>(BuildBandWrite(), d => d.Score >= Guard));

        // ---- Post-window invariants.
        var matchMissing = new List<int>();      // completeness
        for (int i = WriteBandStart; i < StickyMatchEnd; i++)
        {
            var v = await tree.GetAsync<ChaosDoc>(KeyOf(i));
            if (v is null || v.Score != MarkerScore) matchMissing.Add(i);
        }

        var noMatchWritten = new List<int>();    // soundness
        for (int i = StickyMatchEnd; i < StickyNoMatchEnd; i++)
        {
            var v = await tree.GetAsync<ChaosDoc>(KeyOf(i));
            if (v is null || v.Score != NoMatchSeedScore) noMatchWritten.Add(i);
        }

        var lowerLeaked = new List<int>();       // range bound
        for (int i = LowerProtectedStart; i < WriteBandStart; i++)
        {
            var v = await tree.GetAsync<ChaosDoc>(KeyOf(i));
            if (v is null || v.Score != ProtectedScore) lowerLeaked.Add(i);
        }

        var upperLeaked = new List<int>();       // range bound
        for (int i = UpperProtectedStart; i < UniverseEnd; i++)
        {
            var v = await tree.GetAsync<ChaosDoc>(KeyOf(i));
            if (v is null || v.Score != ProtectedScore) upperLeaked.Add(i);
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(matchMissing, Is.Empty,
                "Completeness violated: guard-matching keys did not receive the marker (first 20): " +
                string.Join(",", matchMissing.Take(20)));

            Assert.That(noMatchWritten, Is.Empty,
                "Soundness violated: guarded-out sticky-no-match keys were written (first 20): " +
                string.Join(",", noMatchWritten.Take(20)));

            Assert.That(lowerLeaked, Is.Empty,
                "Range bound violated: lower protected band was written by the conditional write (first 20): " +
                string.Join(",", lowerLeaked.Take(20)));
            Assert.That(upperLeaked, Is.Empty,
                "Range bound violated: upper protected band was written by the conditional write (first 20): " +
                string.Join(",", upperLeaked.Take(20)));

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("conditional-write-attempts", 0), Is.GreaterThan(0),
                "Conditional-write worker must have attempted at least one batch.");
            Assert.That(stats.GetValueOrDefault("scan-attempts", 0), Is.GreaterThan(0),
                "Soundness scanner must have started at least one scan.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos conditional-set-many workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-30}{kv.Value}");
    }

    private static (string Band, int Score) ClassifySeed(int i)
    {
        if (i < WriteBandStart) return ("lower", ProtectedScore);
        if (i < StickyMatchEnd) return ("sticky-match", MatchSeedScore);
        if (i < StickyNoMatchEnd) return ("sticky-no-match", NoMatchSeedScore);
        if (i < WriteBandEnd) return ("churn", NoMatchSeedScore);
        return ("upper", ProtectedScore);
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
