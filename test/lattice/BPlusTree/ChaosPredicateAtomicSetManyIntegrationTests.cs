using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the guarded atomic bulk write
/// (<see cref="TypedLatticeExtensions.SetManyAtomicAsync{T}(ILattice, System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, T}}, System.Linq.Expressions.Expression{System.Func{T, bool}}, CancellationToken)"/>)
/// under sustained split churn and concurrent conflicting point writes. Two
/// guarded-atomic workers repeatedly submit whole-batch writes gated by
/// <c>Score &gt;= Guard</c>, evaluated server-side against each key's pre-saga
/// value: one targets a band whose every key always satisfies the guard (so the
/// batch must <b>always commit</b>), the other targets a band containing a
/// permanently failing "poison" key (so the batch must <b>always abort with
/// PreconditionFailed</b> and write nothing). This proves the guarded atomic
/// write is <b>all-or-nothing</b> (a committed batch stamps every key; an
/// aborted batch stamps none), <b>sound</b> (the never-matching band never
/// surfaces the marker and the poison key is never touched), and <b>outcome-
/// accurate</b> (the returned <see cref="AtomicWriteOutcome"/> matches the
/// band's guard reality), even while splits move keys between shards mid-saga
/// and point writers rewrite values concurrently.
/// </summary>
/// <remarks>
/// The marker score (<see cref="MarkerScore"/>) is itself <c>&gt;= Guard</c> and
/// point writers only ever write guard-satisfying values onto the always-match
/// band, so that band's guard verdict is invariant under the conflicting
/// writes. Universe / band layout mirrors
/// <see cref="ChaosPredicateConditionalSetManyIntegrationTests"/>.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosPredicateAtomicSetManyIntegrationTests
{
    private sealed record ChaosDoc(int Index, string Band, int Score);

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int MatchStart = 0;
    private const int MatchEnd = 50;
    private const int FailStart = 50;       // index 50 is the poison key
    private const int FailEnd = 100;
    private const int ChurnStart = 100;
    private const int UniverseEnd = 250;

    private const int PoisonIndex = FailStart;

    private const int Guard = 500;
    private const int MarkerScore = 7777;    // >= Guard => idempotent once stamped
    private const int MatchSeedScore = 1000; // >= Guard
    private const int PoisonScore = 0;       // <  Guard, never rewritten

    private const int PointWriterCount = 3;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan WriteInterval = TimeSpan.FromMilliseconds(400);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"ga-{i:D6}";

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

    private static List<KeyValuePair<string, ChaosDoc>> MarkerBatch(int start, int end)
    {
        var entries = new List<KeyValuePair<string, ChaosDoc>>(end - start);
        for (int i = start; i < end; i++)
            entries.Add(new(KeyOf(i), new ChaosDoc(i, "marked", MarkerScore)));
        return entries;
    }

    [Test]
    public async Task Chaos_guarded_atomic_set_many_under_split_churn_is_all_or_nothing_and_sound()
    {
        var treeId = $"ga-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        for (int i = MatchStart; i < UniverseEnd; i++)
        {
            var (band, score) = ClassifySeed(i);
            await tree.SetAsync(KeyOf(i), new ChaosDoc(i, band, score));
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Keys that must never surface the marker: the always-fail band (its
        // guarded batch never commits) and everything outside the two guarded
        // bands.
        static bool MustNeverBeMarked(int idx) => idx >= FailStart;

        _ = await tree.CountPerShardAsync();
        _ = await tree.GetAsync<ChaosDoc>(KeyOf(0));

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;
        var workers = new List<Task>();

        // ---- Point writers: rewrite always-match and churn keys with fresh
        // values. Match-band rewrites stay >= Guard so that band's verdict is
        // invariant; the poison key is never touched.
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
                        int idx;
                        int score;
                        if (rng.Next(2) == 0)
                        {
                            idx = rng.Next(MatchStart, MatchEnd);     // always-match band
                            score = rng.Next(Guard, 1001);            // stays >= Guard
                        }
                        else
                        {
                            idx = rng.Next(ChurnStart, UniverseEnd);  // free churn band
                            score = rng.Next(0, 1001);
                        }
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

        // ---- Always-commit worker: the always-match band must commit every time.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(WriteInterval, ct);
                    Bump(stats, "commit-attempts");
                    var outcome = await tree.SetManyAtomicAsync<ChaosDoc>(
                        MarkerBatch(MatchStart, MatchEnd), d => d.Score >= Guard);
                    if (outcome != AtomicWriteOutcome.Committed)
                        failures.Add($"always-match band returned {outcome}, expected Committed");
                    else
                        Bump(stats, "commits");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-commits"); }
                catch (Exception ex)
                {
                    failures.Add($"commit-worker threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Always-abort worker: the poison-bearing band must always abort
        // with PreconditionFailed and never write.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(WriteInterval, ct);
                    Bump(stats, "abort-attempts");
                    var outcome = await tree.SetManyAtomicAsync<ChaosDoc>(
                        MarkerBatch(FailStart, FailEnd), d => d.Score >= Guard);
                    if (outcome != AtomicWriteOutcome.PreconditionFailed)
                        failures.Add($"poison band returned {outcome}, expected PreconditionFailed");
                    else
                        Bump(stats, "aborts");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-aborts"); }
                catch (Exception ex)
                {
                    failures.Add($"abort-worker threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Soundness scanner: the always-fail band must never surface the
        // marker, and the poison key must stay at its seed score.
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
                        if (KeyOf(idx) != kv.Key)
                            failures.Add($"scan: key '{kv.Key}' carried Index {idx}");
                        if (MustNeverBeMarked(idx) && kv.Value.Score == MarkerScore)
                            failures.Add($"scan: never-commit band key '{kv.Key}' surfaced the marker");
                        if (idx == PoisonIndex && kv.Value.Score != PoisonScore)
                            failures.Add($"scan: poison key '{kv.Key}' was mutated to {kv.Value.Score}");
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

        // ---- Final deterministic passes.
        await Task.Delay(100);
        var commitOutcome = await RetryUntilOutcomeAsync(
            () => tree.SetManyAtomicAsync<ChaosDoc>(MarkerBatch(MatchStart, MatchEnd), d => d.Score >= Guard));
        var abortOutcome = await RetryUntilOutcomeAsync(
            () => tree.SetManyAtomicAsync<ChaosDoc>(MarkerBatch(FailStart, FailEnd), d => d.Score >= Guard));

        // ---- Post-window invariants.
        var matchMissing = new List<int>();      // all-or-nothing commit / completeness
        for (int i = MatchStart; i < MatchEnd; i++)
        {
            var v = await tree.GetAsync<ChaosDoc>(KeyOf(i));
            if (v is null || v.Score != MarkerScore) matchMissing.Add(i);
        }

        var failMarked = new List<int>();        // soundness: poison band never written
        for (int i = FailStart; i < FailEnd; i++)
        {
            var v = await tree.GetAsync<ChaosDoc>(KeyOf(i));
            if (v is not null && v.Score == MarkerScore) failMarked.Add(i);
        }

        var poison = await tree.GetAsync<ChaosDoc>(KeyOf(PoisonIndex));

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(commitOutcome, Is.EqualTo(AtomicWriteOutcome.Committed),
                "Final always-match guarded batch must commit.");
            Assert.That(abortOutcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed),
                "Final poison-band guarded batch must abort with PreconditionFailed.");

            Assert.That(matchMissing, Is.Empty,
                "All-or-nothing commit violated: always-match keys did not all carry the marker (first 20): " +
                string.Join(",", matchMissing.Take(20)));

            Assert.That(failMarked, Is.Empty,
                "Soundness violated: poison-band keys surfaced the marker (first 20): " +
                string.Join(",", failMarked.Take(20)));

            Assert.That(poison, Is.Not.Null);
            Assert.That(poison!.Score, Is.EqualTo(PoisonScore),
                "Soundness violated: the poison key was mutated.");

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("commit-attempts", 0), Is.GreaterThan(0),
                "Always-commit worker must have attempted at least one batch.");
            Assert.That(stats.GetValueOrDefault("abort-attempts", 0), Is.GreaterThan(0),
                "Always-abort worker must have attempted at least one batch.");
            Assert.That(stats.GetValueOrDefault("scan-attempts", 0), Is.GreaterThan(0),
                "Soundness scanner must have started at least one scan.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos guarded-atomic-set-many workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-32}{kv.Value}");
    }

    private static (string Band, int Score) ClassifySeed(int i)
    {
        if (i < MatchEnd) return ("always-match", MatchSeedScore);
        if (i == PoisonIndex) return ("poison", PoisonScore);
        if (i < FailEnd) return ("fail-other", MatchSeedScore);
        return ("churn", MatchSeedScore);
    }

    private static async Task<AtomicWriteOutcome> RetryUntilOutcomeAsync(Func<Task<AtomicWriteOutcome>> action)
    {
        for (int attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                await Task.Delay(100);
            }
        }
        return await action();
    }
}
