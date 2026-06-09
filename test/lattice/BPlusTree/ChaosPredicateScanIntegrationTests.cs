using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the typed streaming-scan predicate push-down overloads
/// (<see cref="TypedLatticeExtensions.ScanEntriesAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string?, string?, bool, bool?, int?, CancellationToken)"/>,
/// <see cref="TypedLatticeExtensions.ScanKeysAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string?, string?, bool, bool?, int?, CancellationToken)"/>
/// and <see cref="TypedLatticeExtensions.ScanValuesAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string?, string?, bool, bool?, int?, CancellationToken)"/>)
/// under sustained split churn and concurrent conflicting writes. The predicate
/// is evaluated inside each shard's leaf-scan page materialization, so this
/// test proves the filtered scans are <b>ordered</b> (keys ascending end-to-end
/// including across a mid-scan split), <b>sound</b> (every surfaced value
/// satisfies the predicate), and <b>complete</b> over a steady-state band whose
/// values never change while keys migrate between shards.
/// </summary>
/// <remarks>
/// Universe layout: 600 keys, each value a JSON <c>ChaosScored</c> document.
/// <list type="bullet">
///   <item><description><c>ps-000000..ps-000199</c> - <b>stable band</b>: written once with <c>Score == Index</c> and never rewritten. A completeness worker scans this band with <c>d =&gt; d.Score &lt; 100</c> and asserts the surfaced keys are exactly indices <c>[0, 100)</c> in ascending order.</description></item>
///   <item><description><c>ps-000200..ps-000599</c> - <b>churn band</b>: point writers continuously rewrite these keys with fresh random scores. Soundness workers scan with <c>d =&gt; d.Score &gt;= Threshold</c> over entries / values and assert every surfaced document satisfies the predicate, and that keys arrive strictly ascending.</description></item>
/// </list>
/// Post-window invariants: no ordering, soundness, or completeness violation;
/// a final stable-band filtered scan returns exactly indices <c>[0, 100)</c>;
/// every worker made progress.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosPredicateScanIntegrationTests
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
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);

    private static readonly string StableStartKey = KeyOf(StableStart);
    private static readonly string ChurnStartKey = KeyOf(ChurnStart);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"ps-{i:D6}";

    private static int IndexOfKey(string key) =>
        key.StartsWith("ps-", StringComparison.Ordinal)
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
    public async Task Chaos_predicate_scans_under_split_churn_are_ordered_sound_and_complete()
    {
        var treeId = $"ps-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        for (int i = StableStart; i < UniverseEnd; i++)
        {
            var band = i < ChurnStart ? "stable" : "churn";
            var score = i < ChurnStart ? i : i % ScoreSpread;
            await tree.SetAsync(KeyOf(i), new ChaosScored(i, band, score));
        }

        var expectedStableMatch = Enumerable.Range(StableStart, ChurnStart - StableStart)
            .Where(i => i < StableMatchThreshold)
            .Select(KeyOf)
            .ToList();

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Warm cold-activation paths outside the chaos window.
        _ = await tree.CountPerShardAsync();
        await foreach (var _ in tree.ScanEntriesAsync<ChaosScored>(d => d.Score >= ChurnThreshold)) { break; }

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Point writers: rewrite churn-band keys with fresh scores.
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
            }, ct));
        }

        // ---- Entry-scan soundness + ordering: every surfaced entry must
        // satisfy the predicate, and keys must arrive strictly ascending even
        // as splits move keys between shards mid-scan.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    Bump(stats, "entry-scan-attempts");
                    string? prevKey = null;
                    await foreach (var entry in tree.ScanEntriesAsync<ChaosScored>(d => d.Score >= ChurnThreshold).WithCancellation(ct))
                    {
                        if (prevKey is not null && string.CompareOrdinal(entry.Key, prevKey) <= 0)
                            failures.Add($"entry-scan: out-of-order '{entry.Key}' after '{prevKey}'");
                        prevKey = entry.Key;
                        if (entry.Value.Score < ChurnThreshold)
                            failures.Add($"entry-scan: '{entry.Key}' surfaced score {entry.Value.Score} < {ChurnThreshold}");
                    }
                    Bump(stats, "entry-scans");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-entry-scans"); }
                catch (Exception ex)
                {
                    failures.Add($"entry-scan threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Value-scan soundness: every surfaced value satisfies the
        // predicate (the value projection drops non-matching values server-side).
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    Bump(stats, "value-scan-attempts");
                    await foreach (var value in tree.ScanValuesAsync<ChaosScored>(d => d.Score >= ChurnThreshold).WithCancellation(ct))
                    {
                        if (value.Score < ChurnThreshold)
                            failures.Add($"value-scan: surfaced score {value.Score} < {ChurnThreshold}");
                    }
                    Bump(stats, "value-scans");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-value-scans"); }
                catch (Exception ex)
                {
                    failures.Add($"value-scan threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Key-scan completeness + ordering over the stable band: the band
        // never mutates, so a filtered key scan must surface exactly the
        // deterministic match set in ascending order on every pass, regardless
        // of split churn. This also exercises the keys-only path (the leaf
        // reads each value to test the predicate but only keys cross the wire).
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    Bump(stats, "key-scan-attempts");
                    var got = new List<string>(expectedStableMatch.Count);
                    string? prevKey = null;
                    await foreach (var key in tree.ScanKeysAsync<ChaosScored>(
                        d => d.Score < StableMatchThreshold,
                        startInclusive: StableStartKey,
                        endExclusive: ChurnStartKey).WithCancellation(ct))
                    {
                        if (prevKey is not null && string.CompareOrdinal(key, prevKey) <= 0)
                            failures.Add($"key-scan: out-of-order '{key}' after '{prevKey}'");
                        prevKey = key;
                        got.Add(key);
                    }
                    if (!got.SequenceEqual(expectedStableMatch, StringComparer.Ordinal))
                    {
                        var missing = expectedStableMatch.Except(got, StringComparer.Ordinal).Take(10);
                        var extra = got.Except(expectedStableMatch, StringComparer.Ordinal).Take(10);
                        failures.Add($"key-scan: stable match drift. missing=[{string.Join(",", missing)}] extra=[{string.Join(",", extra)}]");
                    }
                    Bump(stats, "key-scans");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-key-scans"); }
                catch (Exception ex)
                {
                    failures.Add($"key-scan threw: {ex.GetType().Name}: {ex.Message}");
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

        // ---- Post-window invariants.
        await Task.Delay(100);

        var finalStable = new List<string>();
        await foreach (var key in tree.ScanKeysAsync<ChaosScored>(
            d => d.Score < StableMatchThreshold,
            startInclusive: StableStartKey,
            endExclusive: ChurnStartKey))
        {
            finalStable.Add(key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(finalStable, Is.EqualTo(expectedStableMatch),
                "Final stable-band filtered key scan did not return exactly the deterministic match set in order.");

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("entry-scan-attempts", 0), Is.GreaterThan(0),
                "Entry-scan worker must have started at least one filtered scan.");
            Assert.That(stats.GetValueOrDefault("value-scan-attempts", 0), Is.GreaterThan(0),
                "Value-scan worker must have started at least one filtered scan.");
            Assert.That(stats.GetValueOrDefault("key-scan-attempts", 0), Is.GreaterThan(0),
                "Key-scan worker must have started at least one filtered scan.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos predicate-scan workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }
}
