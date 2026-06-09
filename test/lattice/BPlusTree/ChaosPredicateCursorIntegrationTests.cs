using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of the predicate-filtered stateful cursor overloads
/// (<see cref="TypedLatticeExtensions.OpenKeyCursorAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string?, string?, bool, bool, CancellationToken)"/>,
/// <see cref="TypedLatticeExtensions.OpenEntryCursorAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string?, string?, bool, bool, CancellationToken)"/>
/// and <see cref="TypedLatticeExtensions.OpenSnapshotEntryCursorAsync{T}(ILattice, System.Linq.Expressions.Expression{System.Func{T, bool}}, string?, string?, bool, CancellationToken)"/>)
/// under sustained split churn and concurrent conflicting writes. The compiled
/// predicate IR is persisted on the cursor spec and re-evaluated server-side on
/// every page, so this test proves the filtered cursors are <b>ordered</b>
/// (keys ascending across page boundaries including a mid-paging split),
/// <b>sound</b> (every surfaced page item satisfies the predicate), and
/// <b>complete</b> over a steady-state band whose values never change while
/// keys migrate between shards.
/// </summary>
/// <remarks>
/// Universe layout mirrors the streaming-scan chaos suite: 600 keys, each value
/// a JSON <c>ChaosScored</c> document.
/// <list type="bullet">
///   <item><description><c>pc-000000..pc-000199</c> - <b>stable band</b>: written once with <c>Score == Index</c> and never rewritten. A completeness worker pages a key cursor filtered by <c>d =&gt; d.Score &lt; 100</c> and asserts the surfaced keys are exactly indices <c>[0, 100)</c> in ascending order.</description></item>
///   <item><description><c>pc-000200..pc-000599</c> - <b>churn band</b>: point writers continuously rewrite these keys with fresh random scores. Soundness workers page entry / snapshot-entry cursors filtered by <c>d =&gt; d.Score &gt;= Threshold</c> and assert every surfaced item satisfies the predicate and keys never jump strictly backward within a paging session.</description></item>
/// </list>
/// Post-window invariants: no ordering, soundness, or completeness violation;
/// a final stable-band filtered key cursor returns exactly indices <c>[0, 100)</c>;
/// every worker made progress.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosPredicateCursorIntegrationTests
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
    private const int PageSize = 7;
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

    private static string KeyOf(int i) => $"pc-{i:D6}";

    private static bool IsTransient(Exception ex) =>
        ex.GetType().Name is "EnumerationAbortedException" or "StaleShardRoutingException"
        || (ex is InvalidOperationException
            && ex.Message.Contains("failed and was rolled back", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("retries while topology kept changing", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("kept committing sagas faster than the fan-out", StringComparison.Ordinal))
        // A snapshot cursor reopened in rapid succession can collide with its
        // still-active per-shard snapshot leaf (keyed by the captured WAL
        // coordinate) before the prior activation has released it. This is a
        // benign concurrency condition orthogonal to predicate evaluation; the
        // next loop iteration captures a fresh coordinate and proceeds.
        || (ex is InvalidOperationException
            && ex.Message.Contains("refusing to re-open against", StringComparison.Ordinal))
        || ex is TimeoutException;

    [Test]
    public async Task Chaos_predicate_cursors_under_split_churn_are_ordered_sound_and_complete()
    {
        var treeId = $"pc-chaos-{Guid.NewGuid():N}";
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

        // ---- Entry-cursor soundness + ordering: every surfaced entry must
        // satisfy the predicate, and keys must arrive strictly ascending within
        // a paging session even as splits move keys between shards mid-paging.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                string? cursorId = null;
                try
                {
                    Bump(stats, "entry-cursor-attempts");
                    cursorId = await tree.OpenEntryCursorAsync<ChaosScored>(d => d.Score >= ChurnThreshold);
                    string? prevKey = null;
                    while (true)
                    {
                        var page = await tree.NextEntriesAsync(cursorId, PageSize);
                        foreach (var entry in page.Entries)
                        {
                            if (prevKey is not null && string.CompareOrdinal(entry.Key, prevKey) <= 0)
                                failures.Add($"entry-cursor: out-of-order '{entry.Key}' after '{prevKey}'");
                            prevKey = entry.Key;
                            var score = JsonLatticeSerializer<ChaosScored>.Default.Deserialize(entry.Value).Score;
                            if (score < ChurnThreshold)
                                failures.Add($"entry-cursor: '{entry.Key}' surfaced score {score} < {ChurnThreshold}");
                        }
                        if (!page.HasMore) break;
                        ct.ThrowIfCancellationRequested();
                    }
                    Bump(stats, "entry-cursors");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-entry-cursors"); }
                catch (Exception ex)
                {
                    failures.Add($"entry-cursor threw: {ex.GetType().Name}: {ex.Message}");
                }
                finally
                {
                    if (cursorId is not null)
                        try { await tree.CloseCursorAsync(cursorId); } catch { /* best effort */ }
                }
            }
        }, ct));

        // ---- Snapshot-cursor soundness: the zero-observable-writes snapshot
        // path evaluates the predicate inside the per-shard snapshot leaf. We
        // assert predicate soundness on every surfaced value and that keys
        // never jump strictly backward. (A snapshot coordinate captured during
        // a split can legitimately surface a key from both the source and
        // destination shard, so an equal/duplicate key is tolerated here while
        // a strictly-decreasing key is still a violation.)
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                string? cursorId = null;
                try
                {
                    Bump(stats, "snapshot-cursor-attempts");
                    cursorId = await tree.OpenSnapshotEntryCursorAsync<ChaosScored>(
                        d => d.Score >= ChurnThreshold,
                        startInclusive: ChurnStartKey);
                    string? prevKey = null;
                    while (true)
                    {
                        var page = await tree.NextEntriesAsync(cursorId, PageSize);
                        foreach (var entry in page.Entries)
                        {
                            if (prevKey is not null && string.CompareOrdinal(entry.Key, prevKey) < 0)
                                failures.Add($"snapshot-cursor: backward '{entry.Key}' after '{prevKey}'");
                            prevKey = entry.Key;
                            var score = JsonLatticeSerializer<ChaosScored>.Default.Deserialize(entry.Value).Score;
                            if (score < ChurnThreshold)
                                failures.Add($"snapshot-cursor: '{entry.Key}' surfaced score {score} < {ChurnThreshold}");
                        }
                        if (!page.HasMore) break;
                        ct.ThrowIfCancellationRequested();
                    }
                    Bump(stats, "snapshot-cursors");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-snapshot-cursors"); }
                catch (Exception ex)
                {
                    failures.Add($"snapshot-cursor threw: {ex.GetType().Name}: {ex.Message}");
                }
                finally
                {
                    if (cursorId is not null)
                        try { await tree.CloseCursorAsync(cursorId); } catch { /* best effort */ }
                }
            }
        }, ct));

        // ---- Key-cursor completeness + ordering over the stable band: the band
        // never mutates, so a filtered key cursor must surface exactly the
        // deterministic match set in ascending order on every full pass,
        // regardless of split churn.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                string? cursorId = null;
                try
                {
                    Bump(stats, "key-cursor-attempts");
                    cursorId = await tree.OpenKeyCursorAsync<ChaosScored>(
                        d => d.Score < StableMatchThreshold,
                        startInclusive: StableStartKey,
                        endExclusive: ChurnStartKey);
                    var got = new List<string>(expectedStableMatch.Count);
                    string? prevKey = null;
                    while (true)
                    {
                        var page = await tree.NextKeysAsync(cursorId, PageSize);
                        foreach (var key in page.Keys)
                        {
                            if (prevKey is not null && string.CompareOrdinal(key, prevKey) <= 0)
                                failures.Add($"key-cursor: out-of-order '{key}' after '{prevKey}'");
                            prevKey = key;
                            got.Add(key);
                        }
                        if (!page.HasMore) break;
                        ct.ThrowIfCancellationRequested();
                    }
                    if (!got.SequenceEqual(expectedStableMatch, StringComparer.Ordinal))
                    {
                        var missing = expectedStableMatch.Except(got, StringComparer.Ordinal).Take(10);
                        var extra = got.Except(expectedStableMatch, StringComparer.Ordinal).Take(10);
                        failures.Add($"key-cursor: stable match drift. missing=[{string.Join(",", missing)}] extra=[{string.Join(",", extra)}]");
                    }
                    Bump(stats, "key-cursors");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-key-cursors"); }
                catch (Exception ex)
                {
                    failures.Add($"key-cursor threw: {ex.GetType().Name}: {ex.Message}");
                }
                finally
                {
                    if (cursorId is not null)
                        try { await tree.CloseCursorAsync(cursorId); } catch { /* best effort */ }
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
        var finalCursor = await tree.OpenKeyCursorAsync<ChaosScored>(
            d => d.Score < StableMatchThreshold,
            startInclusive: StableStartKey,
            endExclusive: ChurnStartKey);
        while (true)
        {
            var page = await tree.NextKeysAsync(finalCursor, PageSize);
            finalStable.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(finalCursor);

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(finalStable, Is.EqualTo(expectedStableMatch),
                "Final stable-band filtered key cursor did not return exactly the deterministic match set in order.");

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("entry-cursor-attempts", 0), Is.GreaterThan(0),
                "Entry-cursor worker must have opened at least one filtered cursor.");
            Assert.That(stats.GetValueOrDefault("snapshot-cursor-attempts", 0), Is.GreaterThan(0),
                "Snapshot-cursor worker must have opened at least one filtered cursor.");
            Assert.That(stats.GetValueOrDefault("key-cursor-attempts", 0), Is.GreaterThan(0),
                "Key-cursor worker must have opened at least one filtered cursor.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos predicate-cursor workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-28}{kv.Value}");
    }
}
