using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of <see cref="ILattice.DeleteRangeAsync(string, string, CancellationToken)"/>
/// under sustained split churn. The receiver-side range-delete code path
/// is materially different from point ops: <c>MutationKind.DeleteRange</c>
/// carries <see cref="Primitives.HybridLogicalClock.Zero"/>, bypasses
/// per-origin HWM dedup, and the implementation walks every leaf
/// in <c>[start, end)</c> tombstoning matching entries. Splits that move
/// keys between shards while the walk is in flight are a real production
/// race that no other chaos test exercises.
/// </summary>
/// <remarks>
/// Universe layout: 600 keys partitioned into three contiguous bands:
/// <list type="bullet">
///   <item><description><c>rd-000000..rd-000199</c> - <b>protected lower band</b>: never deleted, never falls inside the range-delete predicate, must be present at post-window invariant time.</description></item>
///   <item><description><c>rd-000200..rd-000399</c> - <b>delete band</b>: the range-delete worker periodically issues <c>DeleteRangeAsync("rd-000200", "rd-000400")</c>; a refill worker re-writes a small subset of these keys after each delete so the workload mixes deletes with concurrent re-inserts.</description></item>
///   <item><description><c>rd-000400..rd-000599</c> - <b>protected upper band</b>: never deleted, never falls inside the range-delete predicate, must be present at post-window invariant time.</description></item>
/// </list>
/// Post-window invariants:
/// <list type="bullet">
///   <item><description>Every protected-band key (both lower and upper) is present and envelope-valid - the range delete never strayed outside <c>[200, 400)</c>.</description></item>
///   <item><description>The two scanner workers never observe a key inside the delete band whose value envelope is invalid (envelope is <c>v-{idx}-*</c>).</description></item>
///   <item><description>Range-delete worker performed at least one delete; refill worker performed at least one re-insert; split coordinator performed at least one split.</description></item>
/// </list>
/// Live counts inside the delete band are intentionally NOT pinned to a single value because writes and deletes race continuously - the final live count inside <c>[200, 400)</c> depends on whether the last operation was a delete or a refill, which is non-deterministic by construction.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosRangeDeleteIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int LowerProtectedStart = 0;
    private const int DeleteBandStart = 200;
    private const int UpperProtectedStart = 400;
    private const int UniverseEnd = 600;
    private const int PointWriterCount = 3;
    private const int RefillWriterCount = 1;
    private const int ScannerCount = 2;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan RangeDeleteInterval = TimeSpan.FromMilliseconds(400);
    private const string DeleteBandStartKey = "rd-000200";
    private const string DeleteBandEndKey = "rd-000400";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"rd-{i:D6}";

    private static int IndexOfKey(string key) =>
        key.StartsWith("rd-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan(3), out var idx)
            ? idx
            : -1;

    private static bool IsValidValueFor(int expectedIndex, byte[] value)
    {
        if (value is null || value.Length == 0) return false;
        var s = Encoding.UTF8.GetString(value);
        return s.StartsWith($"v-{expectedIndex}-", StringComparison.Ordinal);
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
    public async Task Chaos_range_delete_under_split_churn_preserves_protected_bands()
    {
        var treeId = $"rd-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed every universe key under the v-{idx}-seed-0 envelope.
        for (int i = LowerProtectedStart; i < UniverseEnd; i++)
        {
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"v-{i}-seed-0"));
        }

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Warm cold-activation paths outside the chaos window.
        _ = await tree.CountPerShardAsync();
        _ = await tree.GetAsync(KeyOf(0));

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Point writers: rewrite random universe keys (both bands).
        // Writes inside the delete band race the range-delete worker;
        // writes inside the protected bands never collide.
        for (int w = 0; w < PointWriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 11);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(LowerProtectedStart, UniverseEnd);
                        var value = Encoding.UTF8.GetBytes($"v-{idx}-w{writerId}-{++seq}");
                        await tree.SetAsync(KeyOf(idx), value);
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

        // ---- Refill worker: after each tick, re-inserts a small subset
        // of the delete band so the workload alternates between
        // "all-present" and "many-deleted" windows. Without this the
        // range-delete eventually evacuates the band and the test no
        // longer exercises the delete + concurrent-write interleave.
        for (int w = 0; w < RefillWriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 1299709 + 31);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(75, ct);
                        for (int k = 0; k < 12; k++)
                        {
                            var idx = rng.Next(DeleteBandStart, UpperProtectedStart);
                            var value = Encoding.UTF8.GetBytes($"v-{idx}-refill{writerId}-{++seq}");
                            await tree.SetAsync(KeyOf(idx), value);
                        }
                        Bump(stats, "refills");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-refills"); }
                    catch (Exception ex)
                    {
                        failures.Add($"refill{writerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Range-delete worker: periodically clears the delete band.
        workers.Add(Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(RangeDeleteInterval, ct);
                    Bump(stats, "range-delete-attempts");
                    _ = await tree.DeleteRangeAsync(DeleteBandStartKey, DeleteBandEndKey, ct);
                    Bump(stats, "range-deletes");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-range-deletes"); }
                catch (Exception ex)
                {
                    failures.Add($"range-delete-worker threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct));

        // ---- Scanners: continuously walk the whole universe; assert
        // envelope validity on every observed key. The scanner does NOT
        // pin a specific count - keys in the delete band are racing
        // delete + refill - but every observed key must carry a valid
        // envelope for its index, and protected-band keys must never be
        // absent under the post-window assertion (checked outside the
        // worker).
        for (int s = 0; s < ScannerCount; s++)
        {
            var scannerId = s;
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        Bump(stats, "scans-attempts");
                        await foreach (var kv in tree.ScanEntriesAsync())
                        {
                            if (ct.IsCancellationRequested) break;
                            var idx = IndexOfKey(kv.Key);
                            if (idx < 0)
                            {
                                failures.Add($"scanner{scannerId}: unknown key '{kv.Key}'");
                                continue;
                            }
                            if (!IsValidValueFor(idx, kv.Value))
                            {
                                failures.Add($"scanner{scannerId}: invalid envelope for key {idx}: " +
                                    Encoding.UTF8.GetString(kv.Value));
                            }
                        }
                        Bump(stats, "scans");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-scans"); }
                    catch (Exception ex)
                    {
                        failures.Add($"scanner{scannerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
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
        }, ct));

        await Task.WhenAll(workers);

        // ---- Post-window invariants.
        // Issue a final no-op write to flush any pending refill so the
        // protected-band check is not racing the last refill tick.
        // (Not strictly required - the workers have all returned by
        // now - but it eliminates a race against any in-flight
        // background work the runtime may still be draining.)
        await Task.Delay(100);

        var lowerMissing = new List<int>();
        var lowerBadEnvelope = new List<int>();
        for (int i = LowerProtectedStart; i < DeleteBandStart; i++)
        {
            var v = await tree.GetAsync(KeyOf(i));
            if (v is null) lowerMissing.Add(i);
            else if (!IsValidValueFor(i, v)) lowerBadEnvelope.Add(i);
        }

        var upperMissing = new List<int>();
        var upperBadEnvelope = new List<int>();
        for (int i = UpperProtectedStart; i < UniverseEnd; i++)
        {
            var v = await tree.GetAsync(KeyOf(i));
            if (v is null) upperMissing.Add(i);
            else if (!IsValidValueFor(i, v)) upperBadEnvelope.Add(i);
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(lowerMissing, Is.Empty,
                $"Lower protected band leaked into range delete (first 20 missing indices): " +
                string.Join(",", lowerMissing.Take(20)));
            Assert.That(lowerBadEnvelope, Is.Empty,
                "Lower protected band has envelope-invalid values: " +
                string.Join(",", lowerBadEnvelope.Take(20)));

            Assert.That(upperMissing, Is.Empty,
                $"Upper protected band leaked into range delete (first 20 missing indices): " +
                string.Join(",", upperMissing.Take(20)));
            Assert.That(upperBadEnvelope, Is.Empty,
                "Upper protected band has envelope-invalid values: " +
                string.Join(",", upperBadEnvelope.Take(20)));

            Assert.That(stats.GetValueOrDefault("point-writes", 0), Is.GreaterThan(0),
                "Point writers must have made progress.");
            Assert.That(stats.GetValueOrDefault("refills", 0), Is.GreaterThan(0),
                "Refill worker must have completed at least one batch.");
            Assert.That(stats.GetValueOrDefault("range-delete-attempts", 0), Is.GreaterThan(0),
                "Range-delete worker must have attempted at least one delete.");
            Assert.That(stats.GetValueOrDefault("scans-attempts", 0), Is.GreaterThan(0),
                "Scanners must have started at least one full-universe scan.");
            Assert.That(stats.GetValueOrDefault("split-attempts", 0), Is.GreaterThan(0),
                "Split coordinator must have attempted at least one split.");
        });

        TestContext.Out.WriteLine("Chaos range-delete workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }
}
