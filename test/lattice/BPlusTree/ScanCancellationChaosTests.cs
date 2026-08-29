using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage of mid-flight scan cancellation. N scanner workers
/// each repeatedly open <see cref="Orleans.Lattice.ILattice.KeysAsync"/>
/// or <see cref="Orleans.Lattice.ILattice.EntriesAsync"/>,
/// cancel after a small random interval, and re-open. Meanwhile a
/// writer worker keeps churning the universe so the scanner's
/// underlying enumerator is genuinely live and not just iterating
/// over a static snapshot.
/// </summary>
/// <remarks>
/// Invariants:
/// <list type="bullet">
///   <item><description>Mid-flight cancellation must surface as
///   <see cref="OperationCanceledException"/> (the scanner's catch
///   handles it) or as an
///   <c>EnumerationAbortedException</c> from a stateless-worker
///   enumerator activation collection - both are tolerated. Any
///   other exception leaving a scanner worker is a regression in the
///   enumerator's cancellation handling.</description></item>
///   <item><description>Every observed key on a partial-scan path must
///   match the <c>v-{idx}-*</c> envelope - cancellation must not yield
///   half-decoded rows or otherwise corrupt the per-entry shape.</description></item>
///   <item><description>After the cancellation chaos window closes,
///   a fresh full-universe scan must complete cleanly and return
///   exactly the pinned universe count. This is the cleanup invariant
///   the test exists to pin: a leaked enumerator that pinned a leaf
///   activation would normally manifest as either a hang or a
///   wrong-count post-window scan.</description></item>
///   <item><description>Workload categories must have made progress:
///   the scanners must have completed at least one cancellation, and
///   the writer must have updated at least one key.</description></item>
/// </list>
/// The cancellation interval is deliberately tight (5-25 ms) so the
/// scanners are mostly cancelling mid-page rather than after the page
/// completes; this exercises the cancellation seam at the most stressful
/// point in the enumerator's lifecycle.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ScanCancellationChaosTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int UniverseSize = 200;
    private const int ScannerCount = 4;
    private const int WriterCount = 2;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(12);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"scancel-{i:D5}";

    private static int IndexOfKey(string key) =>
        key.StartsWith("scancel-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan(8), out var idx)
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
            && ex.Message.Contains("retries while topology kept changing", StringComparison.Ordinal))
        || (ex is InvalidOperationException
            && ex.Message.Contains("kept committing sagas faster than the fan-out", StringComparison.Ordinal))
        || ex is TimeoutException;

    [Test]
    public async Task Chaos_repeated_scan_cancellation_under_writes_preserves_post_window_invariants()
    {
        var treeId = $"scancel-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        for (int i = 0; i < UniverseSize; i++)
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"v-{i}-seed-0"));

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k) =>
            s.AddOrUpdate(k, 1, (_, v) => v + 1);

        using var chaosCts = new CancellationTokenSource(ChaosDuration);
        var chaosCt = chaosCts.Token;

        var workers = new List<Task>();

        // ---- Writer workers: keep the universe churning so the
        // scanner's underlying enumerator is genuinely live.
        for (int w = 0; w < WriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 23);
                int seq = 0;
                while (!chaosCt.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var value = Encoding.UTF8.GetBytes($"v-{idx}-w{writerId}-{++seq}");
                        await tree.SetAsync(KeyOf(idx), value);
                        Bump(stats, "writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-writes"); }
                    catch (Exception ex)
                    {
                        failures.Add($"writer{writerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, chaosCt));
        }

        // ---- Scanner workers: open scan, cancel after a tight random
        // interval, repeat. Half do KeysAsync, half do EntriesAsync to
        // exercise both enumerator shapes.
        for (int s = 0; s < ScannerCount; s++)
        {
            var scannerId = s;
            var doEntries = (s % 2) == 1;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(scannerId * 15485863 + 41);
                while (!chaosCt.IsCancellationRequested)
                {
                    // Per-scan cancellation token: cancels after a tight
                    // random interval to drive mid-page cancellation.
                    var delayMs = 5 + rng.Next(20);
                    using var scanCts = CancellationTokenSource.CreateLinkedTokenSource(chaosCt);
                    scanCts.CancelAfter(delayMs);

                    try
                    {
                        if (doEntries)
                        {
                            Bump(stats, "entry-scan-attempts");
                            await foreach (var kv in tree.ScanEntriesAsync(cancellationToken: scanCts.Token))
                            {
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
                            // Fell off the end without cancellation - still counts as
                            // a clean scan.
                            Bump(stats, "entry-scan-completions");
                        }
                        else
                        {
                            Bump(stats, "key-scan-attempts");
                            await foreach (var k in tree.ScanKeysAsync(cancellationToken: scanCts.Token))
                            {
                                if (IndexOfKey(k) < 0)
                                {
                                    failures.Add($"scanner{scannerId}: unknown key '{k}'");
                                }
                            }
                            Bump(stats, "key-scan-completions");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected; this is the chaos signal we're driving.
                        Bump(stats, "scan-cancellations");
                    }
                    catch (Exception ex) when (IsTransient(ex))
                    {
                        Bump(stats, "transient-scans");
                    }
                    catch (Exception ex)
                    {
                        failures.Add($"scanner{scannerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, chaosCt));
        }

        await Task.WhenAll(workers);

        // ---- Post-window cleanup invariant. A leaked enumerator that
        // pinned a leaf activation would normally surface here as either
        // a hang on the first scan (the enumerator's activation still
        // owns a lock) or a wrong-count scan (the leaked enumerator's
        // stale state interfering with the fresh one).
        var finalCount = await tree.CountAsync();
        var finalKeys = new HashSet<string>();
        await foreach (var k in tree.ScanKeysAsync(maxAttempts: 5))
        {
            finalKeys.Add(k);
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} non-transient failures (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "Post-window CountAsync must match the pinned universe size - " +
                "a value other than the seed-pinned universe count implies the writer churn " +
                "(which only rewrites existing keys) introduced or dropped keys.");

            Assert.That(finalKeys.Count, Is.EqualTo(UniverseSize),
                "Post-window KeysAsync must yield exactly the pinned universe - a " +
                "leaked enumerator pinning a leaf activation, or a cancellation that left " +
                "the leaf's per-scan state inconsistent, would surface here.");

            Assert.That(stats.GetValueOrDefault("writes", 0), Is.GreaterThan(0),
                "Writer workers must have made progress.");
            Assert.That(stats.GetValueOrDefault("scan-cancellations", 0), Is.GreaterThan(0),
                "At least one scan must have cancelled mid-flight (otherwise the test is vacuous).");
            Assert.That(
                stats.GetValueOrDefault("key-scan-attempts", 0) + stats.GetValueOrDefault("entry-scan-attempts", 0),
                Is.GreaterThan(0),
                "Scanner workers must have started at least one scan.");
        });

        TestContext.Out.WriteLine("Scan cancellation workload stats:");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }
}
