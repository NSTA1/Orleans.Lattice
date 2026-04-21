using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos stress test targeted at the online resize path. Runs a dense
/// concurrent workload of point reads, point writes, scans, and counts
/// against a tree while an online resize changes the fan-out in the
/// background under <see cref="SnapshotMode.Online"/>.
/// <para>
/// Exercises code paths not covered by the main chaos or reshard chaos suites:
/// </para>
/// <list type="bullet">
/// <item><description>The <c>TreeResizeGrain</c> coordinator's phase machine
/// (Snapshot → Swap → Reject → Cleanup) under sustained traffic.</description></item>
/// <item><description>Shadow-forwarding on every source shard: reads and
/// writes must remain transparent across the entire resize window.</description></item>
/// <item><description>The alias swap: mid-flight point reads and writes must
/// continue to see a consistent view before, during, and after the swap.</description></item>
/// <item><description>Strongly-consistent <c>CountAsync</c> / <c>KeysAsync</c>
/// during the online snapshot drain and the Rejecting phase.</description></item>
/// <item><description>No data loss, no duplicate keys, no envelope violations
/// across the full resize window.</description></item>
/// </list>
/// The universe is pinned during the chaos window so the exact-count
/// invariant is well-defined. All writes respect the same <c>v-{idx}-*</c>
/// envelope as the main chaos and reshard chaos tests.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosResizeIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int UniverseSize = 200;
    private const int WriterCount = 3;
    private const int ReaderCount = 2;
    private const int ScannerCount = 1;
    private const int CounterCount = 1;
    private const int ResizeTargetMaxLeafKeys = 16;
    private const int ResizeTargetMaxInternalChildren = 16;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(20);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static string KeyOf(int i) => $"resize-chaos-{i:D5}";

    private static int IndexOfKey(string key)
        => key.StartsWith("resize-chaos-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan("resize-chaos-".Length), out var idx)
            ? idx
            : -1;

    private static bool IsValidValueFor(int expectedIndex, byte[] value)
    {
        if (value is null || value.Length == 0) return false;
        var s = Encoding.UTF8.GetString(value);
        return s.StartsWith($"v-{expectedIndex}-", StringComparison.Ordinal);
    }

    private static bool IsTransient(Exception ex) =>
        ex.GetType().Name is "EnumerationAbortedException"
            or "StaleShardRoutingException"
            or "StaleTreeRoutingException"
        || ex is TimeoutException;

    private static async Task SeedAsync(ILattice tree)
    {
        for (int i = 0; i < UniverseSize; i++)
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"v-{i}-seed-0"));
    }

    private static async Task<HashSet<string>> DrainKeysWithRetryAsync(ILattice tree, int maxAttempts)
    {
        for (int attempt = 1; ; attempt++)
        {
            var keys = new HashSet<string>();
            try
            {
                await foreach (var k in tree.KeysAsync()) keys.Add(k);
                return keys;
            }
            catch (Exception ex) when (ex.GetType().Name == "EnumerationAbortedException" && attempt < maxAttempts)
            {
                // Retry with a fresh enumeration.
            }
        }
    }

    [Test]
    public async Task Chaos_resize_under_concurrent_load_preserves_all_data()
    {
        var treeId = $"resize-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);

        await SeedAsync(tree);

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k)
            => s.AddOrUpdate(k, 1, (_, v) => v + 1);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Point writers.
        for (int w = 0; w < WriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 7919 + 1);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var value = Encoding.UTF8.GetBytes($"v-{idx}-{writerId}-{++seq}");
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

        // ---- Point readers: every observed value must match the envelope.
        for (int r = 0; r < ReaderCount; r++)
        {
            var readerId = r;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(readerId * 15485863 + 5);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var value = await tree.GetAsync(KeyOf(idx));
                        if (value is null)
                            failures.Add($"reader{readerId}: key {KeyOf(idx)} missing mid-chaos");
                        else if (!IsValidValueFor(idx, value))
                            failures.Add($"reader{readerId}: key {KeyOf(idx)} value envelope violated");
                        Bump(stats, "point-reads");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-reads"); }
                    catch (Exception ex)
                    {
                        failures.Add($"reader{readerId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Scanners: full-tree KeysAsync; no duplicates, no unknown keys.
        for (int s = 0; s < ScannerCount; s++)
        {
            var scannerId = s;
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var seen = new HashSet<string>();
                        await foreach (var k in tree.KeysAsync())
                        {
                            if (ct.IsCancellationRequested) break;
                            if (!seen.Add(k))
                                failures.Add($"scanner{scannerId}: duplicate '{k}'");
                            if (IndexOfKey(k) < 0)
                                failures.Add($"scanner{scannerId}: unknown key '{k}'");
                        }
                        Bump(stats, "keys-scans");
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

        // ---- Counters: CountAsync must always equal the pinned universe size.
        for (int c = 0; c < CounterCount; c++)
        {
            var counterId = c;
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var count = await tree.CountAsync();
                        if (count != UniverseSize)
                            failures.Add($"counter{counterId}: CountAsync={count}, expected {UniverseSize}");
                        Bump(stats, "counts");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-counts"); }
                    catch (Exception ex)
                    {
                        failures.Add($"counter{counterId} threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

        // ---- Resize driver: kicks off the resize, then manually drives the
        // coordinator's phase machine (Snapshot → Swap → Reject → Cleanup) to
        // completion. Integration timers in TestingHost tick far too slowly
        // to finish inside the chaos window otherwise. RunResizePassAsync is
        // idempotent and internally drives the snapshot coordinator pass.
        workers.Add(Task.Run(async () =>
        {
            try
            {
                Bump(stats, "resize-attempts");
                await resize.ResizeAsync(ResizeTargetMaxLeafKeys, ResizeTargetMaxInternalChildren);
                Bump(stats, "resize-kicked");

                while (!ct.IsCancellationRequested)
                {
                    if (await resize.IsIdleAsync()) { Bump(stats, "resize-complete"); break; }

                    await resize.RunResizePassAsync();
                    Bump(stats, "resize-passes");

                    await Task.Delay(100, ct);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-resize"); }
            catch (Exception ex)
            {
                failures.Add($"resize-driver threw: {ex.GetType().Name}: {ex.Message}");
            }
        }, ct));

        await Task.WhenAll(workers);

        // Post-chaos drain: finish any residual resize work against a
        // quiescent system so the final invariants are evaluated cleanly.
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!drainCts.IsCancellationRequested && !await resize.IsIdleAsync())
        {
            await resize.RunResizePassAsync();
            await Task.Delay(100);
        }

        var finalCount = await tree.CountAsync();
        var finalKeys = await DrainKeysWithRetryAsync(tree, maxAttempts: 5);
        var resizeDone = await resize.IsIdleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "Post-chaos CountAsync must match the pinned universe size.");
            Assert.That(finalKeys.Count, Is.EqualTo(UniverseSize),
                "Post-chaos KeysAsync must yield exactly the pinned universe.");
            Assert.That(resizeDone, Is.True,
                "Resize must complete within the post-chaos drain window.");

            foreach (var op in new[] { "point-writes", "point-reads", "keys-scans", "counts" })
            {
                Assert.That(stats.GetValueOrDefault(op, 0), Is.GreaterThan(0),
                    $"Workload category '{op}' must have performed at least one operation.");
            }

            Assert.That(stats.GetValueOrDefault("resize-kicked", 0), Is.GreaterThan(0),
                "Resize must have been successfully initiated.");
            Assert.That(stats.GetValueOrDefault("resize-passes", 0), Is.GreaterThan(0),
                "Resize coordinator must have run at least one pass.");
        });
    }
}
