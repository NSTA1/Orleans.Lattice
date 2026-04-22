using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// chaos + fault-injection theory. Runs a concurrent read/write/scan/split
/// workload against a multi-shard tree while a fault injector arms one-shot
/// <c>WriteStateAsync</c> faults on random shard-root and leaf grains at a
/// configurable rate. Tolerates arbitrary exceptions during the fault window;
/// after faults are disarmed and a quiescence window elapses, the tree must
/// converge to the exact pinned universe with all values matching their
/// monotonic envelope.
///
/// <para>
/// The <c>faultProbability</c> parameter is the probability, per 20 ms tick,
/// that the injector arms a fresh one-shot fault on a randomly-chosen target
/// grain. Orleans' <see cref="FaultInjectionGrainStorage"/> consumes each fault
/// on the next write; the injector re-arms continuously during the chaos
/// window, so this approximates a steady-state storage-failure rate.
/// </para>
///
/// <para>
/// This test exercises recovery surfaces: resumable splits
/// (<c>SplitInProgress</c>), two-phase root promotion (<c>PendingPromotion</c>),
/// shadow <c>MergeManyAsync</c> atomicity, registry version stamping under
/// retry, and idempotent <c>BulkGraft</c>.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ChaosWithFaultsIntegrationTests
{
    private MultiShardFaultInjectionClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int UniverseSize = 200;
    private const int WriterCount = 3;
    private const int BulkWriterCount = 2;
    private const int AtomicWriterCount = 1;
    private const int ReaderCount = 2;
    private const int BulkReaderCount = 2;
    private const int ScannerCount = 2;
    private const int CounterCount = 1;
    private static readonly TimeSpan ChaosDuration = TimeSpan.FromSeconds(8);
    private static readonly TimeSpan SplitInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan FaultInjectionInterval = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan QuiescenceTimeout = TimeSpan.FromSeconds(15);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MultiShardFaultInjectionClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static string KeyOf(int i) => $"fchaos-{i:D5}";

    private static bool IsValidValueFor(int expectedIndex, byte[] value)
    {
        if (value is null || value.Length == 0) return false;
        var s = Encoding.UTF8.GetString(value);
        return s.StartsWith($"v-{expectedIndex}-", StringComparison.Ordinal);
    }

    private static int IndexOfKey(string key)
        => key.StartsWith("fchaos-", StringComparison.Ordinal)
            && int.TryParse(key.AsSpan(7), out var idx)
            ? idx
            : -1;

    /// <summary>
    /// During the chaos window ANY exception is tolerated and simply counted —
    /// storage faults cascade into many shapes (split failures, state
    /// corruption reports, stale routing, aborted enumerations). The real
    /// correctness check happens after quiescence.
    /// </summary>
    private static bool IsToleratedDuringFaults(Exception _) => true;

    private async Task SeedAsync(ILattice tree)
    {
        for (int i = 0; i < UniverseSize; i++)
            await tree.SetAsync(KeyOf(i), Encoding.UTF8.GetBytes($"v-{i}-seed-0"));
    }

    /// <summary>
    /// Computes the deterministic leaf GrainId for a given shard, matching
    /// <c>ShardRootGrain.EnsureRootAsync</c>'s SHA-256 derivation. Used to
    /// pre-register target grains for the fault injector.
    /// </summary>
    private GrainId GetInitialLeafGrainId(string treeId, int shardIndex)
    {
        var shardKey = $"{treeId}/{shardIndex}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(shardKey));
        var guid = new Guid(hash.AsSpan(0, 16));
        return _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(guid).GetGrainId();
    }

    [TestCase(0.00, TestName = "no_faults_baseline")]
    [TestCase(0.05, TestName = "5pct_fault_probability")]
    [TestCase(0.15, TestName = "15pct_fault_probability")]
    [TestCase(0.30, TestName = "30pct_fault_probability")]
    public async Task Chaos_with_storage_faults_converges_after_quiescence(double faultProbability)
    {
        var treeId = $"fchaos-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var faultCtl = _cluster.GrainFactory.GetGrain<IStorageFaultGrain>(
            LatticeOptions.StorageProviderName);

        // Seed universe with faults disabled.
        await SeedAsync(tree);

        // Discover initial target grain IDs for fault injection.
        var targets = new List<GrainId>();
        for (int s = 0; s < MultiShardFaultInjectionClusterFixture.TestShardCount; s++)
        {
            // Leaf grain (deterministic).
            targets.Add(GetInitialLeafGrainId(treeId, s));
            // Shard-root grain.
            var shardRoot = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{s}");
            // Touch it to ensure activation, then capture its GrainId.
            await shardRoot.GetAsync(KeyOf(0));
            targets.Add(shardRoot.GetGrainId());
        }

        // Warm AtomicWriteGrain (saga coordinator) BEFORE the 8s chaos timer
        // starts. Registry-authoritative sizing flows through the registry on
        // cold activation, and the saga itself has reminder + persistence
        // bootstrapping; on slow Linux Release CI a single cold
        // SetManyAtomicAsync can easily exceed the chaos window, leaving
        // atomicTotal == 1 and the success-rate assertion ill-defined. The
        // warmup call is envelope-valid so the post-quiescence invariant
        // check is unaffected.
        await tree.SetManyAtomicAsync(new List<KeyValuePair<string, byte[]>>
        {
            new(KeyOf(0), Encoding.UTF8.GetBytes("v-0-warmup-0")),
            new(KeyOf(1), Encoding.UTF8.GetBytes("v-1-warmup-0")),
        });

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k)
            => s.AddOrUpdate(k, 1, (_, v) => v + 1);

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = new List<Task>();

        // ---- Fault injector: arms one-shot write faults on random targets.
        if (faultProbability > 0)
        {
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(12345);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(FaultInjectionInterval, ct);
                        if (rng.NextDouble() >= faultProbability) continue;
                        var target = targets[rng.Next(targets.Count)];
                        try
                        {
                            await faultCtl.AddFaultOnWrite(target,
                                new InvalidOperationException("Injected chaos fault"));
                            Bump(stats, "faults-armed");
                        }
                        catch (ArgumentException)
                        {
                            // Fault already armed for this target — skip.
                            Bump(stats, "faults-already-armed");
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        failures.Add($"fault-injector threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }, ct));
        }

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
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-write-errors");
                    }
                }
            }, ct));
        }

        // ---- Bulk writers.
        for (int w = 0; w < BulkWriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 104729 + 3);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var batch = new List<KeyValuePair<string, byte[]>>(8);
                        var seen = new HashSet<int>();
                        while (batch.Count < 8)
                        {
                            var idx = rng.Next(UniverseSize);
                            if (!seen.Add(idx)) continue;
                            batch.Add(new(KeyOf(idx),
                                Encoding.UTF8.GetBytes($"v-{idx}-bulk{writerId}-{++seq}")));
                        }
                        await tree.SetManyAsync(batch);
                        Bump(stats, "bulk-writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-write-errors");
                    }
                }
            }, ct));
        }

        // ---- Atomic writers: saga batch writes under fault injection.
        // Storage faults may cause saga rollback; post-quiescence healing
        // restores envelope-valid values for every universe key.
        for (int w = 0; w < AtomicWriterCount; w++)
        {
            var writerId = w;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(writerId * 1299709 + 13);
                int seq = 0;
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var batch = new List<KeyValuePair<string, byte[]>>(2);
                        var seen = new HashSet<int>();
                        while (batch.Count < 2)
                        {
                            var idx = rng.Next(UniverseSize);
                            if (!seen.Add(idx)) continue;
                            batch.Add(new(KeyOf(idx),
                                Encoding.UTF8.GetBytes($"v-{idx}-atomic{writerId}-{++seq}")));
                        }
                        await tree.SetManyAtomicAsync(batch);
                        Bump(stats, "atomic-writes");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-atomic-write-errors");
                    }
                }
            }, ct));
        }

        // ---- Point readers: envelope-validate values if observed.
        for (int r = 0; r < ReaderCount; r++)
        {
            var readerId = r;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(readerId * 48611 + 5);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var idx = rng.Next(UniverseSize);
                        var v = await tree.GetAsync(KeyOf(idx));
                        // During fault window, null is tolerated: a write may
                        // have failed and a subsequent write hasn't replayed
                        // yet. But any NON-null value must match the envelope.
                        if (v is not null && !IsValidValueFor(idx, v))
                            failures.Add($"reader{readerId}: corrupt value for key {idx}: " +
                                Encoding.UTF8.GetString(v));
                        Bump(stats, "point-reads");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-read-errors");
                    }
                }
            }, ct));
        }

        // ---- Bulk readers.
        for (int r = 0; r < BulkReaderCount; r++)
        {
            var readerId = r;
            workers.Add(Task.Run(async () =>
            {
                var rng = new Random(readerId * 15485863 + 7);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var keys = new List<string>(16);
                        for (int i = 0; i < 16; i++)
                            keys.Add(KeyOf(rng.Next(UniverseSize)));
                        var results = await tree.GetManyAsync(keys);
                        foreach (var kv in results)
                        {
                            var idx = IndexOfKey(kv.Key);
                            if (idx < 0)
                                failures.Add($"bulk-reader{readerId}: unknown key '{kv.Key}'");
                            else if (!IsValidValueFor(idx, kv.Value))
                                failures.Add($"bulk-reader{readerId}: corrupt value for key {idx}");
                        }
                        Bump(stats, "bulk-reads");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-read-errors");
                    }
                }
            }, ct));
        }

        // ---- Scanners: only envelope-check; count/universe invariants
        // deferred to the post-quiescence phase.
        for (int s = 0; s < ScannerCount; s++)
        {
            var scannerId = s;
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        string? prev = null;
                        await foreach (var kv in tree.ScanEntriesAsync(cancellationToken: ct))
                        {
                            var idx = IndexOfKey(kv.Key);
                            if (idx < 0)
                                failures.Add($"scanner{scannerId}: unknown key '{kv.Key}'");
                            else if (!IsValidValueFor(idx, kv.Value))
                                failures.Add($"scanner{scannerId}: corrupt value for key {idx}");
                            if (prev is not null && string.CompareOrdinal(prev, kv.Key) >= 0)
                                failures.Add($"scanner{scannerId}: EntriesAsync out-of-order '{prev}' -> '{kv.Key}'");
                            prev = kv.Key;
                        }
                        Bump(stats, "scans");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-scan-errors");
                    }
                }
            }, ct));
        }

        // ---- Counters: CountAsync tolerated to vary during fault window.
        for (int c = 0; c < CounterCount; c++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        _ = await tree.CountAsync();
                        Bump(stats, "counts");
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) when (IsToleratedDuringFaults(ex))
                    {
                        Bump(stats, "tolerated-count-errors");
                    }
                }
            }, ct));
        }

        // ---- Split coordinator.
        workers.Add(Task.Run(async () =>
        {
            var rng = new Random(42);
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
                    await split.SplitAsync(src);
                    await split.RunSplitPassAsync();
                    Bump(stats, "splits");
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) when (IsToleratedDuringFaults(ex))
                {
                    Bump(stats, "tolerated-split-errors");
                }
            }
        }, ct));

        await Task.WhenAll(workers);

        // ---- Quiescence: fault injector is stopped (cts fired). Any
        // remaining armed one-shot faults will be consumed by the next
        // write on their target. Retry healing writes until convergence.
        await DrainAndHealAsync(tree);

        // ---- Strong post-quiescence invariants.
        var finalCount = await tree.CountAsync();
        var finalKeys = new HashSet<string>();
        await foreach (var k in tree.ScanKeysAsync()) finalKeys.Add(k);
        var finalEntries = new Dictionary<string, byte[]>();
        await foreach (var kv in tree.ScanEntriesAsync()) finalEntries[kv.Key] = kv.Value;

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Observed {failures.Count} envelope/key violations during chaos (first 10):\n " +
                string.Join("\n ", failures.Take(10)));

            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "Post-quiescence CountAsync must equal the pinned universe size.");
            Assert.That(finalKeys.Count, Is.EqualTo(UniverseSize),
                "Post-quiescence KeysAsync must yield exactly the pinned universe.");
            Assert.That(finalEntries.Count, Is.EqualTo(UniverseSize),
                "Post-quiescence EntriesAsync must yield exactly the pinned universe.");

            for (int i = 0; i < UniverseSize; i++)
            {
                var key = KeyOf(i);
                Assert.That(finalKeys.Contains(key), Is.True,
                    $"Post-quiescence: universe key '{key}' missing from KeysAsync.");
                if (finalEntries.TryGetValue(key, out var value))
                    Assert.That(IsValidValueFor(i, value), Is.True,
                        $"Post-quiescence: envelope violation for key '{key}': " +
                        Encoding.UTF8.GetString(value));
            }

            foreach (var op in new[] { "point-writes", "point-reads", "scans", "counts", "splits" })
            {
                Assert.That(stats.GetValueOrDefault(op, 0), Is.GreaterThan(0),
                    $"Workload category '{op}' must have performed at least one operation.");
            }

            if (faultProbability > 0)
                Assert.That(stats.GetValueOrDefault("faults-armed", 0), Is.GreaterThan(0),
                    "Fault injector must have armed at least one fault.");

            // Atomic writes must have executed at every fault level. In the
            // no-fault baseline the saga's success rate must be ≥ 70% (same
            // threshold as the split-only chaos test). Under fault injection
            // the saga is intrinsically more vulnerable — any injected fault
            // in any of the saga's N writes rolls the whole batch back — so
            // the baseline-rate assertion does not apply; we only require
            // that the workload did not completely starve.
            var atomicOk = stats.GetValueOrDefault("atomic-writes", 0);
            var atomicTolerated = stats.GetValueOrDefault("tolerated-atomic-write-errors", 0);
            var atomicTotal = atomicOk + atomicTolerated;
            Assert.That(atomicTotal, Is.GreaterThan(0),
                "Expected at least one atomic-write attempt during the chaos window.");
            if (faultProbability == 0)
            {
                // The 70% ratio is only meaningful with a reasonable sample.
                // In the no-faults baseline, a "tolerated" atomic-write error
                // comes from a concurrent shard split rolling back the saga
                // (an expected compensation path, not a regression). A single
                // such rollback on a tiny sample drops the rate below 70%
                // purely by arithmetic. Apply the ratio floor only when we
                // have enough attempts for it to be statistically meaningful;
                // for small samples, require forward progress plus at most
                // one tolerated error.
                const int MinSampleForRatio = 10;
                if (atomicTotal >= MinSampleForRatio)
                {
                    Assert.That((double)atomicOk / atomicTotal, Is.GreaterThanOrEqualTo(0.70),
                        $"Atomic-write success rate was {atomicOk}/{atomicTotal} = " +
                        $"{(double)atomicOk / atomicTotal:P1}; expected ≥ 70% in the no-faults baseline.");
                }
                else
                {
                    Assert.That(atomicOk, Is.GreaterThanOrEqualTo(1),
                        $"No-faults baseline: expected at least one successful atomic write " +
                        $"(got {atomicOk}/{atomicTotal}).");
                    Assert.That(atomicTolerated, Is.LessThanOrEqualTo(1),
                        $"No-faults baseline with small sample (n={atomicTotal}): expected at most " +
                        $"one tolerated atomic-write error (got {atomicTolerated}).");
                }
            }
        });

        TestContext.Out.WriteLine($"ChaosWithFaults stats (p={faultProbability}):");
        foreach (var kv in stats.OrderBy(k => k.Key))
            TestContext.Out.WriteLine($" {kv.Key,-26}{kv.Value}");
    }

    /// <summary>
    /// Drain any remaining one-shot write faults by replaying writes for
    /// every universe key until three consecutive passes complete without
    /// a tolerated error. Bounded by <see cref="QuiescenceTimeout"/>.
    /// </summary>
    private static async Task DrainAndHealAsync(ILattice tree)
    {
        var deadline = DateTime.UtcNow + QuiescenceTimeout;
        int cleanPasses = 0;
        int healSeq = 0;
        while (cleanPasses < 3 && DateTime.UtcNow < deadline)
        {
            bool passClean = true;
            for (int i = 0; i < UniverseSize; i++)
            {
                try
                {
                    await tree.SetAsync(KeyOf(i),
                        Encoding.UTF8.GetBytes($"v-{i}-heal-{healSeq}"));
                }
                catch
                {
                    passClean = false;
                }
            }
            healSeq++;
            if (passClean) cleanPasses++;
            else cleanPasses = 0;
        }
        if (cleanPasses < 3)
            throw new InvalidOperationException(
                $"Tree did not quiesce within {QuiescenceTimeout}: " +
                $"{cleanPasses} clean healing passes (need 3).");
    }
}
