using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class ShardSplitIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

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

    [Test]
    public async Task Manual_split_migrates_moved_slot_keys_to_new_shard_with_no_data_loss()
    {
        var treeId = $"split-int-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Populate enough keys to span multiple virtual slots on shard 0.
        var expected = new Dictionary<string, string>();
        for (int i = 0; i < 200; i++)
        {
            var key = $"key-{i:D4}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        // Drive a manual split of shard 0 to completion.
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        // Read all keys back via the public API — LatticeGrain should refresh
        // its cached ShardMap on StaleShardRoutingException and route to the
        // new physical shard.
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing after split");
            Assert.That(Encoding.UTF8.GetString(actual!), Is.EqualTo(value), $"Wrong value for '{key}' after split");
        }

        // Routing should now reference 5 physical shards (0..3 originals + 4 new)
        // — query the registry directly to avoid stateless-worker activation cache effects.
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var persistedMap = await registry.GetShardMapAsync(treeId);
        Assert.That(persistedMap, Is.Not.Null, "Split must persist a custom shard map.");
        Assert.That(persistedMap!.GetPhysicalShardIndices(), Does.Contain(4),
            "New physical shard should appear in the post-swap shard map.");
    }

    [Test]
    public async Task SplitAsync_idempotent_for_same_source_shard()
    {
        var treeId = $"split-idem-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("k", [1]);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        // Same source — must not throw.
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task Reads_and_scans_continue_to_return_correct_results_during_split()
    {
        var treeId = $"split-reads-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Pre-populate: 500 keys span enough virtual slots to exercise both
        // moved and non-moved partitions across all four physical shards.
        const int keyCount = 500;
        var expected = new Dictionary<string, byte[]>(keyCount);
        for (int i = 0; i < keyCount; i++)
        {
            var key = $"rk-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"val-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }

        var failures = new ConcurrentBag<string>();
        var readsCompleted = 0;
        var scansCompleted = 0;
        var scansAborted = 0;
        using var cts = new CancellationTokenSource();

        // 4 random-point readers — each picks random keys and verifies value.
        var pointReaders = Enumerable.Range(0, 4).Select(workerId => Task.Run(async () =>
        {
            var rng = new Random(unchecked(workerId * 1_000_003));
            var keys = expected.Keys.ToArray();
            while (!cts.IsCancellationRequested)
            {
                var key = keys[rng.Next(keys.Length)];
                try
                {
                    var actual = await tree.GetAsync(key);
                    if (actual is null)
                    {
                        failures.Add($"GetAsync('{key}') returned null mid-split");
                    }
                    else if (!actual.AsSpan().SequenceEqual(expected[key]))
                    {
                        failures.Add($"GetAsync('{key}') returned wrong value mid-split");
                    }
                    Interlocked.Increment(ref readsCompleted);
                }
                catch (Exception ex)
                {
                    failures.Add($"GetAsync('{key}') threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        })).ToArray();

        // 2 full-range scanners — each iterates KeysAsync / EntriesAsync end-to-end.
        // Note: a scan that brackets the shard-map swap may transiently
        // under- or over-count (snapshot isolation across a topology change is
        // not promised by the public API), but every yielded key/value MUST
        // be valid. The test checks both: data validity is enforced strictly;
        // count drift is tolerated for in-flight scans (a strict count check
        // is performed on the post-split scan below).
        var scanReaders = new[]
        {
            Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        await foreach (var key in tree.KeysAsync())
                        {
                            if (cts.IsCancellationRequested) break;
                            if (!expected.ContainsKey(key))
                            {
                                failures.Add($"KeysAsync yielded unknown key '{key}'");
                            }
                        }
                        Interlocked.Increment(ref scansCompleted);
                    }
                    catch (Exception ex) when (IsTransientScanAbort(ex))
                    {
                        Interlocked.Increment(ref scansAborted);
                    }
                    catch (Exception ex)
                    {
                        failures.Add($"KeysAsync threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }),
            Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        await foreach (var kv in tree.EntriesAsync())
                        {
                            if (cts.IsCancellationRequested) break;
                            if (!expected.TryGetValue(kv.Key, out var want))
                            {
                                failures.Add($"EntriesAsync yielded unknown key '{kv.Key}'");
                            }
                            else if (!kv.Value.AsSpan().SequenceEqual(want))
                            {
                                failures.Add($"EntriesAsync yielded wrong value for '{kv.Key}'");
                            }
                        }
                        Interlocked.Increment(ref scansCompleted);
                    }
                    catch (Exception ex) when (IsTransientScanAbort(ex))
                    {
                        Interlocked.Increment(ref scansAborted);
                    }
                    catch (Exception ex)
                    {
                        failures.Add($"EntriesAsync threw: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }),
        };

        // Let workers warm up so reads are definitely in flight when split begins.
        await Task.Delay(100);

        // Drive a manual split of shard 0 to completion while readers run.
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        // Allow readers to also exercise the post-swap path (refreshed shard map).
        await Task.Delay(250);

        cts.Cancel();
        await Task.WhenAll(pointReaders.Concat(scanReaders));

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty, $"Concurrent reads/scans observed inconsistencies during split:\n {string.Join("\n ", failures.Take(20))}");
            Assert.That(readsCompleted, Is.GreaterThan(0), "At least one point read should have executed during the split window.");
            Assert.That(scansCompleted + scansAborted, Is.GreaterThan(0), "At least one scan attempt should have executed during the split window.");
        });

        // Final consistency check after split completes — both point reads
        // and a full scan must show exactly the expected set.
        foreach (var (key, want) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' missing post-split.");
            Assert.That(actual!.AsSpan().SequenceEqual(want), Is.True, $"Key '{key}' wrong value post-split.");
        }

        var postSplitKeys = new HashSet<string>();
        await foreach (var key in tree.KeysAsync())
        {
            postSplitKeys.Add(key);
        }
        Assert.That(postSplitKeys, Is.EquivalentTo(expected.Keys),
            "Post-split full scan must yield exactly the expected key set (no duplicates, no missing).");
    }

    [Test]
    public async Task Writes_continue_to_succeed_during_split_and_all_values_survive()
    {
        var treeId = $"split-writes-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed a baseline so the tree has structure before the split begins.
        const int baselineKeys = 200;
        for (int i = 0; i < baselineKeys; i++)
        {
            await tree.SetAsync($"bk-{i:D5}", Encoding.UTF8.GetBytes($"baseline-{i}"));
        }

        var failures = new ConcurrentBag<string>();
        // Each worker owns a disjoint key range and records the last value
        // it successfully wrote per key. After the split we read back every
        // recorded key and verify the recorded value is observed.
        const int workerCount = 4;
        const int keysPerWorker = 100;
        var lastWritten = new ConcurrentDictionary<string, byte[]>();
        using var cts = new CancellationTokenSource();

        var writers = Enumerable.Range(0, workerCount).Select(workerId => Task.Run(async () =>
        {
            var iteration = 0;
            while (!cts.IsCancellationRequested)
            {
                var keyIndex = iteration % keysPerWorker;
                var key = $"w{workerId:D2}-k{keyIndex:D4}";
                var value = Encoding.UTF8.GetBytes($"w{workerId}-i{iteration}");
                try
                {
                    await tree.SetAsync(key, value);
                    lastWritten[key] = value;
                    iteration++;
                }
                catch (Exception ex)
                {
                    failures.Add($"SetAsync('{key}') threw: {ex.GetType().Name}: {ex.Message}");
                    break;
                }
            }
        })).ToArray();

        // Let writers warm up.
        await Task.Delay(100);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        // Drive more writes after the swap so we cover the post-reject refresh path too.
        await Task.Delay(250);

        cts.Cancel();
        await Task.WhenAll(writers);

        Assert.That(failures, Is.Empty, $"Concurrent writes failed during split:\n {string.Join("\n ", failures.Take(20))}");
        Assert.That(lastWritten, Is.Not.Empty, "Writers should have produced at least one successful write.");

        // Every recorded write must read back with the recorded value.
        foreach (var (key, want) in lastWritten)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Concurrent-write key '{key}' missing post-split.");
            Assert.That(actual!.AsSpan().SequenceEqual(want), Is.True,
                $"Concurrent-write key '{key}' has wrong value post-split: " +
                $"got '{Encoding.UTF8.GetString(actual!)}', want '{Encoding.UTF8.GetString(want)}'.");
        }

        // Baseline data must also survive intact.
        for (int i = 0; i < baselineKeys; i++)
        {
            var key = $"bk-{i:D5}";
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Baseline key '{key}' missing post-split.");
            Assert.That(Encoding.UTF8.GetString(actual!), Is.EqualTo($"baseline-{i}"),
                $"Baseline key '{key}' wrong post-split.");
        }
    }

    [Test]
    public async Task Split_shadow_forward_preserves_TTL_on_target_shard()
    {
        // Regression test for interaction:
        // shadow-forward used to reconstruct LwwValue via LwwValue.Create(value, version),
        // silently dropping ExpiresAtTicks. After split commit, the target shard held
        // non-expiring copies of TTL'd writes. The fix routes shadow-forward through
        // IBPlusLeafGrain.GetRawEntryAsync so the raw LwwValue (including expiry)
        // is forwarded verbatim.
        var treeId = $"split-ttl-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed enough keys so shard 0 owns multiple virtual slots that will move.
        for (int i = 0; i < 200; i++)
        {
            await tree.SetAsync($"seed-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0); // source enters BeginShadowWrite.

        // Write TTL'd entries while the split is active. Any key whose virtual slot
        // is moved will be shadow-forwarded to the target shard; once the map swaps,
        // subsequent reads route to the target and the entry's expiry must still apply.
        var ttl = TimeSpan.FromMilliseconds(800);
        var ttlKeys = new List<string>();
        for (int i = 0; i < 50; i++)
        {
            var k = $"ttl-{i:D3}";
            ttlKeys.Add(k);
            await tree.SetAsync(k, Encoding.UTF8.GetBytes($"e{i}"), ttl);
        }

        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        // Immediately after split: every TTL'd key should still be live
        // (TTL=800ms hasn't elapsed yet).
        foreach (var k in ttlKeys)
        {
            Assert.That(await tree.GetAsync(k), Is.Not.Null,
                $"TTL'd key '{k}' should be live immediately after split.");
        }

        // Wait past the TTL. Every TTL'd key must now read null regardless of which
        // physical shard serves it — previously, shadow-forwarded copies on the
        // target shard had ExpiresAtTicks=0 and would remain live indefinitely.
        await Task.Delay(ttl + TimeSpan.FromMilliseconds(400));

        var leaked = new List<string>();
        foreach (var k in ttlKeys)
        {
            if (await tree.GetAsync(k) is not null)
                leaked.Add(k);
        }

        Assert.That(leaked, Is.Empty,
            $"TTL must survive shadow-forward: {leaked.Count} key(s) remained live past expiry " +
            $"({string.Join(", ", leaked.Take(5))}{(leaked.Count > 5 ? "..." : "")}).");
    }

    private static bool IsTransientScanAbort(Exception ex)
    {
        // Orleans throws EnumerationAbortedException (and wraps it) when a streaming
        // enumerator's remote target activation is recycled mid-scan. Identify it
        // by type name to avoid taking a hard dependency on internal Orleans types.
        for (var current = (Exception?)ex; current is not null; current = current.InnerException)
        {
            if (current.GetType().Name == "EnumerationAbortedException")
            {
                return true;
            }
        }
        return false;
    }
}
