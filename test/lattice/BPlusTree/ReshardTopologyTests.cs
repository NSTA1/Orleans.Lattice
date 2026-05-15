using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;
#if LATTICE_DIAG
using Orleans.Lattice.BPlusTree.Grains;
#endif
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Universal cross-cluster atomic-visibility acceptance fixture: a saga
/// whose touched-shard set straddles an online
/// <see cref="ILattice.ReshardAsync"/> must remain atomically visible
/// to a continuous reader. Reshard composes per-shard splits; saga
/// writes mid-flight flow through the source shards' shadow-forward
/// machinery to the new owners and the saga's terminal broadcast
/// retries onto the new owner via
/// <see cref="StaleShardRoutingException"/>. Throughout the window, a
/// continuous reader observes either zero or all keys at every poll.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ReshardTopologyTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int BatchSize = 16;
    private const int IterationCount = 15;
    private const int ReshardTarget = 8;
    private static readonly TimeSpan PollCadence = TimeSpan.FromMilliseconds(10);

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string KeyOf(int i) => $"reshard-tx-{i:D2}";

    private static byte[] Value(int round, int i) =>
        Encoding.UTF8.GetBytes($"v-{round:D3}-{i:D2}");

    private static int RoundOf(byte[] value)
    {
        if (value is null || value.Length == 0) return -1;
        var s = Encoding.UTF8.GetString(value);
        if (!s.StartsWith("v-", StringComparison.Ordinal)) return -1;
        var dash = s.IndexOf('-', 2);
        if (dash < 0) return -1;
        return int.TryParse(s.AsSpan(2, dash - 2), out var r) ? r : -1;
    }

    [Test]
    public async Task Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard()
    {
#if LATTICE_DIAG
        DiagSink.Reset();
        DiagSink.Write($"[DIAG test-start] Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard");
#endif
        var treeId = $"reshard-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed round 0 across all 16 keys so the universe is pinned before
        // the chaos window opens.
        for (int i = 0; i < BatchSize; i++)
            await tree.SetAsync(KeyOf(i), Value(0, i));

        var seedBatch = new List<KeyValuePair<string, byte[]>>(BatchSize);
        for (int i = 0; i < BatchSize; i++)
            seedBatch.Add(new(KeyOf(i), Value(0, i)));
        await tree.SetManyAtomicAsync(seedBatch);

        var allKeys = Enumerable.Range(0, BatchSize).Select(KeyOf).ToList();

        var failures = new ConcurrentBag<string>();
        long totalPolls = 0;
        long fullPostPolls = 0;
        long fullPrePolls = 0;
        long fullHiddenPolls = 0;

        // Kick off an online reshard from 4 to 8 physical shards in
        // parallel with the saga iterations, then pump the coordinator
        // and every per-shard split that it spawns. Saga writes that hit
        // a slot mid-migration are shadow-forwarded to the new owner;
        // the saga's terminal broadcast retries onto the new owner via
        // StaleShardRoutingException.
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        await tree.ReshardAsync(ReshardTarget);

        using var driverCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var driver = Task.Run(async () =>
        {
            while (!driverCts.IsCancellationRequested)
            {
                if (await reshard.IsIdleAsync()) return;
                await reshard.RunReshardPassAsync();

                var map = await registry.GetShardMapAsync(treeId)
                    ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);
                foreach (var idx in map.GetPhysicalShardIndices())
                {
                    if (driverCts.IsCancellationRequested) break;
                    var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                    if (!await split.IsIdleAsync())
                        await split.RunSplitPassAsync();
                }

                try { await Task.Delay(100, driverCts.Token); }
                catch (OperationCanceledException) { return; }
            }
        }, driverCts.Token);

        for (int round = 1; round <= IterationCount; round++)
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG test-round-start] round={round}");
#endif
            var newBatch = new List<KeyValuePair<string, byte[]>>(BatchSize);
            for (int i = 0; i < BatchSize; i++)
                newBatch.Add(new(KeyOf(i), Value(round, i)));

            using var cts = new CancellationTokenSource();
            var ct = cts.Token;
            var preRound = round - 1;
            var postRound = round;
            var capturedRound = round;

            var reader = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Dictionary<string, byte[]> snapshot;
                    try
                    {
                        snapshot = await tree.GetManyAsync(allKeys);
                    }
                    catch (OperationCanceledException) { return; }
                    catch (StaleShardRoutingException) { continue; }
                    catch (StaleTreeRoutingException) { continue; }

                    Interlocked.Increment(ref totalPolls);

                    int preCount = 0, postCount = 0, missingCount = 0, otherCount = 0;
                    List<(string Key, int Round)>? otherDetail = null;
                    foreach (var key in allKeys)
                    {
                        if (!snapshot.TryGetValue(key, out var bytes) || bytes is null)
                        {
                            missingCount++;
                            continue;
                        }
                        var observedRound = RoundOf(bytes);
                        if (observedRound == preRound) preCount++;
                        else if (observedRound == postRound) postCount++;
                        else
                        {
                            otherCount++;
                            (otherDetail ??= []).Add((key, observedRound));
                        }
                    }

                    if (otherCount > 0)
                    {
                        var detail = otherDetail is null
                            ? string.Empty
                            : " detail=[" + string.Join(", ", otherDetail.Select(d => $"{d.Key}=r{d.Round}")) + "]";
                        failures.Add($"round={capturedRound}: unknown-round (pre={preCount}, post={postCount}, missing={missingCount}, other={otherCount}){detail}");
                    }
                    else if (preCount == BatchSize)
                        Interlocked.Increment(ref fullPrePolls);
                    else if (postCount == BatchSize)
                        Interlocked.Increment(ref fullPostPolls);
                    else if (missingCount == BatchSize)
                        Interlocked.Increment(ref fullHiddenPolls);
                    else
                    {
#if LATTICE_DIAG
                        var perKey = string.Join(',', allKeys.Select(k =>
                        {
                            if (!snapshot.TryGetValue(k, out var b) || b is null) return $"{k}=missing";
                            return $"{k}=r{RoundOf(b)}";
                        }));
                        DiagSink.Write($"[DIAG reader-result-mix] round={capturedRound} pre={preCount} post={postCount} missing={missingCount} perKey=[{perKey}]");
#endif
                        failures.Add($"round={capturedRound}: split (pre={preCount}, post={postCount}, missing={missingCount})");
                    }

                    try { await Task.Delay(PollCadence, ct); }
                    catch (OperationCanceledException) { return; }
                }
            }, ct);

            await tree.SetManyAtomicAsync(newBatch);
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG test-round-committed] round={round}");
#endif
            await Task.Delay(PollCadence + PollCadence, CancellationToken.None);
            cts.Cancel();
            try { await reader; } catch (OperationCanceledException) { }
        }

        // Drain any residual reshard work - both the coordinator itself
        // and any per-shard split it spawned - against a quiescent saga
        // loop.
        while (!await reshard.IsIdleAsync())
        {
            await reshard.RunReshardPassAsync();
            var map = await registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);
            foreach (var idx in map.GetPhysicalShardIndices())
            {
                var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                if (!await split.IsIdleAsync())
                    await split.RunSplitPassAsync();
            }
            await Task.Delay(100);
        }
        driverCts.Cancel();
        try { await driver; } catch (OperationCanceledException) { }

        var reshardIdle = await reshard.IsIdleAsync();
        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                "Atomic visibility violation across mid-saga reshard."
                + Environment.NewLine + string.Join(Environment.NewLine, failures));

            Assert.That(totalPolls, Is.GreaterThan(0));
            Assert.That(fullPostPolls, Is.GreaterThan(0),
                "Reader must observe at least one fully-post-saga snapshot across the iteration loop.");
            Assert.That(reshardIdle, Is.True,
                "Reshard must complete before the test exits.");
        });

        // Final invariant: the universe is intact at iteration N after
        // the reshard commits.
        var finalSnapshot = await tree.GetManyAsync(allKeys);
        for (int i = 0; i < BatchSize; i++)
        {
            Assert.That(finalSnapshot.TryGetValue(KeyOf(i), out var bytes), Is.True);
            Assert.That(RoundOf(bytes!), Is.EqualTo(IterationCount),
                $"Key {KeyOf(i)} must hold the final round's value.");
        }

        TestContext.Out.WriteLine($"polls={totalPolls}, pre={fullPrePolls}, hidden={fullHiddenPolls}, post={fullPostPolls}");
    }
}
