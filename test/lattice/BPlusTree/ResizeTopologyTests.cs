using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Universal cross-cluster atomic-visibility acceptance fixture: a saga
/// whose execute phase straddles an online
/// <see cref="ILattice.ResizeAsync"/> must remain atomically visible to
/// a continuous reader. The resize takes a snapshot of the source
/// physical tree, drains it into a destination tree, then swaps the
/// alias; saga writes mid-flight are shadow-forwarded to the
/// destination and the saga's terminal broadcast retries onto the new
/// owner via <see cref="StaleTreeRoutingException"/>. Throughout the
/// window, a continuous reader observes either zero or all keys at
/// every poll.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ResizeTopologyTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int BatchSize = 16;
    private const int IterationCount = 15;
    private const int ResizeTargetMaxLeafKeys = 8;
    private const int ResizeTargetMaxInternalChildren = 8;
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

    private static string KeyOf(int i) => $"resize-tx-{i:D2}";

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
    public async Task Continuous_reader_observes_zero_or_all_keys_through_mid_saga_resize()
    {
        var treeId = $"resize-{Guid.NewGuid():N}";
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

        // Kick off an online resize in parallel with the saga iterations,
        // then pump the coordinator to completion. Saga writes that hit
        // the source physical tree during the snapshot drain are
        // shadow-forwarded to the destination; the saga's terminal
        // broadcast retries onto the new owner via
        // StaleTreeRoutingException after the alias swap.
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(ResizeTargetMaxLeafKeys, ResizeTargetMaxInternalChildren);

        using var driverCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var driver = Task.Run(async () =>
        {
            while (!driverCts.IsCancellationRequested)
            {
                if (await resize.IsIdleAsync()) return;
                await resize.RunResizePassAsync();
                try { await Task.Delay(100, driverCts.Token); }
                catch (OperationCanceledException) { return; }
            }
        }, driverCts.Token);

        for (int round = 1; round <= IterationCount; round++)
        {
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
                        else otherCount++;
                    }

                    if (otherCount > 0)
                        failures.Add($"round={capturedRound}: unknown-round (pre={preCount}, post={postCount}, missing={missingCount}, other={otherCount})");
                    else if (preCount == BatchSize)
                        Interlocked.Increment(ref fullPrePolls);
                    else if (postCount == BatchSize)
                        Interlocked.Increment(ref fullPostPolls);
                    else if (missingCount == BatchSize)
                        Interlocked.Increment(ref fullHiddenPolls);
                    else
                        failures.Add($"round={capturedRound}: split (pre={preCount}, post={postCount}, missing={missingCount})");

                    try { await Task.Delay(PollCadence, ct); }
                    catch (OperationCanceledException) { return; }
                }
            }, ct);

            await tree.SetManyAtomicAsync(newBatch);
            await Task.Delay(PollCadence + PollCadence, CancellationToken.None);
            cts.Cancel();
            try { await reader; } catch (OperationCanceledException) { }
        }

        // Drain any residual resize work against a quiescent saga loop.
        while (!await resize.IsIdleAsync())
        {
            await resize.RunResizePassAsync();
            await Task.Delay(100);
        }
        driverCts.Cancel();
        try { await driver; } catch (OperationCanceledException) { }

        var resizeIdle = await resize.IsIdleAsync();
        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                "Atomic visibility violation across mid-saga resize."
                + Environment.NewLine + string.Join(Environment.NewLine, failures));

            Assert.That(totalPolls, Is.GreaterThan(0));
            Assert.That(fullPostPolls, Is.GreaterThan(0),
                "Reader must observe at least one fully-post-saga snapshot across the iteration loop.");
            Assert.That(resizeIdle, Is.True,
                "Resize must complete before the test exits.");
        });

        // Final invariant: the universe is intact at iteration N after the
        // alias swap.
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
