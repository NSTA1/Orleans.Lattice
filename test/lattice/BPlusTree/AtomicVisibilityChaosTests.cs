using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class AtomicVisibilityChaosTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    private const int BatchSize = 16;
    private const int IterationCount = 50;
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

    private static string KeyOf(int i) => $"atomic-{i:D2}";

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
    public async Task Continuous_reader_observes_zero_or_all_keys_for_every_poll()
    {
        var treeId = $"atomic-visibility-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

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

        for (int round = 1; round <= IterationCount; round++)
        {
            var newBatch = new List<KeyValuePair<string, byte[]>>(BatchSize);
            for (int i = 0; i < BatchSize; i++)
                newBatch.Add(new(KeyOf(i), Value(round, i)));

            using var cts = new CancellationTokenSource();
            var ct = cts.Token;
            var preRound = round - 1;
            var postRound = round;

            var reader = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Dictionary<string, byte[]> snapshot;
                    try
                    {
                        snapshot = await tree.GetManyAsync(allKeys);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

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
                    {
                        failures.Add($"round={round}: unknown-round (pre={preCount}, post={postCount}, missing={missingCount}, other={otherCount})");
                    }
                    else if (preCount == BatchSize)
                    {
                        Interlocked.Increment(ref fullPrePolls);
                    }
                    else if (postCount == BatchSize)
                    {
                        Interlocked.Increment(ref fullPostPolls);
                    }
                    else if (missingCount == BatchSize)
                    {
                        Interlocked.Increment(ref fullHiddenPolls);
                    }
                    else
                    {
                        failures.Add($"round={round}: split (pre={preCount}, post={postCount}, missing={missingCount})");
                    }

                    try { await Task.Delay(PollCadence, ct); }
                    catch (OperationCanceledException) { return; }
                }
            }, ct);

            await tree.SetManyAtomicAsync(newBatch);
            await Task.Delay(PollCadence + PollCadence, CancellationToken.None);
            cts.Cancel();
            try { await reader; } catch (OperationCanceledException) { }
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                "Per-tree atomic visibility violation."
                + Environment.NewLine + string.Join(Environment.NewLine, failures));

            Assert.That(totalPolls, Is.GreaterThan(0));
            Assert.That(fullPostPolls, Is.GreaterThan(0));
        });

        var finalSnapshot = await tree.GetManyAsync(allKeys);
        for (int i = 0; i < BatchSize; i++)
        {
            Assert.That(finalSnapshot.TryGetValue(KeyOf(i), out var bytes), Is.True);
            Assert.That(RoundOf(bytes!), Is.EqualTo(IterationCount));
        }

        TestContext.Out.WriteLine($"polls={totalPolls}, pre={fullPrePolls}, hidden={fullHiddenPolls}, post={fullPostPolls}");
    }
}
