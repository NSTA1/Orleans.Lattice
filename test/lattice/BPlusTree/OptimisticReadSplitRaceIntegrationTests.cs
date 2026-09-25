using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Races the interleaved optimistic shard-root point read (issue #3474) against
/// continuous leaf splits on a two-silo cluster, and asserts that no read ever
/// returns a stale or missing value.
/// <para>
/// The shard root now resolves the optimistic read's leaf through its
/// per-activation <see cref="IBPlusLeafGrain"/> reference cache rather than a
/// fresh <c>GetGrain</c> per read. That cache maps a leaf <see cref="GrainId"/> to
/// the reference for that same id and holds no routing: the key-to-leaf decision
/// is still taken on every read from the shard root's routing tables and
/// validated against the routing epoch. This fixture is the end-to-end proof of
/// that claim. Splits move keys out of the leaf a reader resolved, so a cache
/// that did carry routing would surface here as a read that returns an older
/// value than one already acknowledged, or null for a key already written.
/// </para>
/// <para>
/// <b>The staleness predicate.</b> Each hot key is written with a strictly
/// increasing counter by one writer, which publishes the counter only after the
/// write is acknowledged. A reader samples that floor BEFORE it issues its read,
/// so any read it then receives must carry a value at least as new. The tree runs
/// with the default <see cref="LatticeOptions.CacheTtl"/> of zero, so the serial
/// fallback is also required to be fresh and the predicate holds on both paths.
/// </para>
/// <para>
/// <b>Why it is not vacuous.</b> The run asserts that leaf splits happened on the
/// tree and that optimistic reads were actually served (the <c>validated</c> arm
/// of the outcome counter), so it cannot pass by having every read fall back to
/// the serial path or by running against a tree that never split.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class OptimisticReadSplitRaceIntegrationTests
{
    private const string TreeName = "optimistic-read-split-race";
    private const int HotKeyCount = 8;
    private const int InsertCount = 400;
    private const int ReaderCount = 8;

    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;

        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
            ShardCount = 1,
        });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Optimistic_reads_never_return_stale_or_missing_values_while_leaves_split()
    {
        Assert.That(_cluster.Silos, Has.Count.GreaterThanOrEqualTo(2),
            "the race must span silos so leaf and shard-root activations are not all co-located");

        var tree = _cluster.Client.GetGrain<ILattice>(TreeName);
        var hotKeys = Enumerable.Range(0, HotKeyCount).Select(i => $"h{i:D2}").ToArray();
        var committed = new long[HotKeyCount];
        foreach (var key in hotKeys)
        {
            await tree.SetAsync(key, BitConverter.GetBytes(0L));
        }

        var outcomes = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
        long splits = 0;
        using var outcomeListener = MeterListening.StartForInstrument(
            LatticeMetrics.ShardRootOptimisticReadOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                if (TagValue(tags, LatticeMetrics.TagTree) == TreeName
                    && TagValue(tags, LatticeMetrics.TagOutcome) is { } outcome)
                {
                    outcomes.AddOrUpdate(outcome, value, (_, running) => running + value);
                }
            }));
        using var splitListener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplits,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                if (TagValue(tags, LatticeMetrics.TagTree) == TreeName)
                {
                    Interlocked.Add(ref splits, value);
                }
            }));

        var inserted = new ConcurrentQueue<string>();
        var violations = new ConcurrentQueue<string>();
        using var stop = new CancellationTokenSource();
        long reads = 0;

        // Inserted keys sort between the hot keys ("h03-00017" lies between "h03"
        // and "h04"), so every split lands in a leaf that owns a hot key and moves
        // hot keys between leaves while they are being read.
        var inserter = Task.Run(async () =>
        {
            var rng = new Random(3474);
            for (var n = 0; n < InsertCount; n++)
            {
                var key = $"h{rng.Next(HotKeyCount):D2}-{n:D5}";
                await tree.SetAsync(key, BitConverter.GetBytes((long)n));
                inserted.Enqueue(key);
            }
        });

        var writers = Enumerable.Range(0, HotKeyCount).Select(i => Task.Run(async () =>
        {
            for (long v = 1; !stop.IsCancellationRequested; v++)
            {
                await tree.SetAsync(hotKeys[i], BitConverter.GetBytes(v));
                Volatile.Write(ref committed[i], v);
            }
        })).ToArray();

        var readers = Enumerable.Range(0, ReaderCount).Select(r => Task.Run(async () =>
        {
            var rng = new Random(r);
            while (!stop.IsCancellationRequested)
            {
                var i = rng.Next(HotKeyCount);
                var floor = Volatile.Read(ref committed[i]);
                var value = await tree.GetAsync(hotKeys[i]);
                Interlocked.Increment(ref reads);
                if (value is null)
                {
                    violations.Enqueue($"{hotKeys[i]} read null after value {floor} was acknowledged");
                }
                else if (BitConverter.ToInt64(value) < floor)
                {
                    violations.Enqueue($"{hotKeys[i]} read stale value {BitConverter.ToInt64(value)} after {floor} was acknowledged");
                }

                // Keys already inserted must stay readable wherever a split moved them.
                if (inserted.TryPeek(out _) && rng.Next(4) == 0)
                {
                    var snapshot = inserted.ToArray();
                    var key = snapshot[rng.Next(snapshot.Length)];
                    if (await tree.GetAsync(key) is null)
                    {
                        violations.Enqueue($"inserted key {key} read null after its write was acknowledged");
                    }
                }
            }
        })).ToArray();

        try
        {
            await inserter.WaitAsync(TimeSpan.FromMinutes(3));
        }
        finally
        {
            stop.Cancel();
            await Task.WhenAll(writers.Concat(readers)).WaitAsync(TimeSpan.FromMinutes(1));
        }

        outcomeListener.Dispose();
        splitListener.Dispose();

        var breakdown = string.Join(", ", outcomes.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value}"));
        TestContext.Out.WriteLine(
            $"reads={Interlocked.Read(ref reads)} splits={Interlocked.Read(ref splits)} outcomes: {breakdown}");
        Assert.Multiple(() =>
        {
            Assert.That(violations, Is.Empty,
                $"no read may return a value older than one already acknowledged, or null for a written key "
                + $"(reads={Interlocked.Read(ref reads)}, splits={Interlocked.Read(ref splits)}, outcomes: {breakdown})");
            Assert.That(Interlocked.Read(ref splits), Is.GreaterThan(0),
                "the tree must split during the run, or the race was never exercised");
            Assert.That(outcomes.GetValueOrDefault("validated"), Is.GreaterThan(0),
                $"optimistic reads must actually be served during the run, or every read took the serial path ({breakdown})");
        });
    }

    private static string? TagValue(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, key, StringComparison.Ordinal))
            {
                return tag.Value as string;
            }
        }

        return null;
    }
}
