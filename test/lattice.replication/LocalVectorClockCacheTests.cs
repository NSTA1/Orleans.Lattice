using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LocalVectorClockCacheTests
{
    private const string TreeId = "tree";
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static (LocalVectorClockCache Cache, IGrainFactory Factory, IReplicationHighWaterMarkGrain Grain)
        CreateCache(VersionVector? coldStartVector = null)
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        grain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(coldStartVector ?? new VersionVector());
        return (new LocalVectorClockCache(factory), factory, grain);
    }

    // ------------------------------------------------------------------
    // GetSnapshotAsync
    // ------------------------------------------------------------------

    [Test]
    public async Task GetSnapshotAsync_first_call_per_tree_seeds_from_grain_and_returns_clone()
    {
        var seed = new VersionVector();
        seed.Entries[RemoteCluster] = Hlc(50);
        var (cache, _, grain) = CreateCache(seed);

        var snapshot = await cache.GetSnapshotAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(Hlc(50)));
            Assert.That(snapshot, Is.Not.SameAs(seed),
                "GetSnapshotAsync must return a defensive clone, not the grain's reference.");
        });
        await grain.Received(1).GetVectorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetSnapshotAsync_subsequent_calls_per_tree_return_in_memory_state_without_grain_hop()
    {
        var (cache, _, grain) = CreateCache();

        await cache.GetSnapshotAsync(TreeId);
        await cache.GetSnapshotAsync(TreeId);
        await cache.GetSnapshotAsync(TreeId);

        await grain.Received(1).GetVectorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetSnapshotAsync_returns_independent_clones_so_caller_mutations_do_not_leak()
    {
        var (cache, _, _) = CreateCache();

        var first = await cache.GetSnapshotAsync(TreeId);
        first.Entries[RemoteCluster] = Hlc(99);

        var second = await cache.GetSnapshotAsync(TreeId);
        Assert.That(second.GetClock(RemoteCluster), Is.EqualTo(HybridLogicalClock.Zero),
            "Mutating a returned snapshot must not bleed into the cache's state.");
    }

    [Test]
    public async Task GetSnapshotAsync_per_tree_isolation_uses_distinct_grain_activations()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grainA = Substitute.For<IReplicationHighWaterMarkGrain>();
        var grainB = Substitute.For<IReplicationHighWaterMarkGrain>();
        var seedA = new VersionVector();
        seedA.Entries[RemoteCluster] = Hlc(10);
        var seedB = new VersionVector();
        seedB.Entries[RemoteCluster] = Hlc(20);
        grainA.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(seedA);
        grainB.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(seedB);
        factory.GetGrain<IReplicationHighWaterMarkGrain>("alpha").Returns(grainA);
        factory.GetGrain<IReplicationHighWaterMarkGrain>("beta").Returns(grainB);
        var cache = new LocalVectorClockCache(factory);

        var alpha = await cache.GetSnapshotAsync("alpha");
        var beta = await cache.GetSnapshotAsync("beta");

        Assert.Multiple(() =>
        {
            Assert.That(alpha.GetClock(RemoteCluster), Is.EqualTo(Hlc(10)));
            Assert.That(beta.GetClock(RemoteCluster), Is.EqualTo(Hlc(20)));
        });
    }

    [Test]
    public async Task GetSnapshotAsync_concurrent_first_callers_share_one_cold_start_rpc()
    {
        // Single-flight cold-start: every concurrent first reader for
        // a tree must observe a single underlying GetVectorAsync RPC,
        // not one per reader.
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        var gate = new TaskCompletionSource<VersionVector>();
        grain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(gate.Task);
        var cache = new LocalVectorClockCache(factory);

        var t1 = cache.GetSnapshotAsync(TreeId);
        var t2 = cache.GetSnapshotAsync(TreeId);
        var t3 = cache.GetSnapshotAsync(TreeId);

        gate.SetResult(new VersionVector());
        await Task.WhenAll(t1, t2, t3);

        await grain.Received(1).GetVectorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void GetSnapshotAsync_throws_on_null_or_empty_tree_id()
    {
        var (cache, _, _) = CreateCache();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await cache.GetSnapshotAsync(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await cache.GetSnapshotAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public async Task GetSnapshotAsync_handles_null_grain_snapshot_as_empty()
    {
        // NSubstitute's default for Task<VersionVector> is null. The
        // cache must tolerate this and return an empty vector rather
        // than NRE'ing in MergeFrom.
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        // Default behaviour - no Returns() configured - so the grain
        // returns Task.FromResult<VersionVector>(null!).
        var cache = new LocalVectorClockCache(factory);

        var snapshot = await cache.GetSnapshotAsync(TreeId);

        Assert.That(snapshot.Entries, Is.Empty);
    }

    [Test]
    public async Task GetSnapshotAsync_swallows_grain_failure_and_retries_cold_start()
    {
        // Best-effort cold-start: a transient grain failure must not
        // fault the producer's emit. The next call retries.
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        var attempt = 0;
        grain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            if (Interlocked.Increment(ref attempt) == 1)
            {
                throw new InvalidOperationException("transient");
            }
            return Task.FromResult(new VersionVector());
        });
        var cache = new LocalVectorClockCache(factory);

        var first = await cache.GetSnapshotAsync(TreeId);
        var second = await cache.GetSnapshotAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries, Is.Empty,
                "First call must return an empty vector after the transient failure (no fault propagated).");
            Assert.That(second.Entries, Is.Empty);
            Assert.That(attempt, Is.EqualTo(2),
                "Second call must retry the cold-start after the first attempt failed.");
        });
    }

    // ------------------------------------------------------------------
    // AdvanceLocal
    // ------------------------------------------------------------------

    [Test]
    public async Task AdvanceLocal_advances_diagonal_monotonically_and_reflects_in_next_snapshot()
    {
        var (cache, _, _) = CreateCache();

        // Seed cold-start.
        await cache.GetSnapshotAsync(TreeId);

        cache.AdvanceLocal(TreeId, LocalCluster, Hlc(10));
        var afterFirst = await cache.GetSnapshotAsync(TreeId);

        cache.AdvanceLocal(TreeId, LocalCluster, Hlc(20));
        var afterSecond = await cache.GetSnapshotAsync(TreeId);

        // Pointwise-max: a smaller candidate must not regress the diagonal.
        cache.AdvanceLocal(TreeId, LocalCluster, Hlc(5));
        var afterRegress = await cache.GetSnapshotAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst.GetClock(LocalCluster), Is.EqualTo(Hlc(10)));
            Assert.That(afterSecond.GetClock(LocalCluster), Is.EqualTo(Hlc(20)));
            Assert.That(afterRegress.GetClock(LocalCluster), Is.EqualTo(Hlc(20)));
        });
    }

    [Test]
    public async Task AdvanceLocal_lazily_creates_tree_state_without_grain_hop()
    {
        // Calling AdvanceLocal before GetSnapshotAsync must not block
        // on the cold-start RPC (the producer cannot afford a grain hop
        // from inside the WAL append's hot path on a tree that has not
        // yet been snapshot-read). The tree state is created lazily
        // and the cold-start fires only when GetSnapshotAsync is called.
        var (cache, _, grain) = CreateCache();

        cache.AdvanceLocal(TreeId, LocalCluster, Hlc(100));

        await grain.DidNotReceive().GetVectorAsync(Arg.Any<CancellationToken>());

        var snapshot = await cache.GetSnapshotAsync(TreeId);
        Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(Hlc(100)),
            "AdvanceLocal made before cold-start must not be lost when cold-start lands.");
    }

    [Test]
    public void AdvanceLocal_throws_on_null_or_empty_args()
    {
        var (cache, _, _) = CreateCache();

        Assert.Multiple(() =>
        {
            Assert.That(() => cache.AdvanceLocal(null!, LocalCluster, Hlc(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => cache.AdvanceLocal(string.Empty, LocalCluster, Hlc(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => cache.AdvanceLocal(TreeId, null!, Hlc(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => cache.AdvanceLocal(TreeId, string.Empty, Hlc(1)), Throws.InstanceOf<ArgumentException>());
        });
    }

    // ------------------------------------------------------------------
    // AdvanceForeign
    // ------------------------------------------------------------------

    [Test]
    public async Task AdvanceForeign_advances_origin_entry_monotonically()
    {
        var (cache, _, _) = CreateCache();
        await cache.GetSnapshotAsync(TreeId);

        cache.AdvanceForeign(TreeId, RemoteCluster, Hlc(50));
        var afterFirst = await cache.GetSnapshotAsync(TreeId);

        cache.AdvanceForeign(TreeId, RemoteCluster, Hlc(30));
        var afterRegress = await cache.GetSnapshotAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst.GetClock(RemoteCluster), Is.EqualTo(Hlc(50)));
            Assert.That(afterRegress.GetClock(RemoteCluster), Is.EqualTo(Hlc(50)));
        });
    }

    [Test]
    public async Task AdvanceForeign_and_AdvanceLocal_coexist_in_same_tree_vector()
    {
        var (cache, _, _) = CreateCache();
        await cache.GetSnapshotAsync(TreeId);

        cache.AdvanceLocal(TreeId, LocalCluster, Hlc(10));
        cache.AdvanceForeign(TreeId, RemoteCluster, Hlc(20));
        cache.AdvanceForeign(TreeId, "site-c", Hlc(30));

        var snapshot = await cache.GetSnapshotAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Entries, Has.Count.EqualTo(3));
            Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(Hlc(10)));
            Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(Hlc(20)));
            Assert.That(snapshot.GetClock("site-c"), Is.EqualTo(Hlc(30)));
        });
    }

    [Test]
    public void AdvanceForeign_throws_on_null_or_empty_args()
    {
        var (cache, _, _) = CreateCache();

        Assert.Multiple(() =>
        {
            Assert.That(() => cache.AdvanceForeign(null!, RemoteCluster, Hlc(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => cache.AdvanceForeign(string.Empty, RemoteCluster, Hlc(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => cache.AdvanceForeign(TreeId, null!, Hlc(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => cache.AdvanceForeign(TreeId, string.Empty, Hlc(1)), Throws.InstanceOf<ArgumentException>());
        });
    }

    // ------------------------------------------------------------------
    // Concurrent advance correctness
    // ------------------------------------------------------------------

    [Test]
    public async Task Concurrent_advances_produce_pointwise_max()
    {
        // Multiple threads racing to advance the same origin diagonal
        // must converge to the maximum HLC across every advance -
        // pointwise-max under the per-tree lock, not last-writer-wins.
        var (cache, _, _) = CreateCache();
        await cache.GetSnapshotAsync(TreeId);

        const int iterations = 200;
        const int threadCount = 8;
        var tasks = new Task[threadCount];
        for (var t = 0; t < tasks.Length; t++)
        {
            var threadIndex = t;
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var ticks = (threadIndex + 1) * 1000L + i;
                    cache.AdvanceLocal(TreeId, LocalCluster, Hlc(ticks));
                }
            });
        }

        await Task.WhenAll(tasks);
        var snapshot = await cache.GetSnapshotAsync(TreeId);
        var expectedMax = Hlc(threadCount * 1000L + (iterations - 1));
        Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(expectedMax),
            "Pointwise-max under contention must converge to the highest HLC across every advance.");
    }

    [Test]
    public async Task GetSnapshotAsync_cancelled_token_throws_without_cold_start_rpc()
    {
        // The public entry point checks the token before allocating
        // any per-tree state, so a pre-cancelled token must not even
        // touch the grain.
        var (cache, _, grain) = CreateCache();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await cache.GetSnapshotAsync(TreeId, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        await grain.DidNotReceive().GetVectorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetSnapshotAsync_caller_cancellation_does_not_abort_shared_cold_start()
    {
        // The cold-start RPC is single-flight: a cancelled reader
        // must not tear down the underlying grain call (which would
        // force every concurrent waiter to restart). Cancellation
        // is observed via WaitAsync(ct) so the cancelled reader
        // throws OperationCanceledException while the others continue.
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(grain);
        var gate = new TaskCompletionSource<VersionVector>();
        grain.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(gate.Task);
        var cache = new LocalVectorClockCache(factory);

        using var cts = new CancellationTokenSource();
        var cancelled = cache.GetSnapshotAsync(TreeId, cts.Token);
        var survivor = cache.GetSnapshotAsync(TreeId, CancellationToken.None);

        cts.Cancel();

        Assert.That(async () => await cancelled, Throws.InstanceOf<OperationCanceledException>());
        Assert.That(survivor.IsCompleted, Is.False, "Surviving waiter must not be torn down by the cancelled peer.");

        gate.SetResult(new VersionVector());
        var snapshot = await survivor;
        Assert.That(snapshot.Entries, Is.Empty);
        await grain.Received(1).GetVectorAsync(Arg.Any<CancellationToken>());
    }
}

