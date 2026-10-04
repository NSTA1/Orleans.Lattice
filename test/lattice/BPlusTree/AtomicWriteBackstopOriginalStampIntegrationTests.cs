using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;
using System.Diagnostics.Metrics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4522 (PR2a) on a real in-process cluster: the coordinator's
/// committed-values backstop carries each key's original prepare stamp P.
/// <para>
/// The scenario is b0c753b8's FreshStampBackstop trace. A saga prepares its keys
/// on shard 0 and records its commit decision. Its terminal is then held at the
/// coordinator, and a shard split moves some of the keys to a new shard. The
/// split's retroactive sweep installs the decided value there at P. A plain write
/// W, acknowledged on the new shard after the split, is stamped above P. When the
/// terminal is released, the coordinator's broadcast reaches the new shard,
/// because the key's owner drifted, and backstops the key there. At a fresh,
/// dominating stamp that backstop overwrote W, losing an acknowledged write. At P
/// it loses to W under last-writer-wins.
/// </para>
/// <para>
/// The read-back has no fallback: a shard root that reactivated after the
/// prepares is read exhaustively, and a read-back that keeps failing aborts the
/// saga rather than letting it commit without its stamps.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AtomicWriteBackstopOriginalStampIntegrationTests
{
    private const int KeyCount = 32;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        CoordinatorHold.Release();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ResetHooks()
    {
        CoordinatorHold.Release();
        ReadBackHold.Release();
        ReadBackFault.Disarm();
    }

    private async Task<(string Tree, ILattice Router, IShardRootGrain Shard)> CreateTreeAsync(string prefix, int? maxLeafKeys = null)
    {
        var tree = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = maxLeafKeys });
        return (tree, _cluster.GrainFactory.GetGrain<ILattice>(tree),
                _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{tree}/0"));
    }

    private static string Key(int i) => $"k-{i:D2}";

    private static async Task<string?> ReadAsync(ILattice router, string key) =>
        await router.GetAsync(key) is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    private async Task<List<PendingMutationSnapshot>> PendingAsync(IShardRootGrain shard)
    {
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        return await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId)
            .GetPendingMutationsForSlotsAsync(new[] { 0 }, 1);
    }

    /// <summary>
    /// Starts an atomic batch over <see cref="KeyCount"/> keys with the
    /// coordinator's terminal held, and returns once it has decided and reached
    /// the broadcast.
    /// </summary>
    private static Task<Task> StartHeldAtomicBatchAsync(ILattice router) =>
        StartHeldAtomicBatchAsync(router, Enumerable.Range(0, KeyCount).Select(Key).ToList());

    private static async Task<Task> StartHeldAtomicBatchAsync(ILattice router, IReadOnlyList<string> keys)
    {
        CoordinatorHold.Arm();
        var write = router.SetManyAtomicAsync(
            keys.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"saga-{k}"))).ToList());
        var reached = await Task.WhenAny(CoordinatorHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.That(reached, Is.SameAs(CoordinatorHold.Reached),
            "PRECONDITION: the saga's terminal broadcast must have been reached and held");
        return write;
    }

    /// <summary>
    /// Splits shard 0 while the terminal is held, then acknowledges a plain write
    /// on a key the split moved. Returns that key.
    /// </summary>
    private async Task<string> SplitThenWriteLaterAsync(string tree, ILattice router)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{tree}/0");
        await split.SplitAsync(0).WaitAsync(TimeSpan.FromSeconds(60));
        await split.RunSplitPassAsync().WaitAsync(TimeSpan.FromSeconds(60));

        var routing = await router.GetRoutingAsync(forceRefresh: true);
        var moved = Enumerable.Range(0, KeyCount).Select(Key).FirstOrDefault(k => routing.Map.Resolve(k) != 0);
        Assert.That(moved, Is.Not.Null, "PRECONDITION: the split moved at least one of the saga's keys");

        await router.SetAsync(moved!, Encoding.UTF8.GetBytes("later")).WaitAsync(TimeSpan.FromSeconds(30));
        Assert.That(await ReadAsync(router, moved!), Is.EqualTo("later"),
            "PRECONDITION: the write acknowledged after the split is visible before the terminal");
        return moved!;
    }

    [Test]
    public async Task The_shard_reads_back_the_original_stamp_of_every_marked_prepare()
    {
        var (_, router, shard) = await CreateTreeAsync("readback");
        var write = await StartHeldAtomicBatchAsync(router);
        try
        {
            var pending = await PendingAsync(shard);
            var tx = pending.Select(p => p.TransactionId).Distinct().Single();

            var expected = pending.ToDictionary(p => p.Key, p => (HybridLogicalClock?)p.Timestamp);
            var fast = await shard.GetOriginalPrepareStampsAsync(tx, exhaustive: false);
            var exhaustive = await shard.GetOriginalPrepareStampsAsync(tx, exhaustive: true);
            var unknown = await shard.GetOriginalPrepareStampsAsync(Guid.NewGuid(), exhaustive: true);

            Assert.That(pending, Has.Count.EqualTo(KeyCount));
            Assert.That(pending.All(p => p.StampIsOriginal), Is.True, "PRECONDITION: every prepare is marked");
            Assert.That(fast, Is.EquivalentTo(expected),
                "every prepare the routing tier dispatched here is reported at its bucketed stamp");
            Assert.That(exhaustive, Is.EquivalentTo(expected), "the exhaustive pass reads the same buckets");
            Assert.That(unknown, Is.Empty, "a saga that never prepared here has no bucket on the shard");
        }
        finally
        {
            CoordinatorHold.Release();
            await write;
        }
    }

    [Test]
    public async Task A_backstop_after_a_post_decision_split_never_overwrites_a_later_write()
    {
        var (tree, router, _) = await CreateTreeAsync("backstop-split");
        var write = await StartHeldAtomicBatchAsync(router);
        string moved;
        try
        {
            moved = await SplitThenWriteLaterAsync(tree, router);
        }
        finally
        {
            CoordinatorHold.Release();
        }

        await write.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(await ReadAsync(router, moved), Is.EqualTo("later"),
            "the coordinator's backstop must apply the saga's value at its prepare stamp, below the later write");
        for (var i = 0; i < KeyCount; i++)
        {
            if (Key(i) == moved) continue;
            Assert.That(await ReadAsync(router, Key(i)), Is.EqualTo($"saga-{Key(i)}"), Key(i));
        }
    }

    /// <summary>
    /// The leaf-level form of the trace, and the one where the coordinator's
    /// backstop is the decisive delivery. A saga prepares <c>m</c> on leaf L and
    /// decides. With its terminal held, plain writes below <c>m</c> split L, so
    /// <c>m</c>'s range moves to a new sibling while its bucket stays stranded on
    /// L, and a write W of <c>m</c> lands on the sibling above P (the sibling
    /// inherits L's clock). On release the shard routes the coordinator's
    /// backstop for <c>m</c> to the sibling, which holds no bucket and no record
    /// of the saga's terminal: at a fresh stamp it overwrote W.
    /// </summary>
    /// <param name="prefix">The tree-name prefix.</param>
    /// <param name="reactivateShardBeforeReadBack">
    /// Holds the coordinator's read-back and reactivates the shard root first,
    /// so the shard no longer records which leaves the prepares reached and only
    /// the exhaustive pass can find the stamps.
    /// </param>
    private async Task<(string Tree, ILattice Router, Task Write)> LeafSplitThenWriteLaterAsync(string prefix, bool reactivateShardBeforeReadBack = false)
    {
        var (tree, router, shard) = await CreateTreeAsync(prefix, maxLeafKeys: 4);
        if (reactivateShardBeforeReadBack)
            ReadBackHold.Arm();
        CoordinatorHold.Arm();
        var write = router.SetManyAtomicAsync(
            new[] { "m", "z" }.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"saga-{k}"))).ToList());
        try
        {
            if (reactivateShardBeforeReadBack)
            {
                var atReadBack = await Task.WhenAny(ReadBackHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.That(atReadBack, Is.SameAs(ReadBackHold.Reached), "PRECONDITION: the read-back was reached and held");
                await shard.ForceDeactivateAsync();
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                ReadBackHold.Release();
            }

            var reached = await Task.WhenAny(CoordinatorHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.That(reached, Is.SameAs(CoordinatorHold.Reached),
                "PRECONDITION: the saga's terminal broadcast must have been reached and held");

            for (var i = 0; i < 12; i++)
                await router.SetAsync($"a-{i:D2}", Encoding.UTF8.GetBytes("filler")).WaitAsync(TimeSpan.FromSeconds(30));
            await router.SetAsync("m", Encoding.UTF8.GetBytes("later")).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.That(await ReadAsync(router, "m"), Is.EqualTo("later"),
                "PRECONDITION: the write acknowledged after the split is visible before the terminal");
        }
        catch
        {
            ReadBackHold.Release();
            CoordinatorHold.Release();
            throw;
        }

        return (tree, router, write);
    }

    [Test]
    public async Task A_backstop_after_a_post_decision_leaf_split_never_overwrites_a_later_write()
    {
        var (_, router, write) = await LeafSplitThenWriteLaterAsync("backstop-leaf-split");

        CoordinatorHold.Release();
        await write.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(await ReadAsync(router, "m"), Is.EqualTo("later"),
            "the coordinator's backstop must apply the saga's value at its prepare stamp, below the later write");
        Assert.That(await ReadAsync(router, "z"), Is.EqualTo("saga-z"));
    }

    [Test]
    public async Task A_shard_root_reactivated_before_the_read_back_still_yields_every_stamp()
    {
        // The reactivated shard root has lost its in-memory record of the
        // leaves the prepares reached. The exhaustive pass reads its whole
        // chain, whose buckets are replayed from the log, so P is still found
        // and the backstop still never overwrites the later write.
        var recorded = new List<(string? Tree, string? Reason)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.AtomicWritePrepareStampReadBackSlowPath,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? tree = null, reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                    else if (tag.Key == LatticeMetrics.TagReason) reason = tag.Value as string;
                }

                lock (recorded) recorded.Add((tree, reason));
            }));
        var (tree, router, write) = await LeafSplitThenWriteLaterAsync("backstop-reactivated", reactivateShardBeforeReadBack: true);

        CoordinatorHold.Release();
        await write.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(await ReadAsync(router, "m"), Is.EqualTo("later"));
        Assert.That(await ReadAsync(router, "z"), Is.EqualTo("saga-z"));
        lock (recorded)
        {
            Assert.That(recorded.Where(r => r.Tree == tree).Select(r => r.Reason), Is.EqualTo(new[] { LatticeMetrics.PrepareStampReadBackExhaustive }),
                "the exhaustive pass found every stamp itself: no incomplete read, no batch retry");
        }
    }

    [Test]
    public async Task A_shard_split_before_the_read_back_still_yields_every_stamp()
    {
        // A shard split between the prepares and the read-back moves some of the
        // buckets to a new shard. The read-back covers every shard a split of
        // the touched shards leads to, so it still accounts for every key - with
        // no incomplete read and no batch retry - and a write acknowledged on a
        // moved key after the decision survives the terminal.
        var recorded = new List<(string? Tree, string? Reason)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.AtomicWritePrepareStampReadBackSlowPath,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? tree = null, reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                    else if (tag.Key == LatticeMetrics.TagReason) reason = tag.Value as string;
                }

                lock (recorded) recorded.Add((tree, reason));
            }));
        var (tree, router, _) = await CreateTreeAsync("readback-split");
        ReadBackHold.Arm();
        CoordinatorHold.Arm();
        var write = router.SetManyAtomicAsync(
            Enumerable.Range(0, KeyCount)
                .Select(i => new KeyValuePair<string, byte[]>(Key(i), Encoding.UTF8.GetBytes($"saga-{Key(i)}")))
                .ToList());
        string moved;
        try
        {
            var atReadBack = await Task.WhenAny(ReadBackHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(atReadBack, Is.SameAs(ReadBackHold.Reached), "PRECONDITION: the read-back was reached and held");
            var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{tree}/0");
            await split.SplitAsync(0).WaitAsync(TimeSpan.FromSeconds(60));
            await split.RunSplitPassAsync().WaitAsync(TimeSpan.FromSeconds(60));
            ReadBackHold.Release();

            var reached = await Task.WhenAny(CoordinatorHold.Reached, write, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.That(reached, Is.SameAs(CoordinatorHold.Reached), "PRECONDITION: the saga decided and reached its broadcast");

            var routing = await router.GetRoutingAsync(forceRefresh: true);
            moved = Enumerable.Range(0, KeyCount).Select(Key).First(k => routing.Map.Resolve(k) != 0);
            await router.SetAsync(moved, Encoding.UTF8.GetBytes("later")).WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            ReadBackHold.Release();
            CoordinatorHold.Release();
        }

        await write.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.That(await ReadAsync(router, moved), Is.EqualTo("later"));
        for (var i = 0; i < KeyCount; i++)
        {
            if (Key(i) == moved) continue;
            Assert.That(await ReadAsync(router, Key(i)), Is.EqualTo($"saga-{Key(i)}"), Key(i));
        }

        lock (recorded)
        {
            Assert.That(recorded.Where(r => r.Tree == tree).Select(r => r.Reason),
                Has.None.EqualTo(LatticeMetrics.PrepareStampReadBackIncomplete).And.None.EqualTo(LatticeMetrics.PrepareStampReadBackFailed),
                "the read-back found every stamp without failing the batch");
        }
    }

    [Test]
    public async Task A_saga_whose_read_back_keeps_failing_aborts_atomically()
    {
        // No fallback to a dominating stamp: a read-back that never succeeds
        // fails the batch until the retries are spent, and the saga aborts.
        var (_, router, _) = await CreateTreeAsync("readback-abort");
        ReadBackFault.Arm();

        Assert.CatchAsync(async () => await router.SetManyAtomicAsync(
            new[] { "m", "z" }.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"saga-{k}"))).ToList())
            .WaitAsync(TimeSpan.FromSeconds(120)));

        Assert.That(ReadBackFault.Faulted, Is.GreaterThan(0), "PRECONDITION: the read-back was refused");
        Assert.That(await ReadAsync(router, "m"), Is.Null);
        Assert.That(await ReadAsync(router, "z"), Is.Null);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, HookFilter>();
        }
    }

    /// <summary>
    /// Holds the atomic-write coordinator's <c>AppendTxTerminalAsync</c> calls
    /// while armed. The split's sweep issues its own terminal calls from a shard
    /// root, which are not held. Static because the TestingHost silo runs
    /// in-process.
    /// </summary>
    private static class CoordinatorHold
    {
        private static TaskCompletionSource _reached = NewSource();
        private static TaskCompletionSource _released = NewSource();
        private static int _armed;

        private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static Task Reached => _reached.Task;

        internal static void Arm()
        {
            _reached = NewSource();
            _released = NewSource();
            Volatile.Write(ref _armed, 1);
        }

        internal static void Release()
        {
            Volatile.Write(ref _armed, 0);
            _released.TrySetResult();
        }

        internal static Task WaitIfArmedAsync()
        {
            if (Volatile.Read(ref _armed) == 0)
                return Task.CompletedTask;
            _reached.TrySetResult();
            return _released.Task;
        }
    }

    /// <summary>Holds the coordinator's first read-back while armed.</summary>
    private static class ReadBackHold
    {
        private static TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private static TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private static int _armed;

        internal static Task Reached => _reached.Task;

        internal static void Arm()
        {
            _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _armed, 1);
        }

        internal static void Release()
        {
            Volatile.Write(ref _armed, 0);
            _released.TrySetResult();
        }

        internal static Task WaitIfArmedAsync()
        {
            if (Volatile.Read(ref _armed) == 0)
                return Task.CompletedTask;
            _reached.TrySetResult();
            return _released.Task;
        }
    }

    /// <summary>Refuses the coordinator's read-back while armed.</summary>
    private static class ReadBackFault
    {
        private static int _armed;
        private static int _faulted;

        internal static int Faulted => Volatile.Read(ref _faulted);

        internal static void Arm()
        {
            Volatile.Write(ref _faulted, 0);
            Volatile.Write(ref _armed, 1);
        }

        internal static void Disarm() => Volatile.Write(ref _armed, 0);

        internal static bool TryFault()
        {
            if (Volatile.Read(ref _armed) == 0) return false;
            Interlocked.Increment(ref _faulted);
            return true;
        }
    }

    private sealed class HookFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            var fromCoordinator = context.SourceId is { } source
                && source.Type.ToString().Contains("atomicwrite", StringComparison.OrdinalIgnoreCase);
            if (fromCoordinator)
            {
                switch (context.InterfaceMethod?.Name)
                {
                    case "AppendTxTerminalAsync":
                        await CoordinatorHold.WaitIfArmedAsync();
                        break;
                    case "GetOriginalPrepareStampsAsync":
                        if (ReadBackFault.TryFault())
                            throw new TimeoutException("test: read-back refused");
                        await ReadBackHold.WaitIfArmedAsync();
                        break;
                }
            }

            await context.Invoke();
        }
    }
}
