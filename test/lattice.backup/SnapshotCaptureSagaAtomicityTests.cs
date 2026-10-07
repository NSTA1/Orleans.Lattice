using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression coverage for issue #4485: a full backup (and every snapshot
/// capture it is built on) must never hold an atomic batch torn - one key at its
/// post-saga value and another at its pre-saga value - whichever way the
/// capture interleaves with the saga's per-shard terminal broadcast.
/// <list type="number">
/// <item><description>
/// <b>Mechanism 1 (pending resolved pre-saga).</b> The capture runs while one
/// shard's terminal has landed and the other's is held: the decision is
/// recorded, one shard has applied the commit, the other still holds the
/// prepared bucket.
/// </description></item>
/// <item><description>
/// <b>Mechanism 2 (shards captured apart).</b> One shard is captured before the
/// saga prepares there, and the other is held until the saga has had every
/// chance to commit and broadcast.
/// </description></item>
/// </list>
/// The second also shows the cost model: plain writes are not blocked while a
/// capture holds the saga decision gate, and a saga whose decision fell inside
/// the capture completes once the capture releases the gate.
/// </summary>
[Category("Integration")]
public sealed class SnapshotCaptureSagaAtomicityTests
{
    private const string TreeId = "torn-capture";

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private ILatticeBackupCaptureService Capture => SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        CaptureSagaCallGate.Reset();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [SetUp]
    public void SetUp() => CaptureSagaCallGate.Reset();

    [TearDown]
    public void TearDown() => CaptureSagaCallGate.Reset();

    [Test]
    public async Task Capture_during_a_half_broadcast_saga_holds_the_batch_on_one_side()
    {
        var treeId = TreeId + "-m1";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (routing, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));

        var hold = CaptureSagaCallGate.ArmTerminalHold(
            heldShardKey: ShardKey(routing, k1),
            freeShardKey: ShardKey(routing, k0));

        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(hold.Entered.Task, "the held shard's terminal never reached the filter");
        await WithTimeout(hold.FreeDone.Task, "the free shard's terminal never returned");

        // The capture must complete while the held terminal is still parked:
        // capturing never waits on a saga's broadcast.
        var backup = await WithTimeout(
            Capture.CaptureAsync(new LatticeBackupCaptureRequest("torn-m1", BackupScopeSelector.WholeTree(treeId))),
            "the capture blocked on a parked saga terminal");
        var entries = await DecodeAsync(backup.Manifest);

        hold.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed after its terminal was released");

        var v0 = ValueOf(entries, k0);
        var v1 = ValueOf(entries, k1);
        Assert.Multiple(() =>
        {
            Assert.That(v1, Is.EqualTo(v0), $"captured {k0}={v0} {k1}={v1}: the atomic batch is torn across the capture");
            Assert.That(v0, Is.EqualTo("post"), "the saga was decided before the capture, so the capture holds it post-saga");
        });
    }

    [Test]
    public async Task Shards_captured_apart_hold_the_batch_on_one_side_and_do_not_block_writes()
    {
        var treeId = TreeId + "-m2";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (routing, a, b) = await PickKeysOnDifferentShardsAsync(tree);
        // Capture concurrency is 1, so shards are captured in ascending index
        // order: hold the higher-indexed shard and let the lower one go first.
        if (routing.Map.Resolve(a) > routing.Map.Resolve(b))
            (a, b) = (b, a);
        await tree.SetAsync(a, Bytes("pre"));
        await tree.SetAsync(b, Bytes("pre"));

        var first = CaptureSagaCallGate.ArmCaptureObserved(ShardKey(routing, a));
        var held = CaptureSagaCallGate.ArmCaptureHold(ShardKey(routing, b));

        var capture = Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("torn-m2", BackupScopeSelector.WholeTree(treeId)));
        await WithTimeout(first.Task, "the first shard was never captured");
        await WithTimeout(held.Entered.Task, "the second shard's capture never reached the filter");

        // The first shard is captured, before the saga prepared there. Run the
        // saga now and give it every chance to commit and broadcast to both
        // shards before the second shard is captured.
        var saga = tree.SetManyAtomicAsync([new(a, Bytes("post")), new(b, Bytes("post"))]);
        var sagaDuringCapture = await Task.WhenAny(saga, Task.Delay(TimeSpan.FromSeconds(3))) == saga;

        // Writes are never blocked by a capture: a plain write issued while the
        // capture holds the saga decision gate completes.
        var write = Stopwatch.StartNew();
        await WithTimeout(tree.SetAsync("free-write", Bytes("w")), "a plain write blocked behind the capture");
        write.Stop();

        var stall = Stopwatch.StartNew();
        held.Release.TrySetResult();
        var backup = await WithTimeout(capture, "the capture never completed");
        await WithTimeout(saga, "the saga never completed after the capture released the gate");
        stall.Stop();

        var entries = await DecodeAsync(backup.Manifest);
        var va = ValueOf(entries, a);
        var vb = ValueOf(entries, b);
        TestContext.Out.WriteLine(
            $"saga completed during capture: {sagaDuringCapture}; plain write mid-capture: {write.ElapsedMilliseconds}ms; " +
            $"saga completion after gate release: {stall.ElapsedMilliseconds}ms; captured {a}={va} {b}={vb}");
        Assert.Multiple(() =>
        {
            Assert.That(vb, Is.EqualTo(va), $"captured {a}={va} {b}={vb}: the atomic batch is torn across the capture");
            Assert.That(sagaDuringCapture, Is.False, "the saga's decision must wait for the capture to release the gate");
            Assert.That(ValueOf(entries, "free-write"), Is.Null.Or.EqualTo("w"));
        });
        Assert.That(Str(await tree.GetAsync(a)), Is.EqualTo("post"));
        Assert.That(Str(await tree.GetAsync(b)), Is.EqualTo("post"));
    }

    [Test]
    public async Task Backup_set_holds_a_cross_tree_batch_on_one_side_while_its_terminal_broadcast_straddles_the_set()
    {
        var treeA = TreeId + "-set-a";
        var treeB = TreeId + "-set-b";
        const string key = "x";
        var a = _cluster.GrainFactory.GetGrain<ILattice>(treeA);
        var b = _cluster.GrainFactory.GetGrain<ILattice>(treeB);
        await a.SetAsync(key, Bytes("pre"));
        await b.SetAsync(key, Bytes("pre"));
        var routingA = await a.GetRoutingAsync(forceRefresh: true);
        var routingB = await b.GetRoutingAsync(forceRefresh: true);

        // Tree A's terminal lands; tree B's is parked after B recorded its local
        // decision. The set's drain gate passes (no delegation is in flight on
        // either tree), so before the fix A was captured post-saga and B pre-saga.
        var hold = CaptureSagaCallGate.ArmTerminalHold(
            heldShardKey: ShardKey(routingB, key),
            freeShardKey: ShardKey(routingA, key));

        var saga = _cluster.GrainFactory.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new(key, Bytes("post"))]),
                new LatticeTreeBatch(treeB, [new(key, Bytes("post"))]),
            ],
            $"xtx-4485-{Guid.NewGuid():N}");
        await WithTimeout(hold.Entered.Task, "tree B's terminal never reached the filter");
        await WithTimeout(hold.FreeDone.Task, "tree A's terminal never returned");

        var set = await WithTimeout(
            Capture.CaptureSetAsync(new LatticeBackupSetCaptureRequest(
                "torn-set",
                [BackupScopeSelector.WholeTree(treeA), BackupScopeSelector.WholeTree(treeB)])
            {
                CrossTreeConsistent = true,
            }),
            "the set capture blocked on a parked saga terminal");

        hold.Release.TrySetResult();
        await WithTimeout(saga, "the cross-tree saga never completed after its terminal was released");

        var memberA = set.Members.Single(m => m.Manifest.Scope.TreeId == treeA);
        var memberB = set.Members.Single(m => m.Manifest.Scope.TreeId == treeB);
        var va = ValueOf(await DecodeAsync(memberA.Manifest), key);
        var vb = ValueOf(await DecodeAsync(memberB.Manifest), key);
        Assert.Multiple(() =>
        {
            Assert.That(vb, Is.EqualTo(va), $"captured {treeA}:{key}={va} {treeB}:{key}={vb}: the cross-tree batch is torn across the set");
            Assert.That(va, Is.EqualTo("post"), "the batch was finalized on both trees before the capture");
        });
    }

    [Test]
    public async Task A_saga_decided_during_a_capture_waits_no_longer_than_the_capture()
    {
        // The cost model of the decision gate: writes are never blocked, and a
        // saga whose decision falls inside a capture waits at most for the
        // capture. Measured on a tree with a few thousand keys spread over every
        // shard, so the capture fan-out does real work.
        var treeId = TreeId + "-stall";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        const int keyCount = 3_000;
        var seed = new List<KeyValuePair<string, byte[]>>(keyCount);
        for (var i = 0; i < keyCount; i++)
            seed.Add(new($"seed-{i:D5}", Bytes(new string('v', 64))));
        await tree.SetManyAsync(seed);

        var captureClock = Stopwatch.StartNew();
        var capture = Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("stall", BackupScopeSelector.WholeTree(treeId)));
        var sagaClock = Stopwatch.StartNew();
        await tree.SetManyAtomicAsync([new("stall-a", Bytes("1")), new("stall-b", Bytes("1"))]);
        sagaClock.Stop();
        await capture;
        captureClock.Stop();

        TestContext.Out.WriteLine(
            $"{keyCount} keys: capture {captureClock.ElapsedMilliseconds}ms end to end; " +
            $"saga issued during it completed in {sagaClock.ElapsedMilliseconds}ms");
        Assert.That(
            sagaClock.Elapsed,
            Is.LessThanOrEqualTo(captureClock.Elapsed + TimeSpan.FromSeconds(5)),
            "a saga's decision waits for at most the capture that holds the gate");
    }

    // ---- Helpers --------------------------------------------------------

    private static async Task<(RoutingInfo Routing, string K0, string K1)> PickKeysOnDifferentShardsAsync(ILattice tree)
    {
        // Seed one write so the tree exists, then read its authoritative map.
        await tree.SetAsync("seed", Bytes("seed"));
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        var first = "key-000";
        var firstShard = routing.Map.Resolve(first);
        for (var i = 1; i < 10_000; i++)
        {
            var candidate = $"key-{i:D3}";
            if (routing.Map.Resolve(candidate) != firstShard)
                return (routing, first, candidate);
        }

        throw new InvalidOperationException("Every candidate key resolved to one physical shard; the test needs at least two.");
    }

    private static string ShardKey(RoutingInfo routing, string key) =>
        $"{routing.PhysicalTreeId}/{routing.Map.Resolve(key)}";

    private async Task<List<LwwEntry>> DecodeAsync(BackupManifest manifest)
    {
        var sink = SiloServices.GetRequiredService<ILatticeBackupSink>();
        var serializer = SiloServices.GetRequiredService<Serializer>();
        var all = new List<LwwEntry>();
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            await foreach (var chunk in sink.ReadArtifactAsync(descriptor.ArtifactId))
            {
                all.AddRange(serializer.Deserialize<LwwEntry[]>(chunk));
            }
        }

        return all;
    }

    private static string? ValueOf(List<LwwEntry> entries, string key) =>
        entries.SingleOrDefault(e => e.Key == key) is { } entry && !entry.IsTombstone ? Str(entry.Value) : null;

    private static async Task WithTimeout(Task task, string because)
    {
        if (await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60))) != task)
            Assert.Fail(because);
        await task;
    }

    private static async Task<T> WithTimeout<T>(Task<T> task, string because)
    {
        await WithTimeout((Task)task, because);
        return await task;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string? Str(byte[]? b) => b is null ? null : Encoding.UTF8.GetString(b);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.MaxConcurrentSnapshotCaptures = 1);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup();
            siloBuilder.AddOutgoingGrainCallFilter<CaptureSagaCallGate>();
        }
    }

    /// <summary>
    /// Silo-side outgoing call filter that parks a chosen shard's saga terminal
    /// append, or a chosen shard's snapshot baseline capture, until the test
    /// releases it.
    /// </summary>
    internal sealed class CaptureSagaCallGate : IOutgoingGrainCallFilter
    {
        private static volatile TerminalHold? s_terminal;
        private static readonly ConcurrentDictionary<string, Hold> s_captureHolds = new(StringComparer.Ordinal);
        private static readonly ConcurrentDictionary<string, TaskCompletionSource> s_captureObserved = new(StringComparer.Ordinal);

        internal static TerminalHold ArmTerminalHold(string heldShardKey, string freeShardKey) =>
            s_terminal = new TerminalHold(heldShardKey, freeShardKey);

        internal static Hold ArmCaptureHold(string shardKey) => s_captureHolds.GetOrAdd(shardKey, static _ => new Hold());

        internal static TaskCompletionSource ArmCaptureObserved(string shardKey) =>
            s_captureObserved.GetOrAdd(shardKey, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        internal static void Reset()
        {
            s_terminal?.Release.TrySetResult();
            s_terminal = null;
            foreach (var hold in s_captureHolds.Values) hold.Release.TrySetResult();
            s_captureHolds.Clear();
            s_captureObserved.Clear();
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            var target = context.TargetId.Key.ToString() ?? string.Empty;
            var method = context.MethodName;

            if (method == nameof(IShardRootGrain.AppendTxTerminalAsync) && s_terminal is { } terminal)
            {
                if (target == terminal.HeldShardKey)
                {
                    terminal.Entered.TrySetResult();
                    await terminal.Release.Task;
                }
                else if (target == terminal.FreeShardKey)
                {
                    await context.Invoke();
                    terminal.FreeDone.TrySetResult();
                    return;
                }
            }

            var isCapture = method is "CaptureSnapshotBaselineAsync" or "CaptureGatedSnapshotBaselineAsync";
            if (isCapture && s_captureHolds.TryGetValue(target, out var hold))
            {
                hold.Entered.TrySetResult();
                await hold.Release.Task;
            }

            await context.Invoke();

            if (isCapture && s_captureObserved.TryGetValue(target, out var observed))
                observed.TrySetResult();
        }
    }

    internal sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    internal sealed class TerminalHold(string heldShardKey, string freeShardKey)
    {
        public string HeldShardKey { get; } = heldShardKey;

        public string FreeShardKey { get; } = freeShardKey;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource FreeDone { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
