using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Wal;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Detectors for issue #4589 on real grains: an incremental backup must be
/// saga-consistent on the same terms as a full one. An atomic write's prepared
/// writes reach the WAL before its decision, so an increment that copied them as
/// data would restore an aborted or undecided batch's writes, or a batch partially.
/// <para>
/// Every test first proves, by reading the WAL itself, that the saga's prepared
/// writes really are inside the increment's delta window (and, for the boundary
/// case, that the rest of the batch is outside it), so none can pass by the saga
/// simply missing the window. The verdict is read from a real restore of the
/// chain into a fresh tree.
/// </para>
/// </summary>
[Category("Integration")]
public sealed partial class LatticeBackupIncrementalSagaConsistencyTests
{
    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private ILatticeBackupCaptureService Capture => SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

    private ILatticeBackupIncrementalCaptureService Incremental =>
        SiloServices.GetRequiredService<ILatticeBackupIncrementalCaptureService>();

    private ILatticeBackupRestoreService Restore => SiloServices.GetRequiredService<ILatticeBackupRestoreService>();

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
        SagaCallGate.Reset();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [SetUp]
    public void SetUp()
    {
        SagaCallGate.Reset();
        BackupInventoryRegistry.Instance.Reset();
    }

    [TearDown]
    public void TearDown() => SagaCallGate.Reset();

    [Test]
    public async Task An_increment_taken_after_a_saga_aborted_does_not_restore_its_writes()
    {
        var treeId = TreeName("aborted");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (routing, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));
        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));

        // k1's prepare always fails, so the saga aborts after k0's prepare landed.
        SagaCallGate.FailPrepareOn(ShardKey(routing, k1));
        Assert.That(
            async () => await tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]),
            Throws.Exception,
            "the saga must abort");
        SagaCallGate.Reset();

        var wal = await ReadWalAsync(treeId);
        AssertPreparedInWindow(wal, baseBackup.Manifest, k0);

        var increment = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));
        var restored = await RestoreAsync(increment, k0, k1);

        Assert.Multiple(() =>
        {
            Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(restored[k0], Is.EqualTo("pre"), "an aborted saga's prepared write was restored as data");
            Assert.That(restored[k1], Is.EqualTo("pre"));
        });
    }

    [Test]
    public async Task An_increment_taken_while_a_saga_is_in_flight_does_not_restore_its_writes()
    {
        var treeId = TreeName("in-flight");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (_, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));
        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));

        // Both prepares land; the commit decision is held, so the saga is undecided.
        var decision = SagaCallGate.HoldDecision();
        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(decision.Entered.Task, "the saga never reached its commit decision");

        var wal = await ReadWalAsync(treeId);
        AssertPreparedInWindow(wal, baseBackup.Manifest, k0);
        AssertPreparedInWindow(wal, baseBackup.Manifest, k1);

        var increment = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));

        decision.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed once its decision was released");

        var restored = await RestoreAsync(increment, k0, k1);
        Assert.Multiple(() =>
        {
            Assert.That(increment.Manifest.Kind, Is.EqualTo(BackupKind.Incremental));
            Assert.That(restored[k0], Is.EqualTo("pre"), "an undecided saga's prepared write was restored as data");
            Assert.That(restored[k1], Is.EqualTo("pre"), "an undecided saga's prepared write was restored as data");
        });
    }

    [Test]
    public async Task A_saga_committed_across_the_base_frontier_is_restored_whole()
    {
        var treeId = TreeName("straddle");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (routing, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));

        // k0's prepare lands; k1's is parked. The full base is taken in between, so
        // the batch straddles the base's frontier.
        var k0Prepared = SagaCallGate.ObservePrepareOn(ShardKey(routing, k0));
        var k1Prepare = SagaCallGate.HoldPrepareOn(ShardKey(routing, k1));
        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(k0Prepared.Task, "k0's prepare never completed");
        await WithTimeout(k1Prepare.Entered.Task, "k1's prepare never reached the filter");

        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));

        k1Prepare.Release.TrySetResult();
        await WithTimeout(saga, "the saga never committed");

        var wal = await ReadWalAsync(treeId);
        AssertPreparedBeforeWindow(wal, baseBackup.Manifest, k0);
        AssertPreparedInWindow(wal, baseBackup.Manifest, k1);

        var increment = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));
        var restored = await RestoreAsync(increment, k0, k1);

        Assert.Multiple(() =>
        {
            Assert.That(restored[k1], Is.EqualTo(restored[k0]),
                $"restored {k0}={restored[k0]} {k1}={restored[k1]}: the committed batch is torn across the chain");
            Assert.That(restored[k0], Is.EqualTo("post"), "the committed saga must be restored whole");
        });
    }

    [Test]
    public async Task A_saga_undecided_at_the_base_and_committed_with_no_record_in_the_window_is_restored_whole()
    {
        var treeId = TreeName("unseen");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (_, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));

        // Both prepares land, then the decision is held: the full base finds the saga
        // pending and undecided, with every prepare before its frontier.
        var decision = SagaCallGate.HoldDecision();
        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(decision.Entered.Task, "the saga never reached its commit decision");
        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));
        Assert.That(baseBackup.Manifest.ConsistencyCut.UndecidedSagaIds, Has.Count.EqualTo(1),
            "the full base must record the saga it held pre-saga because it was undecided");

        // The saga commits, but no terminal reaches the WAL before the increment.
        var terminals = SagaCallGate.HoldTerminals();
        decision.Release.TrySetResult();
        await WithTimeout(terminals.Entered.Task, "the saga never committed");

        var wal = await ReadWalAsync(treeId);
        AssertPreparedBeforeWindow(wal, baseBackup.Manifest, k0);
        AssertPreparedBeforeWindow(wal, baseBackup.Manifest, k1);
        var resume = baseBackup.Manifest.ConsistencyCut.WalPartitionOffsets!;
        Assert.That(
            wal.Where(e => e.Offset >= resume.GetValueOrDefault(e.Partition, 0L))
                .Any(e => e.IsPrepared || e.Mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort),
            Is.False,
            "none of the saga's records may be inside the increment's delta window");

        var increment = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));
        terminals.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed once its terminals were released");

        var restored = await RestoreAsync(increment, k0, k1);
        Assert.Multiple(() =>
        {
            Assert.That(restored[k0], Is.EqualTo("post"), "the saga committed before the increment, so it must be restored");
            Assert.That(restored[k1], Is.EqualTo("post"), "the saga committed before the increment, so it must be restored");
        });
    }

    [Test]
    public async Task A_saga_undecided_across_an_increment_is_handed_on_and_restored_whole_once_it_commits()
    {
        var treeId = TreeName("handed-on");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (_, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));

        var decision = SagaCallGate.HoldDecision();
        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(decision.Entered.Task, "the saga never reached its commit decision");
        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));

        // Still undecided at the first increment, which has none of its records in
        // its window: the increment must hand the saga on.
        var first = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-1", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));
        Assert.That(first.Manifest.ConsistencyCut.UndecidedSagaIds, Is.EquivalentTo(baseBackup.Manifest.ConsistencyCut.UndecidedSagaIds!),
            "the first increment hands the still-undecided saga on");

        var terminals = SagaCallGate.HoldTerminals();
        decision.Release.TrySetResult();
        await WithTimeout(terminals.Entered.Task, "the saga never committed");
        var second = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-2", BackupScopeSelector.WholeTree(treeId), first.BackupId));
        terminals.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed once its terminals were released");

        var restored = await RestoreAsync(second, k0, k1);
        Assert.Multiple(() =>
        {
            Assert.That(restored[k0], Is.EqualTo("post"), "the saga committed before the second increment, so it must be restored");
            Assert.That(restored[k1], Is.EqualTo("post"), "the saga committed before the second increment, so it must be restored");
        });
    }

    [Test]
    public async Task A_saga_left_out_while_undecided_is_restored_whole_by_the_next_increment()
    {
        var treeId = TreeName("held-back");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var (_, k0, k1) = await PickKeysOnDifferentShardsAsync(tree);
        await tree.SetAsync(k0, Bytes("pre"));
        await tree.SetAsync(k1, Bytes("pre"));
        var baseBackup = await Capture.CaptureAsync(new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(treeId)));

        var decision = SagaCallGate.HoldDecision();
        var saga = tree.SetManyAtomicAsync([new(k0, Bytes("post")), new(k1, Bytes("post"))]);
        await WithTimeout(decision.Entered.Task, "the saga never reached its commit decision");
        var first = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-1", BackupScopeSelector.WholeTree(treeId), baseBackup.BackupId));
        Assert.That(await BlockedFloorAsync(treeId), Is.Not.Null,
            "the left-out saga's prepares must stay pinned in the WAL for the next increment");

        decision.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed once its decision was released");
        var second = await Incremental.CaptureIncrementalAsync(
            new LatticeBackupIncrementalCaptureRequest("inc-2", BackupScopeSelector.WholeTree(treeId), first.BackupId));
        var restored = await RestoreAsync(second, k0, k1);
        var floorAfter = await BlockedFloorAsync(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(floorAfter, Is.Null, "once the saga is captured nothing needs the WAL pinned for it");
            Assert.That(second.Manifest.Kind, Is.EqualTo(BackupKind.Incremental),
                "the held-back saga is read again by the next increment, which needs no full fallback");
            Assert.That(restored[k0], Is.EqualTo("post"), "the saga committed before the second increment");
            Assert.That(restored[k1], Is.EqualTo("post"), "the saga committed before the second increment");
        });
    }

    // ---- Helpers --------------------------------------------------------

    private static string TreeName(string kind) => $"inc-saga-{kind}-{Guid.NewGuid():N}";

    private async Task<Dictionary<string, string?>> RestoreAsync(LatticeBackupCaptureResult backup, params string[] keys)
    {
        var target = $"{backup.Manifest.Scope.TreeId}-restored";
        await Restore.RestoreAsync(new LatticeRestoreRequest(backup.BackupId, targetTreeId: target));
        var restored = _cluster.GrainFactory.GetGrain<ILattice>(target);
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            var value = await restored.GetAsync(key);
            values[key] = value is null ? null : Encoding.UTF8.GetString(value);
        }

        return values;
    }

    private async Task<HybridLogicalClock?> BlockedFloorAsync(string treeId)
    {
        var cursors = await SiloServices.GetRequiredService<IWalCursorRegistry>().SnapshotAsync(treeId);
        return cursors.SingleOrDefault(c => c.ConsumerId == $"backup:{treeId}").BlockedAtHlc;
    }

    private async Task<List<WalSubscriptionEntry>> ReadWalAsync(string treeId)
    {
        var subscriber = SiloServices.GetRequiredService<IWalSubscriber>();
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(treeId);
        var checkpoints = new Dictionary<int, long>();
        for (var p = 0; p < partitions; p++)
            checkpoints[p] = -1;

        var handler = new RecordingHandler();
        while (true)
        {
            var context = new WalSubscriptionContext(treeId, $"probe-{Guid.NewGuid():N}", partitions, checkpoints)
            {
                PinWal = false,
            };
            var result = await subscriber.DrainAsync(context, handler, CancellationToken.None);
            foreach (var (partition, offset) in result.AdvancedOffsets)
                checkpoints[partition] = offset;
            if (result.EntriesRead == 0)
                return handler.Entries;
        }
    }

    private static void AssertPreparedInWindow(List<WalSubscriptionEntry> wal, BackupManifest baseManifest, string key)
    {
        var resume = baseManifest.ConsistencyCut.WalPartitionOffsets!;
        Assert.That(
            wal.Any(e => e.IsPrepared && e.Mutation.Key == key && e.Offset >= resume.GetValueOrDefault(e.Partition, 0L)),
            Is.True,
            $"the saga's prepared write of {key} must be inside the increment's delta window");
    }

    private static void AssertPreparedBeforeWindow(List<WalSubscriptionEntry> wal, BackupManifest baseManifest, string key)
    {
        var resume = baseManifest.ConsistencyCut.WalPartitionOffsets!;
        Assert.That(
            wal.Any(e => e.IsPrepared && e.Mutation.Key == key && e.Offset < resume.GetValueOrDefault(e.Partition, 0L)),
            Is.True,
            $"the saga's prepared write of {key} must precede the base's frontier");
    }

    private static async Task<(RoutingInfo Routing, string K0, string K1)> PickKeysOnDifferentShardsAsync(ILattice tree)
    {
        await tree.SetAsync("seed", Bytes("seed"));
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        const string first = "key-000";
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

    private static async Task WithTimeout(Task task, string because)
    {
        if (await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60))) != task)
            Assert.Fail(because);
        await task;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class RecordingHandler : IWalSubscriptionHandler
    {
        public List<WalSubscriptionEntry> Entries { get; } = new();

        public void OnEntry(in WalSubscriptionEntry entry) => Entries.Add(entry);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup();
            siloBuilder.AddOutgoingGrainCallFilter<SagaCallGate>();
        }
    }

    /// <summary>
    /// Silo-side outgoing filter that fails, parks or observes a shard's prepare
    /// (<see cref="IShardRootGrain.SetManyAsync"/>), parks a saga's commit
    /// decision (<see cref="ITxRegistryGrain.MarkCommittedAsync"/>), or parks its
    /// shard terminals (<see cref="IShardRootGrain.AppendTxTerminalAsync"/>).
    /// </summary>
    internal sealed class SagaCallGate : IOutgoingGrainCallFilter
    {
        private static readonly ConcurrentDictionary<string, Hold> s_prepareHolds = new(StringComparer.Ordinal);
        private static readonly ConcurrentDictionary<string, TaskCompletionSource> s_prepareObserved = new(StringComparer.Ordinal);
        private static volatile string? s_failPrepareOn;
        private static volatile Hold? s_decision;
        private static volatile Hold? s_terminals;

        internal static void FailPrepareOn(string shardKey) => s_failPrepareOn = shardKey;

        internal static Hold HoldPrepareOn(string shardKey) => s_prepareHolds.GetOrAdd(shardKey, static _ => new Hold());

        internal static TaskCompletionSource ObservePrepareOn(string shardKey) =>
            s_prepareObserved.GetOrAdd(shardKey, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        internal static Hold HoldDecision() => s_decision = new Hold();

        internal static Hold HoldTerminals() => s_terminals = new Hold();

        internal static void Reset()
        {
            s_failPrepareOn = null;
            foreach (var hold in s_prepareHolds.Values) hold.Release.TrySetResult();
            s_prepareHolds.Clear();
            s_prepareObserved.Clear();
            s_decision?.Release.TrySetResult();
            s_decision = null;
            s_terminals?.Release.TrySetResult();
            s_terminals = null;
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            var target = context.TargetId.Key.ToString() ?? string.Empty;
            var method = context.MethodName;

            if (method == nameof(IShardRootGrain.SetManyAsync) && context.Request.GetInterfaceType() == typeof(IShardRootGrain))
            {
                if (s_failPrepareOn == target)
                    throw new InvalidOperationException("injected prepare failure (issue #4589 detector)");
                if (s_prepareHolds.TryGetValue(target, out var hold))
                {
                    hold.Entered.TrySetResult();
                    await hold.Release.Task;
                }
            }

            if (method == nameof(IShardRootGrain.AppendTxTerminalAsync) && s_terminals is { } terminals)
            {
                terminals.Entered.TrySetResult();
                await terminals.Release.Task;
            }

            if (method == nameof(ITxRegistryGrain.MarkCommittedAsync) && s_decision is { } decision)
            {
                decision.Entered.TrySetResult();
                await decision.Release.Task;
            }

            await context.Invoke();

            if (method == nameof(IShardRootGrain.SetManyAsync) && s_prepareObserved.TryGetValue(target, out var observed))
                observed.TrySetResult();
        }
    }

    internal sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
