using System.Collections.Concurrent;
using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// An atomic saga bound to the previous copy across a shadow-cutover restore and
/// its revert (spec/shard-ownership/ShardOwnershipCutover.tla). The saga's batch
/// is split deterministically across the swap: an outgoing-call filter holds the
/// routing tier's prepared call to one shard of the previous copy, so the other
/// key's prepare lands on the previous copy before the cutover, and the held one
/// reaches it only once the cutover has armed the retained redirect.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShadowCutoverBoundSagaIntegrationTests
{
    private RestoreClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync(b => b.AddSiloBuilderConfigurator<GateConfigurator>());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        PrepareGate.ReleaseAll();
        await _fixture.DisposeAsync();
    }

    private ILatticeRegistry Registry => _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [Test]
    public async Task A_saga_split_across_a_cutover_rebinds_and_commits_whole_on_the_restored_copy()
    {
        // The first key's prepare lands on the previous copy before the cutover;
        // the second reaches it only after the restore armed the retained
        // redirect, which refuses it. The previous copy mirrors nowhere, so the
        // saga re-binds to the restored copy, re-dispatches the whole batch there
        // and commits it whole on the copy the tree now resolves to.
        var (tree, k1, k2, restore) = await SplitASagaAcrossACutoverAsync();

        var read = await tree.GetManyAsync([k1, k2]);
        Assert.Multiple(() =>
        {
            Assert.That(restore.ShadowPhysicalTreeId, Is.Not.EqualTo(restore.PreviousPhysicalTreeId),
                "precondition: the restore cut the tree over to a new copy");
            Assert.That(Text(read, k1), Is.EqualTo("new"),
                "the saga must commit on the copy the tree resolves to after the cutover");
            Assert.That(Text(read, k2), Is.EqualTo("new"),
                "the saga must commit its whole batch on the restored copy");
        });
    }

    [Test]
    public async Task A_saga_rebound_across_a_cutover_is_whole_after_the_revert()
    {
        // Issue #4689: the saga's first prepare stayed on the previous copy when
        // it re-bound, and the revert makes that copy live again while the
        // decision still reads committed. The saga discards what it left behind
        // before it decides, so the previous copy serves its pre-saga values on
        // both keys - the batch was committed on the restored copy, which the
        // revert discards with everything written there.
        var (tree, k1, k2, restore) = await SplitASagaAcrossACutoverAsync();

        await _fixture.Restore.RevertRestoreAsync(restore);

        var read = await tree.GetManyAsync([k1, k2]);
        var p1 = await tree.GetAsync(k1) is { } r1 ? Encoding.UTF8.GetString(r1) : null;
        var p2 = await tree.GetAsync(k2) is { } r2 ? Encoding.UTF8.GetString(r2) : null;
        Assert.Multiple(() =>
        {
            Assert.That((Text(read, k1), Text(read, k2)), Is.EqualTo(("old", "old")),
                "a multi-key read after the revert must see the batch on neither key");
            Assert.That((p1, p2), Is.EqualTo(("old", "old")),
                "point reads after the revert must see the batch on neither key");
        });
    }

    private static string? Text(Dictionary<string, byte[]> read, string key) =>
        read.TryGetValue(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

    /// <summary>
    /// Writes <c>old</c> to two keys on two shards, captures a backup, then runs a
    /// saga writing <c>new</c> to both while a shadow-cutover restore of that
    /// backup lands between its two prepares, and waits for the saga.
    /// </summary>
    private async Task<(ILattice Tree, string K1, string K2, LatticeRestoreResult Restore)> SplitASagaAcrossACutoverAsync()
    {
        var treeId = $"cutover-saga-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 2 });
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        var (k1, k2, held) = await KeysOnTwoShardsAsync(tree);
        await tree.SetAsync(k1, Encoding.UTF8.GetBytes("old"));
        await tree.SetAsync(k2, Encoding.UTF8.GetBytes("old"));
        var backup = await _fixture.Capture.CaptureAsync(new LatticeBackupCaptureRequest(
            $"capture-{treeId}", BackupScopeSelector.WholeTree(treeId)));

        var hold = PrepareGate.Arm($"{treeId}/{held}");
        Task saga;
        LatticeRestoreResult restore;
        try
        {
            saga = tree.SetManyAtomicAsync(
            [
                new KeyValuePair<string, byte[]>(k1, Encoding.UTF8.GetBytes("new")),
                new KeyValuePair<string, byte[]>(k2, Encoding.UTF8.GetBytes("new")),
            ]);
            await hold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

            restore = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
                backup.BackupId, treeId, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        await saga.WaitAsync(TimeSpan.FromSeconds(60));
        return (tree, k1, k2, restore);
    }
    private static async Task<(string K1, string K2, int HeldShard)> KeysOnTwoShardsAsync(ILattice tree)
    {
        RoutingInfo routing;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            routing = await tree.GetRoutingAsync(forceRefresh: true);
        }

        var k1 = "a-0";
        var s1 = routing.Map.Resolve(k1);
        for (var i = 0; i < 1000; i++)
        {
            var k2 = $"b-{i}";
            var s2 = routing.Map.Resolve(k2);
            if (s2 != s1) return (k1, k2, s2);
        }

        throw new InvalidOperationException("no two keys on different shards");
    }

    /// <summary>
    /// Holds the routing tier's first <see cref="IShardRootGrain.SetManyAsync"/> to an
    /// armed shard until the test releases it.
    /// </summary>
    private sealed class PrepareGate : IOutgoingGrainCallFilter
    {
        private static readonly ConcurrentDictionary<string, Hold> Holds = new(StringComparer.Ordinal);

        internal static Hold Arm(string shardKey) => Holds.GetOrAdd(shardKey, static _ => new Hold());

        internal static void ReleaseAll()
        {
            foreach (var hold in Holds.Values) hold.Release.TrySetResult();
        }

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.MethodName == nameof(IShardRootGrain.SetManyAsync)
                && Holds.TryGetValue(context.TargetId.Key.ToString()!, out var hold)
                && hold.Entered.TrySetResult())
            {
                await hold.Release.Task;
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class GateConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => siloBuilder.AddOutgoingGrainCallFilter<PrepareGate>();
    }
}
