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

        var read = await tree.GetManyAsync([k1, k2]);
        Assert.Multiple(() =>
        {
            Assert.That(restore.ShadowPhysicalTreeId, Is.Not.EqualTo(restore.PreviousPhysicalTreeId),
                "precondition: the restore cut the tree over to a new copy");
            Assert.That(read.TryGetValue(k1, out var v1) ? Encoding.UTF8.GetString(v1) : null, Is.EqualTo("new"),
                "the saga must commit on the copy the tree resolves to after the cutover");
            Assert.That(read.TryGetValue(k2, out var v2) ? Encoding.UTF8.GetString(v2) : null, Is.EqualTo("new"),
                "the saga must commit its whole batch on the restored copy");
        });
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
