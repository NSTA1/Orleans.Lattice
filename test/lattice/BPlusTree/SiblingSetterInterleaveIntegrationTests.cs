using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4919: a sibling-pointer setter must be admitted into a leaf whose turn
/// is suspended on an outgoing call, because a split calls it on its neighbour
/// while holding its own split gate. The fixture suspends a NON-interleaving
/// leaf turn - a snapshot capture, held on its call to snapshot storage, made
/// outside the split gate - and asserts that both setters complete while that
/// turn is still running. A suspended [AlwaysInterleave] turn would not
/// discriminate: Orleans records only a running non-interleaving request as the
/// blocking request, so it admits a non-interleaving setter either way.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class SiblingSetterInterleaveIntegrationTests
{
    private TestCluster _cluster = null!;

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
        StorageHold.Release();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Sibling_setters_are_admitted_while_a_capture_turn_is_suspended()
    {
        var treeId = $"sibling-setter-interleave-{Guid.NewGuid():N}";
        await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < 5; i++)
            await tree.SetAsync($"k{i:D2}", [1]);

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var headId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var head = _cluster.Client.GetGrain<IBPlusLeafGrain>(headId);
        var next = await head.GetNextSiblingAsync();
        var prev = await head.GetPrevSiblingAsync();
        Assert.That(next.HasValue, Is.True, "control: the head leaf has split and has a successor");

        StorageHold.Arm(headId);
        // CaptureSnapshotAsync is a non-interleaving turn, and its storage call
        // is made outside the split gate - the shape of C's capture-split link
        // in the #4919 cycle.
        var capture = head.CaptureSnapshotAsync();
        try
        {
            await StorageHold.Entered.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.That(capture.IsCompleted, Is.False, "control: the head's capture is suspended on storage");

            // Rewrite the current pointers, so the chain is unchanged. A
            // non-interleaving setter would queue behind the suspended capture.
            await head.SetNextSiblingAsync(next).WaitAsync(TimeSpan.FromSeconds(10));
            await head.SetPrevSiblingAsync(prev).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(capture.IsCompleted, Is.False,
                "the setters must complete while the leaf's capture is still suspended");
        }
        finally
        {
            StorageHold.Release();
            await capture.WaitAsync(TimeSpan.FromSeconds(20));
        }

        Assert.That(await head.GetNextSiblingAsync(), Is.EqualTo(next));
        Assert.That(await head.GetPrevSiblingAsync(), Is.EqualTo(prev));
        Assert.That(await tree.GetAsync("k00"), Is.Not.Null, "the tree still serves reads");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, StorageHoldFilter>();
        }
    }

    private static class StorageHold
    {
        internal static GrainId? Source { get; private set; }
        internal static TaskCompletionSource Entered { get; private set; } = NewSignal();
        internal static TaskCompletionSource Resume { get; private set; } = NewSignal();

        private static TaskCompletionSource NewSignal() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static void Arm(GrainId source)
        {
            Entered = NewSignal();
            Resume = NewSignal();
            Source = source;
        }

        internal static void Release()
        {
            Source = null;
            Resume.TrySetResult();
        }
    }

    private sealed class StorageHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (StorageHold.Source is { } source
                && context.SourceId == source
                && context.InterfaceMethod.DeclaringType == typeof(ILeafSnapshotStorageGrain))
            {
                StorageHold.Entered.TrySetResult();
                await StorageHold.Resume.Task;
            }

            await context.Invoke();
        }
    }
}
