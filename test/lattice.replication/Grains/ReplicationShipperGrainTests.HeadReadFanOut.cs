using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [TestCase(false, 4)]
    [TestCase(false, 40)]
    [TestCase(true, 4)]
    [TestCase(true, 40)]
    public async Task NotifySourceIdentity_head_reads_overlap_with_a_bound_before_protocol_publication(bool lineageChange, int partitions)
    {
        var (grain, state, factory, walTree) = CreateHeadReadFanOut(lineageChange, partitions);
        var width = Math.Min(partitions, BoundedFanOut.DefaultWidth);
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        var active = 0;
        var peak = 0;
        var censusLock = new object();
        for (var p = 0; p < partitions; p++)
        {
            var slot = p;
            var shard = Substitute.For<IWalShardGrain>();
            factory.GetGrain<IWalShardGrain>($"{walTree}/{p}").Returns(shard);
            shard.GetNextSequenceAsync(CancellationToken.None).Returns(_ => new ValueTask<long>(ReadAsync(slot)));
            shard.ReadShippingAsync(0, 1, CancellationToken.None)
                .Returns(new WalShardShippingPage { Entries = [], NextSequence = 0 });
        }

        var operation = grain.NotifySourceIdentityChangedAsync(walTree, CancellationToken.None);
        try
        {
            await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Multiple(() =>
            {
                Assert.That(entered, Is.EqualTo(width));
                Assert.That(peak, Is.EqualTo(width));
                Assert.That(state.WriteCount, Is.Zero);
                Assert.That(state.State.ReplayFilterHorizon, Is.Null);
                Assert.That(state.State.SourceLineageBoundary, Is.Empty);
                Assert.That(state.State.ReseedRequiredEpoch, Is.Null);
            });
            await factory.GetGrain<IReplicationExportEpochGrain>(Tree).DidNotReceive().GetAsync();
        }
        finally
        {
            release.TrySetResult();
            await operation.WaitAsync(TimeSpan.FromSeconds(5));
        }

        var expected = Enumerable.Range(0, partitions).Select(p => 100L + p).ToArray();
        Assert.That(entered, Is.EqualTo(partitions));
        Assert.That(peak, Is.EqualTo(width));
        if (lineageChange)
        {
            Assert.That(state.State.SourceLineageBoundary.OrderBy(pair => pair.Key).Select(pair => pair.Value), Is.EqualTo(expected));
            Assert.That(state.State.ReseedRequiredEpoch, Is.Not.Null);
        }
        else
        {
            Assert.That(state.State.ReplayFilterHorizon, Is.EqualTo(expected));
            await factory.GetGrain<IWalPurgeHoldGrain>(walTree).Received(1)
                .AddAsync(Arg.Any<string>(), Arg.Any<long[]>());
        }

        async Task<long> ReadAsync(int slot)
        {
            if (!lineageChange)
                Assert.That(state.State.ReplayHoldLog, Is.EqualTo(walTree), "The purge hold must precede every horizon read.");
            lock (censusLock)
            {
                active++;
                peak = Math.Max(peak, active);
            }
            if (Interlocked.Increment(ref entered) == width) arrived.TrySetResult();
            await release.Task;
            lock (censusLock) active--;
            return 100L + slot;
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task NotifySourceIdentity_failed_head_read_settles_siblings_without_publishing_a_partial_boundary(bool lineageChange)
    {
        var (grain, state, factory, walTree) = CreateHeadReadFanOut(lineageChange, 4);
        var release = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        for (var p = 0; p < 4; p++)
        {
            var slot = p;
            var shard = Substitute.For<IWalShardGrain>();
            factory.GetGrain<IWalShardGrain>($"{walTree}/{p}").Returns(shard);
            shard.GetNextSequenceAsync(CancellationToken.None).Returns(_ =>
                new ValueTask<long>(slot == 0 ? Task.FromException<long>(new InvalidOperationException("head unavailable")) : release.Task));
        }

        var operation = grain.NotifySourceIdentityChangedAsync(walTree, CancellationToken.None);
        Assert.That(operation.IsCompleted, Is.False, "A failed fan-out must still await its launched siblings.");
        release.SetResult(100);
        Assert.That(async () => await operation.WaitAsync(TimeSpan.FromSeconds(5)),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("head unavailable"));
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(state.State.ReplayFilterHorizon, Is.Null);
            Assert.That(state.State.SourceLineageBoundary, Is.Empty);
            Assert.That(state.State.ReseedRequiredEpoch, Is.Null);
        });
        await factory.GetGrain<IReplicationExportEpochGrain>(Tree).DidNotReceive().GetAsync();
    }

    private static (ReplicationShipperGrain Grain,
        Fakes.FakePersistentState<ReplicationShipperState> State, IGrainFactory Factory, string WalTree)
        CreateHeadReadFanOut(bool lineageChange, int partitions)
    {
        var walTree = lineageChange ? Tree : "replacement-log";
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var original = Guid.NewGuid();
        registry.GetEntryAsync(Tree).Returns(new TreeRegistryEntry
        {
            PhysicalTreeId = walTree,
            Lineage = lineageChange ? Guid.NewGuid() : original,
        });
        var seed = new ReplicationShipperState
        {
            BoundPhysicalTreeId = Tree,
            BoundSourceLineageKnown = true,
            BoundSourceLineage = original,
        };
        var (grain, state, _, _, _, _, _) = Create(
            new LatticeReplicationOptions { ClusterId = LocalCluster, ReplogPartitions = partitions },
            seedState: seed, grainFactory: factory);
        return (grain, state, factory, walTree);
    }
}
