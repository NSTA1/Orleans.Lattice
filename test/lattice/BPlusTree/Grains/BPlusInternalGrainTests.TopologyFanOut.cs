using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task GetTopologyAsync_bounds_overlapping_reads_and_keeps_child_order()
    {
        var factory = Substitute.For<IGrainFactory>();
        var state = new FakePersistentState<InternalNodeState>();
        var gates = new TaskCompletionSource<ShardTopologyNode>[BoundedFanOut.DefaultWidth + 1];
        var started = 0;
        for (var i = 0; i < gates.Length; i++)
        {
            var id = GrainId.Create("internal", $"topology-{i}");
            state.State.Children.Add(new ChildEntry { ChildId = id });
            var child = Substitute.For<IBPlusInternalGrain>();
            factory.GetGrain<IBPlusInternalGrain>(id).Returns(child);
            gates[i] = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = gates[i];
            child.GetTopologyAsync(1).Returns(_ =>
            {
                started++;
                return gate.Task;
            });
        }
        var grain = new BPlusInternalGrain(Substitute.For<IGrainContext>(), state, factory,
            TestOptionsResolver.Create(factory: factory));

        var read = grain.GetTopologyAsync(2);
        Assert.That(started, Is.EqualTo(BoundedFanOut.DefaultWidth));
        Assert.That(read.IsCompleted, Is.False);
        state.State.Children.Clear();
        for (var i = gates.Length - 1; i >= 0; i--)
            gates[i].SetResult(new ShardTopologyNode { NodeId = $"node-{i}" });
        var topology = await read;

        Assert.That(started, Is.EqualTo(gates.Length));
        Assert.That(topology.ChildFanout, Is.EqualTo(gates.Length));
        Assert.That(topology.Children.Select(c => c.NodeId),
            Is.EqualTo(Enumerable.Range(0, gates.Length).Select(i => $"node-{i}")));
    }

    [Test]
    public async Task GetTopologyAsync_settles_other_reads_before_propagating_failure()
    {
        var factory = Substitute.For<IGrainFactory>();
        var state = new FakePersistentState<InternalNodeState>();
        var failed = new TaskCompletionSource<ShardTopologyNode>(TaskCreationOptions.RunContinuationsAsynchronously);
        var held = new TaskCompletionSource<ShardTopologyNode>(TaskCreationOptions.RunContinuationsAsynchronously);
        foreach (var (id, reply) in new[] { (Child0, failed), (Child1, held) })
        {
            state.State.Children.Add(new ChildEntry { ChildId = id });
            var child = Substitute.For<IBPlusInternalGrain>();
            factory.GetGrain<IBPlusInternalGrain>(id).Returns(child);
            child.GetTopologyAsync(0).Returns(reply.Task);
        }
        var grain = new BPlusInternalGrain(Substitute.For<IGrainContext>(), state, factory,
            TestOptionsResolver.Create(factory: factory));

        var read = grain.GetTopologyAsync(1);
        var failure = new IOException("child unavailable");
        failed.SetException(failure);
        Assert.That(read.IsCompleted, Is.False);
        held.SetResult(new ShardTopologyNode { NodeId = "held" });
        Assert.That(async () => await read, Throws.TypeOf<IOException>().And.SameAs(failure));
    }
}
