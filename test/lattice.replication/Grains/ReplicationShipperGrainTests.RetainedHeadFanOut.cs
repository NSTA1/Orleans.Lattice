using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [TestCase(4)]
    [TestCase(40)]
    public async Task Detach_retained_heads_overlap_before_publishing_the_complete_reseed_marker(int partitions)
    {
        var (grain, state, factory, walTree) = CreateHeadReadFanOut(true, partitions);
        var width = Math.Min(partitions, BoundedFanOut.DefaultWidth);
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        var active = 0;
        var peak = 0;
        for (var p = 0; p < partitions; p++)
        {
            var slot = p;
            var shard = Substitute.For<IWalShardGrain>();
            factory.GetGrain<IWalShardGrain>($"{walTree}/{p}").Returns(shard);
            shard.ReadShippingAsync(0, 1, CancellationToken.None)
                .Returns(_ => new ValueTask<WalShardShippingPage>(ReadAsync(slot)));
        }

        var operation = grain.DetachFromLogAsync(CancellationToken.None);
        try
        {
            await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Multiple(() =>
            {
                Assert.That(entered, Is.EqualTo(width));
                Assert.That(peak, Is.EqualTo(width));
                Assert.That(state.State.ReseedRetainFrom, Is.Null);
                Assert.That(state.State.ReseedRequiredEpoch, Is.Null);
                Assert.That(state.WriteCount, Is.Zero);
            });
        }
        finally
        {
            release.TrySetResult();
            await operation.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.That(entered, Is.EqualTo(partitions));
        Assert.That(peak, Is.EqualTo(width));
        Assert.That(state.State.ReseedRetainFrom,
            Is.EqualTo(Enumerable.Range(0, partitions).Select(p => 100L + p)));
        Assert.That(state.State.ReseedRequiredEpoch, Is.Not.Null);

        async Task<WalShardShippingPage> ReadAsync(int slot)
        {
            Assert.That(state.State.ReplayHoldLog, Is.EqualTo(walTree), "Hold before head capture.");
            var concurrent = Interlocked.Increment(ref active);
            int observed;
            do { observed = Volatile.Read(ref peak); }
            while (concurrent > observed && Interlocked.CompareExchange(ref peak, concurrent, observed) != observed);
            if (Interlocked.Increment(ref entered) == width) arrived.TrySetResult();
            await release.Task;
            Interlocked.Decrement(ref active);
            return new WalShardShippingPage
            {
                Entries = slot % 2 == 0 ? [] : [new WalShardShippingEntry { Sequence = 100 + slot, EncodedPayload = [] }],
                NextSequence = slot % 2 == 0 ? 100 + slot : 1000 + slot,
            };
        }
    }

    [Test]
    public async Task Detach_failed_retained_head_settles_siblings_without_publishing_a_partial_marker()
    {
        var (grain, state, factory, walTree) = CreateHeadReadFanOut(true, 4);
        var release = new TaskCompletionSource<WalShardShippingPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        for (var p = 0; p < 4; p++)
        {
            var slot = p;
            var shard = Substitute.For<IWalShardGrain>();
            factory.GetGrain<IWalShardGrain>($"{walTree}/{p}").Returns(shard);
            shard.ReadShippingAsync(0, 1, CancellationToken.None).Returns(_ =>
                new ValueTask<WalShardShippingPage>(slot == 0
                    ? Task.FromException<WalShardShippingPage>(new InvalidOperationException("head unavailable"))
                    : release.Task));
        }

        var operation = grain.DetachFromLogAsync(CancellationToken.None);
        Assert.That(operation.IsCompleted, Is.False);
        release.SetResult(new WalShardShippingPage { Entries = [], NextSequence = 42 });
        Assert.That(async () => await operation.WaitAsync(TimeSpan.FromSeconds(5)),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("head unavailable"));
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(state.State.ReseedRequiredEpoch, Is.Null);
            Assert.That(state.State.ReseedRetainFrom, Is.Null);
        });
    }
}
