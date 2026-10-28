using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainSplitTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Crdt_repair_binds_in_one_call_before_the_single_retry(bool batch)
    {
        var h = CreateHarness();
        var attempts = 0;
        var bound = false;
        var delta = new byte[] { 1 };
        var deltas = new List<KeyValuePair<string, byte[]>> { new("key", delta) };
        var version = new HybridLogicalClock { WallClockTicks = 123, Counter = 0 };
        h.Leaf.SetBindingAsync(TreeId, 0).Returns(_ =>
        {
            bound = true;
            return Task.CompletedTask;
        });
        h.Leaf.ApplyCrdtDeltaAsync("key", LatticeMergeMode.OrFlag, delta, 0).Returns(_ =>
        {
            if (++attempts == 1)
                return Task.FromException<CrdtApplyResult>(new LatticeCrdtShapeNotRegisteredException("unbound"));
            Assert.That(bound, Is.True, "The binding must finish before retrying the write.");
            return Task.FromResult(new CrdtApplyResult { Version = version });
        });
        h.Leaf.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.OrFlag).Returns(_ =>
        {
            if (++attempts == 1)
                return Task.FromException<SplitResult?>(new LatticeCrdtShapeNotRegisteredException("unbound"));
            Assert.That(bound, Is.True);
            return Task.FromResult<SplitResult?>(null);
        });

        if (batch)
            await h.Grain.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.OrFlag);
        else
            Assert.That(await h.Grain.ApplyCrdtDeltaAsync("key", LatticeMergeMode.OrFlag, delta), Is.EqualTo(version));

        Assert.That(attempts, Is.EqualTo(2));
        await h.Leaf.Received(1).SetBindingAsync(TreeId, 0);
        await h.Leaf.DidNotReceive().SetTreeIdAsync(Arg.Any<string>());
        await h.Leaf.DidNotReceive().SetShardIndexAsync(Arg.Any<int>());
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public void Crdt_repair_does_not_mask_binding_or_retry_failure(bool batch, bool bindingFails)
    {
        var h = CreateHarness();
        var delta = new byte[] { 1 };
        var deltas = new List<KeyValuePair<string, byte[]>> { new("key", delta) };
        var unbound = new LatticeCrdtShapeNotRegisteredException("unbound");
        var failure = new InvalidOperationException("binding or retry failed");
        h.Leaf.SetBindingAsync(TreeId, 0).Returns(bindingFails ? Task.FromException(failure) : Task.CompletedTask);
        h.Leaf.ApplyCrdtDeltaAsync("key", LatticeMergeMode.OrFlag, delta, 0)
            .Returns(Task.FromException<CrdtApplyResult>(unbound), Task.FromException<CrdtApplyResult>(failure));
        h.Leaf.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.OrFlag)
            .Returns(Task.FromException<SplitResult?>(unbound), Task.FromException<SplitResult?>(failure));

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            if (batch)
                await h.Grain.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.OrFlag);
            else
                await h.Grain.ApplyCrdtDeltaAsync("key", LatticeMergeMode.OrFlag, delta);
        });

        Assert.That(thrown, Is.SameAs(failure));
        Assert.That(h.Leaf.ReceivedCalls().Count(call => call.GetMethodInfo().Name ==
            (batch ? nameof(IBPlusLeafGrain.ApplyCrdtDeltaManyAsync) : nameof(IBPlusLeafGrain.ApplyCrdtDeltaAsync))),
            Is.EqualTo(bindingFails ? 1 : 2));
    }
}
