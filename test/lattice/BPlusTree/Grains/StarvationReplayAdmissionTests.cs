using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
[NonParallelizable]
public sealed class StarvationReplayAdmissionTests
{
    [TearDown]
    public void Reset() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    [TestCase(1, 1)]
    [TestCase(2, 1)]
    [TestCase(6, 3)]
    [TestCase(7, 3)]
    public void Acquisition_reserves_only_the_gc_share(int ceiling, int expected)
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, queued: 0);
        using var gate = new SemaphoreSlim(ceiling, ceiling);
        for (var i = 0; i < expected; i++)
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate), Is.True);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate), Is.False);
        Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - expected));
        for (var i = 0; i < expected; i++)
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(ceiling));
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate), Is.True);
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
    }

    [Test]
    public void Failed_shared_acquisition_returns_the_gc_reservation()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        using var gate = new SemaphoreSlim(0, 2);
        for (var i = 0; i < 10; i++)
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate), Is.False);
        gate.Release(2);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate), Is.True);
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(2));
    }

    [Test]
    public void Concurrent_acquisitions_cannot_exceed_the_gc_share()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        using var gate = new SemaphoreSlim(6, 6);
        var acquired = 0;
        Parallel.For(0, 64, _ =>
        {
            if (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate))
                Interlocked.Increment(ref acquired);
        });
        Assert.That(acquired, Is.EqualTo(3));
        Assert.That(gate.CurrentCount, Is.EqualTo(3));
        for (var i = 0; i < acquired; i++)
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(6));
    }
}
