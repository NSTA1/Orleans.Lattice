using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

public partial class ReplicationApplierTests
{
    [Test]
    public async Task IsHoldingManyAsync_matches_persisted_buffer_single_probes_and_origins()
    {
        var h = CreateCausalHarness();
        await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        var grain = h.Factory.GetGrain<ICausalApplyBufferGrain>(Tree);
        var requested = new[] { Hlc(100), Hlc(101), Hlc(100) };
        Assert.That(await grain.IsHoldingManyAsync(RemoteCluster, requested), Is.EqualTo(new[] { true, false, true }));
        Assert.That(await grain.IsHoldingManyAsync("other", requested), Is.EqualTo(new[] { false, false, false }));
        Assert.That(await grain.IsHoldingManyAsync(RemoteCluster, []), Is.Empty);

        h.Vc.Entries[OriginC] = Hlc(50);
        await grain.DrainAsync();
        Assert.That(await grain.IsHoldingManyAsync(RemoteCluster, requested), Is.EqualTo(new[] { false, false, false }));
        Assert.Throws<ArgumentException>(() => grain.IsHoldingManyAsync("", []));
        Assert.Throws<ArgumentNullException>(() => grain.IsHoldingManyAsync(RemoteCluster, null!));
    }
}
