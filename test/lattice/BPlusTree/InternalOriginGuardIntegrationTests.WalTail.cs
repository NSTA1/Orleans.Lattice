using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed partial class InternalOriginGuardIntegrationTests
{
    [Test]
    public void Wal_GetTailOffsetAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-tail/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.GetTailOffsetAsync(CancellationToken.None));
    }

    [Test]
    public void Wal_GetResumeFloorAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-resume/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.GetResumeFloorAsync(CancellationToken.None));
    }
}
