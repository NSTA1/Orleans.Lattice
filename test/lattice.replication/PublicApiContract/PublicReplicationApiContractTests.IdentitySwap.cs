using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Regression coverage for the cross-cluster follow-on to the single-cluster
/// derived-state identity-swap heal. A source tree's physical identity can be
/// swapped underneath its logical registry alias by a shadow-cutover restore,
/// a resize, or a reshard. The outbound shipper addresses source WAL shards by
/// tree id; if it pins the pre-swap physical identity it silently stops tailing
/// the live log and the peer never converges on post-swap writes. The shipper
/// must re-resolve the logical alias to its current physical identity each pump
/// and rebind to the new WAL when the identity changes.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task Shipper_tails_new_source_identity_after_alias_swap()
    {
        var treeId = NextTreeId("identity-swap");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        // Seed a write under the original identity and let it converge so the
        // shipper is warm and bound to the pre-swap physical WAL.
        await treeOnA.SetAsync("k1", Bytes("v1"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k1")) == "v1",
            $"Site B should see Site A's pre-swap write to '{treeId}/k1'.");

        // Swap the source tree's physical identity under its logical alias.
        // Mint a fresh physical tree, repoint the alias, and force the writer
        // to resolve the new identity - exactly what a shadow-cutover restore
        // does after it repoints the alias.
        var physicalV2 = $"{treeId}#v2";
        var registryA = ClientA.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registryA.RegisterAsync(
            physicalV2,
            new TreeRegistryEntry
            {
                MaxLeafKeys = PublicReplicationApiClusterFixture.SmallMaxLeafKeys,
                MaxInternalChildren = PublicReplicationApiClusterFixture.SmallMaxInternalChildren,
                ShardCount = PublicReplicationApiClusterFixture.DefaultShardCount,
            });
        await registryA.SetAliasAsync(treeId, physicalV2);
        await treeOnA.GetRoutingAsync(forceRefresh: true);

        // Write under the new physical identity. This lands on the freshly
        // minted WAL, which the pre-swap shipper cursor does not address.
        await treeOnA.SetAsync("k2", Bytes("v2"));

        // Nudge the shipper. The writer-side doorbell fires against the live
        // WAL append; poke the logical (tree, peer) shipper explicitly so the
        // rebind path is exercised deterministically rather than waiting on the
        // steady-state phase timer.
        await ClientA
            .GetGrain<IReplicationShipperGrain>(
                $"{treeId}/{PublicReplicationApiClusterFixture.SiteBClusterId}")
            .OnDoorbellAsync(CancellationToken.None);

        // The post-swap write must reach Site B: the shipper has to re-resolve
        // the alias to the new physical identity and tail the new WAL.
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k2")) == "v2",
            $"Site B should see the post-swap write to '{treeId}/k2' once the "
            + "shipper rebinds to the new physical source identity.");

        // The pre-swap value must remain converged (LWW keeps both keys).
        var k2OnB = Str(await treeOnB.GetAsync("k2"));
        var k1OnB = Str(await treeOnB.GetAsync("k1"));
        Assert.Multiple(() =>
        {
            Assert.That(k2OnB, Is.EqualTo("v2"));
            Assert.That(k1OnB, Is.EqualTo("v1"));
        });
    }
}
