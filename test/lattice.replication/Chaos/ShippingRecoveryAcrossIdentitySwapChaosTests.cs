using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage of the cross-cluster shipper's recovery when a logical
/// source tree's PHYSICAL identity is swapped under its registry alias
/// mid-workload (a restore-style cutover, possibly repeated) while the
/// inter-site delivery edge is cycled through partition and heal.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this pins.</b> Each shipper pump tick re-resolves the logical
/// source tree to its current physical id via the registry, and when that
/// id changes it clears its per-partition cursors and re-ships from the new
/// physical WAL log start. Peers merge every shipped entry by HLC (LWW), so
/// the re-ship is idempotent. After the workload quiesces every peer site
/// converges on the POST-swap source key set: no peer is left tailing the
/// orphaned pre-swap physical WAL, and the keys written only to the
/// abandoned identity while the edge was partitioned never reach a receiver.
/// </para>
/// <para>
/// <b>Scenario shape.</b> The "doomed" keys are authored into the OLD
/// physical identity only while the outbound edge to the peer is isolated,
/// so they were never shipped. The alias is then repointed to a freshly
/// minted physical tree that carries the reverted key set, and the edge is
/// healed. Because the shipper abandons the old physical WAL on the swap,
/// the doomed keys are never delivered - the achievable and meaningful
/// guarantee under plain LWW shipping, where an unshipped pre-swap-only key
/// simply never leaves the source.
/// </para>
/// <para>
/// <b>Boundary.</b> A key that WAS already shipped to a peer before the swap
/// is not retracted by a subsequent identity swap - LWW cross-cluster
/// shipping carries no tombstone for it. True cross-cluster retraction of an
/// already-delivered key requires the coordinated (write-fenced) restore
/// path, which is exercised by the coordinated-restore chaos suite. This
/// suite deliberately confines the abandoned-identity keys to the partition
/// window so the guarantee it asserts is the one plain shipping can keep.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class ShippingRecoveryAcrossIdentitySwapChaosTests
{
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(45);

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static TreeRegistryEntry ShadowEntry() => new() { MaxLeafKeys = 16, ShardCount = 1 };

    [Test]
    public async Task Peer_converges_on_new_identity_and_abandoned_keys_never_survive_after_single_swap()
    {
        const string treeName = "chaos-idswap-single";
        await using var fixture = new ProductionShipperFixture(treeName, siteCount: 2);
        await fixture.InitializeAsync();

        var peerId = fixture.ClusterIds[1];
        var source = fixture.ClientOf(0).GetGrain<ILattice>(treeName);
        var peer = fixture.ClientOf(1).GetGrain<ILattice>(treeName);
        var registry = fixture.ClientOf(0).GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Baseline: converge a seed set to the peer so the shipper grain is
        // live and its per-peer cursor is genuinely current.
        var baseline = Enumerable.Range(0, 8).Select(i => $"base-{i:D2}").ToArray();
        foreach (var k in baseline) await source.SetAsync(k, V($"seed-{k}"));
        await WaitForPresenceAsync(peer, baseline, ConvergenceTimeout);

        var acceptedBefore = fixture.TransportOf(0).BatchesAccepted;

        // Isolate the peer, then author "doomed" keys into the CURRENT (old)
        // physical identity. They accumulate unshipped in the old WAL.
        fixture.TransportOf(0).IsolateSite(peerId);
        var doomed = Enumerable.Range(0, 10).Select(i => $"doomed-{i:D2}").ToArray();
        foreach (var k in doomed) await source.SetAsync(k, V($"gone-{k}"));

        // Mint a fresh physical identity carrying the reverted set (the
        // baseline re-authored, plus a marker only the new identity holds),
        // then cut the logical alias over to it - still under partition.
        var shadowId = $"{treeName}-gen1";
        await registry.RegisterAsync(shadowId, ShadowEntry());
        var shadow = fixture.ClientOf(0).GetGrain<ILattice>(shadowId);
        foreach (var k in baseline) await shadow.SetAsync(k, V($"reverted-{k}"));
        await shadow.SetAsync("revert-marker", V("only-in-new-identity"));
        await registry.SetAliasAsync(treeName, shadowId);

        // Give the shipper a few ticks to observe the swap while still
        // partitioned, then heal.
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        fixture.TransportOf(0).HealSite(peerId);

        // The peer must converge on the new identity's set...
        var expected = baseline.Append("revert-marker").ToArray();
        await WaitForPresenceAsync(peer, expected, ConvergenceTimeout);

        // ...the reverted values win (higher HLC than the seed)...
        Assert.That(Encoding.UTF8.GetString((await peer.GetAsync("base-00"))!),
            Is.EqualTo("reverted-base-00"));
        Assert.That(Encoding.UTF8.GetString((await peer.GetAsync("revert-marker"))!),
            Is.EqualTo("only-in-new-identity"));

        // ...and no key confined to the abandoned identity ever arrived.
        await AssertAbsenceHoldsAsync(peer, doomed);

        Assert.That(fixture.TransportOf(0).BatchesAccepted, Is.GreaterThan(acceptedBefore),
            "Shipping must resume and deliver batches after the identity swap heals.");
    }

    [Test]
    public async Task Peer_converges_on_final_identity_after_repeated_swaps_under_partition_cycling()
    {
        const string treeName = "chaos-idswap-repeated";
        await using var fixture = new ProductionShipperFixture(treeName, siteCount: 2);
        await fixture.InitializeAsync();

        var peerId = fixture.ClusterIds[1];
        var source = fixture.ClientOf(0).GetGrain<ILattice>(treeName);
        var peer = fixture.ClientOf(1).GetGrain<ILattice>(treeName);
        var registry = fixture.ClientOf(0).GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        var baseline = Enumerable.Range(0, 6).Select(i => $"base-{i:D2}").ToArray();
        foreach (var k in baseline) await source.SetAsync(k, V($"seed-{k}"));
        await WaitForPresenceAsync(peer, baseline, ConvergenceTimeout);

        var allDoomed = new List<string>();
        var currentLogical = source;

        // Three successive cutovers, each performed while the peer is
        // isolated with doomed writes accreting on the identity being
        // abandoned. Only the final identity should be visible on the peer.
        for (var gen = 1; gen <= 3; gen++)
        {
            fixture.TransportOf(0).IsolateSite(peerId);

            var doomed = Enumerable.Range(0, 5).Select(i => $"doomed-g{gen}-{i:D2}").ToArray();
            allDoomed.AddRange(doomed);
            foreach (var k in doomed) await currentLogical.SetAsync(k, V($"gone-{k}"));

            var shadowId = $"{treeName}-gen{gen}";
            await registry.RegisterAsync(shadowId, ShadowEntry());
            var shadow = fixture.ClientOf(0).GetGrain<ILattice>(shadowId);
            foreach (var k in baseline) await shadow.SetAsync(k, V($"gen{gen}-{k}"));
            await shadow.SetAsync($"gen-marker", V($"gen{gen}"));
            await registry.SetAliasAsync(treeName, shadowId);

            await Task.Delay(TimeSpan.FromMilliseconds(250));
            fixture.TransportOf(0).HealSite(peerId);
            await Task.Delay(TimeSpan.FromMilliseconds(250));

            // Subsequent generations write their doomed keys through the new
            // physical identity directly (routing the logical grain post-swap
            // is a separate concern - see the routing-cache self-heal seam).
            currentLogical = shadow;
        }

        var expected = baseline.Append("gen-marker").ToArray();
        await WaitForPresenceAsync(peer, expected, ConvergenceTimeout);

        Assert.That(Encoding.UTF8.GetString((await peer.GetAsync("gen-marker"))!),
            Is.EqualTo("gen3"), "Peer must converge on the final identity's marker.");
        Assert.That(Encoding.UTF8.GetString((await peer.GetAsync("base-00"))!),
            Is.EqualTo("gen3-base-00"), "Peer must carry the final identity's values.");

        await AssertAbsenceHoldsAsync(peer, allDoomed);
    }

    [Test]
    public async Task Both_live_and_partitioned_peers_converge_on_new_identity_after_swap()
    {
        const string treeName = "chaos-idswap-multipeer";
        await using var fixture = new ProductionShipperFixture(treeName, siteCount: 3);
        await fixture.InitializeAsync();

        var partitionedPeerId = fixture.ClusterIds[1];
        var source = fixture.ClientOf(0).GetGrain<ILattice>(treeName);
        var livePeer = fixture.ClientOf(2).GetGrain<ILattice>(treeName);
        var partitionedPeer = fixture.ClientOf(1).GetGrain<ILattice>(treeName);
        var registry = fixture.ClientOf(0).GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        var baseline = Enumerable.Range(0, 6).Select(i => $"base-{i:D2}").ToArray();
        foreach (var k in baseline) await source.SetAsync(k, V($"seed-{k}"));
        await WaitForPresenceAsync(livePeer, baseline, ConvergenceTimeout);
        await WaitForPresenceAsync(partitionedPeer, baseline, ConvergenceTimeout);

        // Isolate only site 1; site 2 stays live across the swap.
        fixture.TransportOf(0).IsolateSite(partitionedPeerId);
        var doomed = Enumerable.Range(0, 8).Select(i => $"doomed-{i:D2}").ToArray();
        foreach (var k in doomed) await source.SetAsync(k, V($"gone-{k}"));

        var shadowId = $"{treeName}-gen1";
        await registry.RegisterAsync(shadowId, ShadowEntry());
        var shadow = fixture.ClientOf(0).GetGrain<ILattice>(shadowId);
        foreach (var k in baseline) await shadow.SetAsync(k, V($"reverted-{k}"));
        await shadow.SetAsync("revert-marker", V("new-identity"));
        await registry.SetAliasAsync(treeName, shadowId);

        await Task.Delay(TimeSpan.FromMilliseconds(300));
        fixture.TransportOf(0).HealSite(partitionedPeerId);

        var expected = baseline.Append("revert-marker").ToArray();
        await WaitForPresenceAsync(livePeer, expected, ConvergenceTimeout);
        await WaitForPresenceAsync(partitionedPeer, expected, ConvergenceTimeout);

        // The peer that was partitioned across the swap must never see the
        // abandoned-identity keys; the live peer received them pre-swap only
        // if it was NOT isolated, so we assert absence solely on the isolated
        // peer where the guarantee is unambiguous.
        await AssertAbsenceHoldsAsync(partitionedPeer, doomed);
    }

    private static async Task WaitForPresenceAsync(ILattice peer, IEnumerable<string> keys, TimeSpan timeout)
    {
        var keysArr = keys.ToArray();
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var allPresent = true;
            foreach (var k in keysArr)
            {
                if (await peer.GetAsync(k) is null) { allPresent = false; break; }
            }
            if (allPresent) return;
            await Task.Delay(100);
        }
        Assert.Fail($"Peer did not converge on {keysArr.Length} keys within {timeout.TotalSeconds}s.");
    }

    /// <summary>
    /// Asserts none of the given keys are present, and - to guard against a
    /// late delivery racing the assertion - that the absence is stable over a
    /// short observation window past the point convergence has already been
    /// confirmed for the expected set.
    /// </summary>
    private static async Task AssertAbsenceHoldsAsync(ILattice peer, IReadOnlyCollection<string> keys)
    {
        for (var probe = 0; probe < 5; probe++)
        {
            foreach (var k in keys)
            {
                var v = await peer.GetAsync(k);
                Assert.That(v, Is.Null,
                    $"Key '{k}' from an abandoned identity must never reach a receiver.");
            }
            await Task.Delay(100);
        }
    }
}
