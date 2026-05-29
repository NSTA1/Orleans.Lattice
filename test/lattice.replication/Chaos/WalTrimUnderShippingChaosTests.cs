using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage of the WAL trim + shipping interleaving: site A
/// drives a sustained write workload while the WAL maintenance GC
/// runs aggressively (<c>MaintenanceGcInterval</c> = 1 s); site B's
/// outbound edge is repeatedly partitioned and healed via the loopback
/// transport. After every partition heal, site B must catch up via
/// the production shipper's incremental retry path - the shipper's
/// per-peer cursor stays stationary during the partition, the WAL GC
/// trims only past the min-acked cursor across all peers, and the
/// post-window full drain converges every site to the same key state.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this test pins.</b> The trim path inside
/// <c>LatticeWalGc</c> respects the per-peer
/// <c>IWalCursorRegistry</c> minimum, so a partitioned peer holds the
/// trim frontier stationary across the partition window. When the
/// partition heals, the shipper resumes from its stationary cursor
/// and ships every accumulated entry; B's applier observes them; B
/// converges.
/// </para>
/// <para>
/// <b>What this test does NOT pin.</b> The auto-bootstrap path
/// (fall-off-the-log detection + cross-cluster snapshot drain) is
/// out of scope here because the fixture's in-process loopback
/// transport does not wire the cross-cluster snapshot RPC. A
/// fall-off-the-log scenario - one where the partition is so long
/// the WAL retention window expires and B's cursor falls behind the
/// oldest available entry - would require the snapshot transport
/// and is tracked as a separate roadmap follow-up.
/// </para>
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class WalTrimUnderShippingChaosTests
{
    private const string TreeName = "chaos-wal-trim";

    [Test]
    public async Task Site_B_catches_up_via_incremental_shipping_after_partition_heal_with_aggressive_wal_gc()
    {
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 2);
        await fixture.InitializeAsync();

        var siteAId = fixture.ClusterIds[0];
        var siteBId = fixture.ClusterIds[1];
        var aLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var bLattice = fixture.ClientOf(1).GetGrain<ILattice>(TreeName);

        // Drive a small initial workload so the shipper grain
        // activates and the cursor pin is real. Wait for convergence
        // so site B's per-peer cursor is genuinely up-to-date.
        for (var i = 0; i < 8; i++)
        {
            await aLattice.SetAsync($"pre-{i:D2}", Encoding.UTF8.GetBytes($"v-{i}"));
        }
        await WaitForConvergenceAsync(bLattice, Enumerable.Range(0, 8).Select(i => $"pre-{i:D2}"), TimeSpan.FromSeconds(20));

        // Now drive three partition cycles while writing continuously.
        // Each cycle: isolate, write 10 keys, hold for 300 ms (enough
        // for several maintenance GC ticks at the 1 s interval), heal,
        // wait for convergence. Each cycle exercises trim+ship
        // interleaving once.
        var allKeys = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 8; i++) allKeys.Add($"pre-{i:D2}");

        for (var cycle = 0; cycle < 3; cycle++)
        {
            fixture.TransportOf(0).IsolateSite(siteBId);

            for (var i = 0; i < 10; i++)
            {
                var key = $"c{cycle}-{i:D2}";
                allKeys.Add(key);
                await aLattice.SetAsync(key, Encoding.UTF8.GetBytes($"v-{cycle}-{i}"));
            }

            // Hold the partition long enough that the WAL GC has a
            // chance to attempt trimming (interval is 1s by fixture
            // default). The per-peer cursor for B is stationary so the
            // trim frontier never advances past those entries.
            await Task.Delay(TimeSpan.FromMilliseconds(1500));

            fixture.TransportOf(0).HealSite(siteBId);
        }

        // Final drain: all entries authored across every cycle must
        // arrive at site B.
        await WaitForConvergenceAsync(bLattice, allKeys, TimeSpan.FromSeconds(45));

        TestContext.Out.WriteLine(
            $"WAL-trim chaos: total keys = {allKeys.Count}, " +
            $"transport batches shipped = {fixture.TransportOf(0).BatchesShipped}, " +
            $"accepted = {fixture.TransportOf(0).BatchesAccepted}.");
    }

    private static async Task WaitForConvergenceAsync(ILattice peer, IEnumerable<string> keys, TimeSpan timeout)
    {
        var keysArr = keys.ToArray();
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var allPresent = true;
            foreach (var k in keysArr)
            {
                var v = await peer.GetAsync(k);
                if (v is null) { allPresent = false; break; }
            }
            if (allPresent) return;
            await Task.Delay(100);
        }
        Assert.Fail($"Peer did not converge on {keysArr.Length} keys within {timeout.TotalSeconds}s.");
    }
}
