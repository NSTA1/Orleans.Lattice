using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Diagnostic smoke tests for <see cref="MultiSiteClusterFixture"/> +
/// <see cref="ChaosDeliveryPump"/>. These are not chaos tests themselves
/// (no random partitions, no concurrent workload) — they exist to
/// short-circuit failure analysis of the convergence chaos suite by
/// pinning the simpler invariants the suite relies on:
/// <list type="bullet">
/// <item><description>Each site's WAL captures locally-authored mutations.</description></item>
/// <item><description>Each site's <see cref="IChangeFeed"/> yields the captured entry.</description></item>
/// <item><description>The delivery pump applies an entry from one site onto another.</description></item>
/// </list>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class MultiSiteClusterFixtureSmokeTests
{
    [Test]
    public async Task Site_change_feed_yields_locally_authored_lww_entry()
    {
        var fixture = new MultiSiteClusterFixture(LatticeMergeMode.LwwRegister, siteCount: 2);
        try
        {
            await fixture.InitializeAsync();
            var lattice = fixture.ClientOf(0).GetGrain<ILattice>("smoke-lww");
            await lattice.SetAsync("k", new byte[] { 1 });

            var entries = new List<WalRecord>();
            await foreach (var e in fixture.ChangeFeedOf(0).Subscribe("smoke-lww", HybridLogicalClock.Zero))
            {
                entries.Add(e);
            }

            Assert.That(entries, Is.Not.Empty,
                "Producer-side WAL should have captured the SetAsync at commit time.");
            Assert.That(entries[0].OriginClusterId, Is.EqualTo(MultiSiteClusterFixture.ClusterIdFor(0)));
            Assert.That(entries[0].Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task Delivery_pump_ships_lww_entry_from_site_0_to_site_1()
    {
        var fixture = new MultiSiteClusterFixture(LatticeMergeMode.LwwRegister, siteCount: 2);
        ChaosDeliveryPump? pump = null;
        try
        {
            await fixture.InitializeAsync();
            pump = new ChaosDeliveryPump(fixture, "smoke-pump");
            pump.Start();

            await fixture.ClientOf(0).GetGrain<ILattice>("smoke-pump").SetAsync("k", new byte[] { 7, 7, 7 });

            try
            {
                await pump.HealAllAndDrainAsync(TimeSpan.FromSeconds(15));
            }
            catch
            {
                while (pump.PumpErrors.TryDequeue(out var ex))
                {
                    TestContext.WriteLine($"Pump error: {ex}");
                }
                throw;
            }

            var siteB = await fixture.ClientOf(1).GetGrain<ILattice>("smoke-pump").GetAsync("k");
            Assert.That(siteB, Is.EqualTo(new byte[] { 7, 7, 7 }),
                "Site 1 should have received site 0's authored value via the pump.");
        }
        finally
        {
            // Pump must dispose BEFORE the fixture so its background tasks
            // don't issue grain calls against already-disposed clusters.
            if (pump is not null)
            {
                await pump.DisposeAsync();
            }
            await fixture.DisposeAsync();
        }
    }
}
