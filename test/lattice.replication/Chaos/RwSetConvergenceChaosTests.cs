using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.RwSet"/>
/// dispatch path. Several sites concurrently add and remove a shared element
/// while a partition isolates one site mid-workload; after the partition heals
/// and the delivery pump drains, every site must observe the same remove-wins
/// outcome.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.RwSet</c> on every silo, so the producer side emits
/// typed <see cref="RwSetDelta"/> on the WAL and the receiver routes through
/// <see cref="ReplicationApplier"/>'s typed-delta apply path under
/// <see cref="LatticeOriginContext"/> - the full mode-declaration ->
/// producer-dispatch -> receiver-merge pipeline this matrix exists to pin.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class RwSetConvergenceChaosTests
{
    private const string TreeName = "chaos-rwset";
    private const string Key = "set";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);
    private static readonly byte[] Element = Encoding.UTF8.GetBytes("member");

    [Test]
    public async Task Concurrent_remove_during_partition_wins_over_add_at_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Phase 1: site 0 adds the element and lets it converge so every
        // site has observed the add dot.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .RwSet(Key).AddAsync(Element, MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: partition site 2 off. Site 0 removes (minting a fresh
        // remove dot) while site 2, behind the partition, concurrently
        // re-adds with a dot that never observed site 0's remove.
        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        await Task.WhenAll(
            site0.RwSet(Key).RemoveAsync(Element, MultiSiteClusterFixture.ClusterIdFor(0)),
            site2.RwSet(Key).AddAsync(Element, MultiSiteClusterFixture.ClusterIdFor(2)));

        // Phase 3: heal and drain. The remove-wins invariant requires every
        // site to converge to "absent": site 0's remove dot was never in
        // site 2's tombstone set, so it survives the merge everywhere.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var present = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .RwSet(Key).ContainsAsync(Element);
            Assert.That(present, Is.False,
                $"Site {i} did not converge to remove-wins outcome.");
        }
    }

    [Test]
    public async Task Add_observing_all_removes_converges_to_present()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Site 0 adds the element and the topology drains so the add dot is
        // observed everywhere.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .RwSet(Key).AddAsync(Element, MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Every site removes (minting its own remove dot) and the topology
        // drains so every remove dot is observed everywhere.
        for (var i = 0; i < SiteCount; i++)
        {
            await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .RwSet(Key).RemoveAsync(Element, MultiSiteClusterFixture.ClusterIdFor(i));
        }
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // One site re-adds after observing every remove dot, then drains.
        // Because the add tombstones every observed remove, no remove survives
        // and the element converges to present everywhere.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .RwSet(Key).AddAsync(Element, MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var present = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .RwSet(Key).ContainsAsync(Element);
            Assert.That(present, Is.True,
                $"Site {i} did not converge to present after an add observing all removes.");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.RwSet, SiteCount);
        public ChaosDeliveryPump Pump { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            Pump = new ChaosDeliveryPump(Fixture, TreeName);
            Pump.Start();
        }

        public async ValueTask DisposeAsync()
        {
            if (Pump is not null)
            {
                await Pump.DisposeAsync();
            }
            await Fixture.DisposeAsync();
        }
    }
}
