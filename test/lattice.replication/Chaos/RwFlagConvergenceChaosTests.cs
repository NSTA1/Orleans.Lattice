using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.RwFlag"/>
/// dispatch path. Several sites concurrently enable and disable a shared
/// flag while a partition isolates one site mid-workload; after the
/// partition heals and the delivery pump drains, every site must observe
/// the same remove-wins outcome.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.RwFlag</c> on every silo, so the producer side
/// emits typed <see cref="RwFlagDelta"/> on the WAL and the receiver
/// routes through <see cref="ReplicationApplier"/>'s typed-delta apply
/// path under <see cref="LatticeOriginContext"/> - the full mode-declaration
/// → producer-dispatch → receiver-merge pipeline this matrix exists to pin.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class RwFlagConvergenceChaosTests
{
    private const string TreeName = "chaos-rwflag";
    private const string Key = "flag";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_disable_during_partition_wins_over_enable_at_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Phase 1: site 0 enables the flag and lets it converge so every
        // site has observed the enable dot.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .RwFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: partition site 2 off. Site 0 disables (minting a fresh
        // disable dot) while site 2, behind the partition, concurrently
        // re-enables with a dot that never observed site 0's disable.
        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        await Task.WhenAll(
            site0.RwFlag(Key).DisableAsync(MultiSiteClusterFixture.ClusterIdFor(0)),
            site2.RwFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(2)));

        // Phase 3: heal and drain. The remove-wins invariant requires every
        // site to converge to "disabled": site 0's disable dot was never in
        // site 2's tombstone set, so it survives the merge everywhere.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var enabled = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .RwFlag(Key).IsEnabledAsync();
            Assert.That(enabled, Is.False,
                $"Site {i} did not converge to remove-wins outcome.");
        }
    }

    [Test]
    public async Task Enable_observing_all_disables_converges_to_enabled()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Site 0 enables the flag and the topology drains so the enable dot
        // is observed everywhere.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .RwFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Every site disables (minting its own disable dot) and the topology
        // drains so every disable dot is observed everywhere.
        for (var i = 0; i < SiteCount; i++)
        {
            await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .RwFlag(Key).DisableAsync(MultiSiteClusterFixture.ClusterIdFor(i));
        }
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // One site re-enables after observing every disable dot, then drains.
        // Because the enable tombstones every observed disable, no disable
        // survives and the flag converges to enabled everywhere.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .RwFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var enabled = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .RwFlag(Key).IsEnabledAsync();
            Assert.That(enabled, Is.True,
                $"Site {i} did not converge to enabled after an enable observing all disables.");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.RwFlag, SiteCount);
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
