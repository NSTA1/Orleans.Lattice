using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.OrFlag"/>
/// dispatch path. Several sites concurrently enable and disable a shared
/// flag while a partition isolates one site mid-workload; after the
/// partition heals and the delivery pump drains, every site must observe
/// the same enable-wins outcome.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.OrFlag</c> on every silo, so the producer side
/// emits typed <see cref="OrFlagDelta"/> on the WAL and the receiver
/// routes through <see cref="ReplicationApplier"/>'s typed-delta apply
/// path under <see cref="LatticeOriginContext"/> - the full mode-declaration
/// → producer-dispatch → receiver-merge pipeline this matrix exists to pin.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class OrFlagConvergenceChaosTests
{
    private const string TreeName = "chaos-orflag";
    private const string Key = "flag";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_enable_during_partition_wins_over_disable_at_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Phase 1: site 0 enables the flag and lets it converge so every
        // site has observed the enable dot.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .OrFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: partition site 2 off. Site 0 disables (observing only
        // the dots it has seen) while site 2, behind the partition,
        // concurrently authors a fresh enable with a dot site 0 cannot
        // observe.
        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        await Task.WhenAll(
            site0.OrFlag(Key).DisableAsync(),
            site2.OrFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(2)));

        // Phase 3: heal and drain. The enable-wins invariant requires every
        // site to converge to "enabled": site 2's enable dot was never in
        // site 0's tombstone set, so it survives the merge everywhere.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var enabled = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .OrFlag(Key).IsEnabledAsync();
            Assert.That(enabled, Is.True,
                $"Site {i} did not converge to enable-wins outcome.");
        }
    }

    [Test]
    public async Task Disable_observed_by_all_sites_converges_to_disabled()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Every site enables the flag and the topology drains so every
        // enable dot is observed everywhere.
        for (var i = 0; i < SiteCount; i++)
        {
            await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .OrFlag(Key).EnableAsync(MultiSiteClusterFixture.ClusterIdFor(i));
        }
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // One site disables after observing every enable dot, then drains.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName).OrFlag(Key).DisableAsync();
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var enabled = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .OrFlag(Key).IsEnabledAsync();
            Assert.That(enabled, Is.False,
                $"Site {i} did not converge to disabled after an observed disable.");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.OrFlag, SiteCount);
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
