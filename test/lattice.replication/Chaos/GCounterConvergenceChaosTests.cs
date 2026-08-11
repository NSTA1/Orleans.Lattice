using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.GCounter"/>
/// dispatch path. Several sites concurrently increment a shared grow-only
/// counter while a partition isolates one site mid-workload; after the
/// partition heals and the delivery pump drains, every site must observe the
/// same converged total - the pointwise-max-per-replica join of every site's
/// contribution.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.GCounter</c> on every silo, so the producer side emits
/// typed <see cref="GCounterDelta"/> on the WAL and the receiver routes through
/// <see cref="ReplicationApplier"/>'s typed-delta apply path under
/// <see cref="LatticeOriginContext"/> - the full mode-declaration ->
/// producer-dispatch -> receiver-merge pipeline this matrix exists to pin.
/// Because a grow-only counter is commutative, associative, and idempotent,
/// the converged total is independent of delivery order and duplicate delivery.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class GCounterConvergenceChaosTests
{
    private const string TreeName = "chaos-gcounter";
    private const string Key = "count";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_increments_during_partition_converge_to_the_summed_total_at_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Phase 1: site 0 increments and lets it converge so every site has
        // observed site 0's component.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .GCounter(Key).IncrementAsync(MultiSiteClusterFixture.ClusterIdFor(0), 5);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: partition site 2 off. Site 0 and site 2 both advance their
        // own grow-only components concurrently; site 2's advance is invisible
        // to the rest of the topology until the partition heals.
        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        await Task.WhenAll(
            site0.GCounter(Key).IncrementAsync(MultiSiteClusterFixture.ClusterIdFor(0), 3),
            site2.GCounter(Key).IncrementAsync(MultiSiteClusterFixture.ClusterIdFor(2), 7));

        // Phase 3: heal and drain. Each site only ever advances its own
        // component, and the receiver merges by pointwise-max, so the
        // converged total is 5 + 3 (site 0) + 7 (site 2) = 15 everywhere.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var value = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .GCounter(Key).ValueAsync();
            Assert.That(value, Is.EqualTo(15),
                $"Site {i} did not converge to the summed grow-only total.");
        }
    }

    [Test]
    public async Task Every_site_increments_and_all_converge_to_the_same_total()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Every site advances its own grow-only component by a distinct
        // amount; the topology drains between rounds so every component is
        // observed everywhere.
        long expected = 0;
        for (var i = 0; i < SiteCount; i++)
        {
            var amount = (i + 1) * 4;
            expected += amount;
            await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .GCounter(Key).IncrementAsync(MultiSiteClusterFixture.ClusterIdFor(i), amount);
        }
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var value = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .GCounter(Key).ValueAsync();
            Assert.That(value, Is.EqualTo(expected),
                $"Site {i} did not converge to the summed total of every site's contribution.");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.GCounter, SiteCount);
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
