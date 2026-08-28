using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.VersionVector"/>
/// dispatch path. Several sites concurrently tick a shared causal-history
/// vector while a partition isolates one site mid-workload; after the partition
/// heals and the delivery pump drains, every site must observe the same
/// converged vector - the pointwise-max-per-replica join of every site's
/// contribution.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.VersionVector</c> on every silo, so the producer side
/// emits typed <see cref="VersionVectorDelta"/> on the WAL and the receiver
/// routes through <see cref="ReplicationApplier"/>'s typed-delta apply path
/// under <see cref="LatticeOriginContext"/> - the full mode-declaration ->
/// producer-dispatch -> receiver-merge pipeline this matrix exists to pin.
/// <see cref="LatticeMergeMode.VersionVector"/> was the one replicable merge
/// mode with no cross-cluster convergence coverage; every sibling mode already
/// has a fixture in this directory.
/// </para>
/// <para>
/// Convergence is asserted structurally rather than through a single scalar
/// projection: a version vector's whole entry map is its observable value, so
/// each site's vector is compared entry-by-entry against site 0's, and mutual
/// <see cref="VersionVector.DominatesOrEquals"/> is asserted in both directions
/// (mutual domination is anti-symmetric, so it holds only when the two vectors
/// are equal - the lattice-level statement of "these replicas converged").
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class VersionVectorConvergenceChaosTests
{
    private const string TreeName = "chaos-versionvector";
    private const string Key = "frontier";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    // Asserts that every site holds exactly the same vector as site 0, by
    // entry-wise clock equality and by mutual domination. Mutual domination is
    // the lattice formulation: A dominates B and B dominates A only when the
    // two carry identical causal information.
    private static void AssertAllSitesConverged(IReadOnlyList<VersionVector> perSite, string because)
    {
        var reference = perSite[0];
        for (var i = 1; i < perSite.Count; i++)
        {
            var actual = perSite[i];
            Assert.Multiple(() =>
            {
                Assert.That(
                    actual.Entries.Keys,
                    Is.EquivalentTo(reference.Entries.Keys),
                    $"Site {i} does not carry the same replica set as site 0 ({because}).");

                foreach (var (replicaId, clock) in reference.Entries)
                {
                    Assert.That(
                        actual.GetClock(replicaId),
                        Is.EqualTo(clock),
                        $"Site {i} disagrees with site 0 on replica '{replicaId}' ({because}).");
                }

                Assert.That(
                    actual.DominatesOrEquals(reference),
                    Is.True,
                    $"Site {i} does not dominate site 0 ({because}).");
                Assert.That(
                    reference.DominatesOrEquals(actual),
                    Is.True,
                    $"Site 0 does not dominate site {i} ({because}).");
            });
        }
    }

    private static async Task<IReadOnlyList<VersionVector>> ReadAllSitesAsync(MultiSiteClusterFixture fixture)
    {
        var vectors = new List<VersionVector>(SiteCount);
        for (var i = 0; i < SiteCount; i++)
        {
            vectors.Add(await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).VersionVector(Key).GetAsync());
        }
        return vectors;
    }

    [Test]
    public async Task Concurrent_ticks_during_partition_converge_to_the_pointwise_max_at_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Phase 1: site 0 ticks and lets it converge, so every site has
        // observed site 0's component before the partition opens.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .VersionVector(Key).TickAsync(MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: partition site 2 off. Site 0 and site 2 both advance their
        // own components concurrently; site 2's advance is invisible to the
        // rest of the topology until the partition heals.
        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(TreeName);

        await Task.WhenAll(
            site0.VersionVector(Key).TickAsync(MultiSiteClusterFixture.ClusterIdFor(0)),
            site2.VersionVector(Key).TickAsync(MultiSiteClusterFixture.ClusterIdFor(2)));

        // Phase 3: heal and drain. Each site only ever advances its own
        // component and the receiver merges by pointwise-max, so both
        // components must survive the partition at every site.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        var vectors = await ReadAllSitesAsync(fixture);
        AssertAllSitesConverged(vectors, "after a partitioned concurrent tick");

        for (var i = 0; i < SiteCount; i++)
        {
            Assert.Multiple(() =>
            {
                Assert.That(
                    vectors[i].GetClock(MultiSiteClusterFixture.ClusterIdFor(0)),
                    Is.GreaterThan(HybridLogicalClock.Zero),
                    $"Site {i} lost site 0's component.");
                Assert.That(
                    vectors[i].GetClock(MultiSiteClusterFixture.ClusterIdFor(2)),
                    Is.GreaterThan(HybridLogicalClock.Zero),
                    $"Site {i} lost site 2's partitioned component.");
            });
        }
    }

    [Test]
    public async Task Every_site_ticks_and_all_converge_to_the_same_vector()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Every site advances its own component; the topology drains once at
        // the end so the deliveries interleave freely rather than being
        // serialised round by round.
        for (var i = 0; i < SiteCount; i++)
        {
            await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .VersionVector(Key).TickAsync(MultiSiteClusterFixture.ClusterIdFor(i));
        }
        await pump.HealAllAndDrainAsync(DrainTimeout);

        var vectors = await ReadAllSitesAsync(fixture);
        AssertAllSitesConverged(vectors, "after every site ticked its own component");

        for (var i = 0; i < SiteCount; i++)
        {
            for (var replica = 0; replica < SiteCount; replica++)
            {
                Assert.That(
                    vectors[i].GetClock(MultiSiteClusterFixture.ClusterIdFor(replica)),
                    Is.GreaterThan(HybridLogicalClock.Zero),
                    $"Site {i} never observed replica {replica}'s component.");
            }
        }
    }

    [Test]
    public async Task Redelivered_ticks_converge_idempotently()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .VersionVector(Key).TickAsync(MultiSiteClusterFixture.ClusterIdFor(0));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        var settled = await ReadAllSitesAsync(fixture);
        AssertAllSitesConverged(settled, "after the initial tick settled");

        // Pointwise-max is idempotent, so re-draining the topology (which can
        // re-deliver an already-applied tick) must not advance any component:
        // a duplicate delivery that bumped a clock would break convergence
        // against a peer that received it exactly once.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        var afterRedelivery = await ReadAllSitesAsync(fixture);
        AssertAllSitesConverged(afterRedelivery, "after a redelivery drain");

        for (var i = 0; i < SiteCount; i++)
        {
            Assert.That(
                afterRedelivery[i].GetClock(MultiSiteClusterFixture.ClusterIdFor(0)),
                Is.EqualTo(settled[i].GetClock(MultiSiteClusterFixture.ClusterIdFor(0))),
                $"Site {i} advanced its clock on a redelivery - the merge is not idempotent.");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.VersionVector, SiteCount);
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
