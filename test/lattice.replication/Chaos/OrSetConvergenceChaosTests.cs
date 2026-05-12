using Orleans.Lattice.BPlusTree.Grains;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.OrSet"/>
/// dispatch path. Three sites issue concurrent OR-Set adds against a
/// single key while a partition isolates one site mid-workload; after
/// the partition heals and the delivery pump drains, every site must
/// observe exactly the union of every authored add.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.OrSet</c> on every silo, so the producer side
/// emits typed CRDT state on the WAL and the receiver routes
/// through <see cref="ReplicationApplier"/>'s
/// <c>ApplyStateMergeAsync&lt;OrSet&gt;</c> path under
/// <see cref="LatticeOriginContext"/> - the full mode-declaration →
/// producer-dispatch → receiver-merge pipeline this matrix exists to
/// pin.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class OrSetConvergenceChaosTests
{
    private const string TreeName = "chaos-orset";
    private const string Key = "k";
    private const int SiteCount = 3;
    private const int AddsPerSite = 25;
    private const int AddsPerSiteWithRemoves = 15;
    private const int RemovesPerSite = 2;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_adds_across_three_sites_under_partition_converge_to_union()
    {
        await using var pumpRunner = new TestRunner();
        await pumpRunner.InitializeAsync();
        var fixture = pumpRunner.Fixture;
        var pump = pumpRunner.Pump;

        // Every site authors a disjoint family of elements ("site-i-elem-N")
        // so the post-convergence union has a known cardinality.
        var perSiteElements = new byte[SiteCount][][];
        for (var i = 0; i < SiteCount; i++)
        {
            perSiteElements[i] = new byte[AddsPerSite][];
            for (var n = 0; n < AddsPerSite; n++)
            {
                perSiteElements[i][n] = Encoding.UTF8.GetBytes($"site-{i}-elem-{n}");
            }
        }

        var workloadTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            workloadTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < AddsPerSite; n++)
                {
                    await lattice.OrSet(Key).AddAsync(perSiteElements[siteIdx][n], MultiSiteClusterFixture.ClusterIdFor(siteIdx));

                    // Mid-workload partition: drop site 2 from the topology
                    // when site 0 reaches the half-way point. The other
                    // sites continue authoring; site 2 keeps authoring its
                    // own batch behind the partition.
                    if (siteIdx == 0 && n == AddsPerSite / 2)
                    {
                        pump.IsolateSite(2);
                    }

                    if (siteIdx == 0 && n == AddsPerSite - 1)
                    {
                        // Heal half a poll-cycle before workload completion
                        // so the pump observes a fully-healed topology
                        // before the drain begins.
                        await Task.Delay(150);
                        pump.HealSite(2);
                    }
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < SiteCount; i++)
        {
            for (var n = 0; n < AddsPerSite; n++)
            {
                expected.Add(Encoding.UTF8.GetString(perSiteElements[i][n]));
            }
        }

        for (var i = 0; i < SiteCount; i++)
        {
            var observed = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).OrSet(Key).GetAsync();
            var actual = observed.Elements()
                .Select(e => Encoding.UTF8.GetString(e))
                .ToHashSet(StringComparer.Ordinal);

            Assert.That(actual, Is.EquivalentTo(expected),
                $"Site {i} did not converge to the union of authored adds.");
        }
    }

    /// <summary>
    /// Convergence under concurrent adds <em>and</em> observed-removes:
    /// every site authors a disjoint family of elements, the topology
    /// is partitioned mid-workload to force divergent histories, and
    /// after the partition heals every site removes a small subset of
    /// the elements it authored (so each remove sees its own dots and
    /// is therefore guaranteed to take effect). After drain, every site
    /// must observe exactly the union of authored adds minus the union
    /// of authored removes - the canonical OR-Set invariant under
    /// concurrent mutation.
    /// </summary>
    [Test]
    public async Task Concurrent_adds_and_removes_across_three_sites_under_partition_converge()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Site i authors elements "site-i-elem-N" for N in [0, AddsPerSiteWithRemoves).
        var perSiteElements = new byte[SiteCount][][];
        for (var i = 0; i < SiteCount; i++)
        {
            perSiteElements[i] = new byte[AddsPerSiteWithRemoves][];
            for (var n = 0; n < AddsPerSiteWithRemoves; n++)
            {
                perSiteElements[i][n] = Encoding.UTF8.GetBytes($"site-{i}-rm-elem-{n}");
            }
        }

        // Phase 1: every site authors its add batch under a partition
        // that isolates site 2 mid-run, so site 2 accumulates a private
        // history while sites 0/1 see each other.
        var addTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            addTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < AddsPerSiteWithRemoves; n++)
                {
                    await lattice.OrSet(Key).AddAsync(perSiteElements[siteIdx][n], MultiSiteClusterFixture.ClusterIdFor(siteIdx));

                    if (siteIdx == 0 && n == AddsPerSiteWithRemoves / 3)
                    {
                        pump.IsolateSite(2);
                    }

                    if (siteIdx == 0 && n == (2 * AddsPerSiteWithRemoves) / 3)
                    {
                        await Task.Delay(150);
                        pump.HealSite(2);
                    }
                }
            });
        }

        await Task.WhenAll(addTasks);

        // Phase 2: heal every edge and let the topology fully drain so
        // that each site has observed every authored add before issuing
        // its removes. This pins the convergence test to the
        // observed-remove semantics: a site can only delete dots it has
        // seen, so every site removing its own authored elements is
        // guaranteed to take effect on every site once the removes
        // propagate.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 3: every site removes RemovesPerSite of its own
        // authored elements. Removing one's own dots is always a safe
        // observed-remove because the authoring site has the dots in
        // its local state by construction.
        var removeTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            removeTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < RemovesPerSite; n++)
                {
                    await lattice.OrSet(Key).RemoveAsync(perSiteElements[siteIdx][n]);
                }
            });
        }

        await Task.WhenAll(removeTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Expected: union of every authored add minus every authored remove.
        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < SiteCount; i++)
        {
            for (var n = 0; n < AddsPerSiteWithRemoves; n++)
            {
                expected.Add(Encoding.UTF8.GetString(perSiteElements[i][n]));
            }
        }
        for (var i = 0; i < SiteCount; i++)
        {
            for (var n = 0; n < RemovesPerSite; n++)
            {
                expected.Remove(Encoding.UTF8.GetString(perSiteElements[i][n]));
            }
        }

        for (var i = 0; i < SiteCount; i++)
        {
            var observed = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).OrSet(Key).GetAsync();
            var actual = observed.Elements()
                .Select(e => Encoding.UTF8.GetString(e))
                .ToHashSet(StringComparer.Ordinal);

            Assert.That(actual, Is.EquivalentTo(expected),
                $"Site {i} did not converge to union(adds) − union(removes).");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.OrSet, SiteCount);
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
