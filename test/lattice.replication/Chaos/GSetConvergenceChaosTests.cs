using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.GSet"/>
/// dispatch path. Several sites concurrently add distinct elements to a
/// shared grow-only set while a partition isolates one site mid-workload;
/// after the partition heals and the delivery pump drains, every site must
/// observe the union of all additions.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.GSet</c> on every silo, so the producer side emits
/// typed <see cref="GSetDelta"/> on the WAL and the receiver routes through
/// <see cref="ReplicationApplier"/>'s typed-delta apply path under
/// <see cref="LatticeOriginContext"/> - the full mode-declaration ->
/// producer-dispatch -> receiver-merge pipeline this matrix exists to pin.
/// Because the set is add-only and merge is set union, the outcome is order
/// and timing independent: every element added at any site survives at every
/// site regardless of interleaving.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class GSetConvergenceChaosTests
{
    private const string TreeName = "chaos-gset";
    private const string Key = "elements";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    [Test]
    public async Task Concurrent_adds_during_partition_converge_to_the_union_at_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Phase 1: site 0 seeds an element and lets it converge so every
        // site has observed at least one add.
        await fixture.ClientOf(0).GetGrain<ILattice>(TreeName)
            .GSet(Key).AddAsync(Bytes("seed"));
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: partition site 2 off. Each site concurrently adds a
        // distinct element; site 2's add is minted behind the partition and
        // never observed by the others until the heal.
        pump.IsolateSite(2);

        await Task.WhenAll(
            fixture.ClientOf(0).GetGrain<ILattice>(TreeName).GSet(Key).AddAsync(Bytes("site-0")),
            fixture.ClientOf(1).GetGrain<ILattice>(TreeName).GSet(Key).AddAsync(Bytes("site-1")),
            fixture.ClientOf(2).GetGrain<ILattice>(TreeName).GSet(Key).AddAsync(Bytes("site-2")));

        // Phase 3: heal and drain. Set union is commutative, associative, and
        // idempotent, so every site must observe the full union - no add is
        // lost regardless of the partition or the interleaving.
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var set = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).GSet(Key).GetAsync();
            Assert.Multiple(() =>
            {
                Assert.That(set.Contains(Bytes("seed")), Is.True, $"Site {i} lost the seed element.");
                Assert.That(set.Contains(Bytes("site-0")), Is.True, $"Site {i} lost site 0's add.");
                Assert.That(set.Contains(Bytes("site-1")), Is.True, $"Site {i} lost site 1's add.");
                Assert.That(set.Contains(Bytes("site-2")), Is.True, $"Site {i} lost site 2's partitioned add.");
            });
        }
    }

    [Test]
    public async Task Duplicate_adds_across_sites_converge_idempotently()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Every site adds the same element. Union of identical singletons is a
        // single-member set, so the flag converges without duplication.
        for (var i = 0; i < SiteCount; i++)
        {
            await fixture.ClientOf(i).GetGrain<ILattice>(TreeName)
                .GSet(Key).AddAsync(Bytes("shared"));
        }
        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var set = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).GSet(Key).GetAsync();
            Assert.Multiple(() =>
            {
                Assert.That(set.Contains(Bytes("shared")), Is.True, $"Site {i} lost the shared element.");
                Assert.That(set.Count, Is.EqualTo(1), $"Site {i} did not converge idempotently.");
            });
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.GSet, SiteCount);
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
