using System.Buffers.Binary;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the bounded-register dispatch paths
/// (<see cref="LatticeMergeMode.MaxRegister"/> and
/// <see cref="LatticeMergeMode.MinRegister"/>). Several sites concurrently
/// advance a shared register while a partition isolates one site mid-workload;
/// after the partition heals and the delivery pump drains, every site must
/// observe the same directional extreme.
/// <para>
/// The fixture configures the test tree with the register merge mode on every
/// silo, so the producer side emits a typed <see cref="BoundedRegisterDelta"/>
/// on the WAL and the receiver routes through <see cref="ReplicationApplier"/>'s
/// typed-delta apply path under <see cref="LatticeOriginContext"/> - the full
/// mode-declaration then producer-dispatch then receiver-merge pipeline this
/// matrix exists to pin. The write carries an explicit total-order key so the
/// receiver folds the directional max/min without the domain comparer; the fold
/// is commutative, associative, and idempotent, so any delivery interleaving
/// converges on the single extreme value.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class BoundedRegisterConvergenceChaosTests
{
    private const string Key = "gauge";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    private static byte[] OrderKey(int value)
    {
        var buffer = new byte[4];
        // Bias to unsigned so the wire order matches numeric order for the
        // non-negative values this test uses.
        BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)value);
        return buffer;
    }

    [Test]
    public async Task Max_register_converges_to_the_greatest_value_at_every_site()
    {
        await using var runner = new TestRunner(LatticeMergeMode.MaxRegister, "chaos-maxregister");
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Seed a low value everywhere so every site has observed a write.
        await fixture.ClientOf(0).GetGrain<ILattice>(runner.TreeName)
            .MaxRegister<int>(Key, OrderKey).SetAsync(5);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Partition site 2 off, then race divergent advances. Site 0 pushes the
        // eventual winner while the isolated site 2 pushes a lower candidate.
        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(runner.TreeName);
        var site1 = fixture.ClientOf(1).GetGrain<ILattice>(runner.TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(runner.TreeName);

        await Task.WhenAll(
            site0.MaxRegister<int>(Key, OrderKey).SetAsync(90),
            site1.MaxRegister<int>(Key, OrderKey).SetAsync(42),
            site2.MaxRegister<int>(Key, OrderKey).SetAsync(17));

        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var value = await fixture.ClientOf(i).GetGrain<ILattice>(runner.TreeName)
                .MaxRegister<int>(Key, OrderKey).GetAsync();
            Assert.That(value, Is.EqualTo(90),
                $"Site {i} did not converge to the max-register extreme.");
        }
    }

    [Test]
    public async Task Min_register_converges_to_the_smallest_value_at_every_site()
    {
        await using var runner = new TestRunner(LatticeMergeMode.MinRegister, "chaos-minregister");
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Seed a high value everywhere so every site has observed a write.
        await fixture.ClientOf(0).GetGrain<ILattice>(runner.TreeName)
            .MinRegister<int>(Key, OrderKey).SetAsync(95);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        pump.IsolateSite(2);

        var site0 = fixture.ClientOf(0).GetGrain<ILattice>(runner.TreeName);
        var site1 = fixture.ClientOf(1).GetGrain<ILattice>(runner.TreeName);
        var site2 = fixture.ClientOf(2).GetGrain<ILattice>(runner.TreeName);

        await Task.WhenAll(
            site0.MinRegister<int>(Key, OrderKey).SetAsync(10),
            site1.MinRegister<int>(Key, OrderKey).SetAsync(58),
            site2.MinRegister<int>(Key, OrderKey).SetAsync(83));

        await pump.HealAllAndDrainAsync(DrainTimeout);

        for (var i = 0; i < SiteCount; i++)
        {
            var value = await fixture.ClientOf(i).GetGrain<ILattice>(runner.TreeName)
                .MinRegister<int>(Key, OrderKey).GetAsync();
            Assert.That(value, Is.EqualTo(10),
                $"Site {i} did not converge to the min-register extreme.");
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public TestRunner(LatticeMergeMode mode, string treeName)
        {
            TreeName = treeName;
            Fixture = new MultiSiteClusterFixture(mode, SiteCount);
        }

        public string TreeName { get; }

        public MultiSiteClusterFixture Fixture { get; }

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
