using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.MvRegister"/>
/// dispatch path. Three sites issue concurrent writes against a single
/// key while partitions separate the concurrent writers; after the
/// partition heals and the delivery pump drains, every site must
/// observe exactly the multi-value frontier - the set of values whose
/// dots are not strictly dominated by any other authored dot.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.MvRegister</c> on every silo, so the producer
/// side stamps a typed CRDT delta on the WAL via
/// <c>LatticeDeltaContext</c> and the receiver routes through
/// <see cref="ReplicationApplier"/>'s
/// <c>ApplyStateMergeAsync&lt;MvRegister&gt;</c> path under
/// <see cref="LatticeOriginContext"/> - the full mode-declaration to
/// producer-dispatch to receiver-merge pipeline this matrix exists to
/// pin.
/// </para>
/// <para>
/// Unlike <see cref="LatticeMergeMode.LwwRegister"/>, MV-Register
/// must not silently drop concurrent writes: when sites write
/// concurrently behind a partition, every authored value whose dot
/// is not superseded by a later observed write from the same replica
/// must survive after the partition heals. Single-replica sequential
/// writes still collapse to the latest value because the new dot
/// strictly supersedes the older one in the dot context.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class MvRegisterConvergenceChaosTests
{
    private const string TreeName = "chaos-mvregister";
    private const string Key = "k";
    private const int SiteCount = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_writes_across_three_sites_under_partition_preserve_frontier()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Set every delivery gate before starting the pump, so no in-flight
        // delivery can make one writer observe another writer's dot.
        for (var i = 0; i < SiteCount; i++)
        {
            pump.IsolateSite(i);
        }
        AssertConnectivity(pump, partitioned: true);
        pump.Start();

        var perSiteValue = new string[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            perSiteValue[i] = $"site-{i}-v1";
        }

        var writeTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            writeTasks[siteIdx] = Task.Run(async () =>
            {
                await SetWithRetryAsync(lattice, MultiSiteClusterFixture.ClusterIdFor(siteIdx), perSiteValue[siteIdx]);
            });
        }

        await Task.WhenAll(writeTasks);
        AssertConnectivity(pump, partitioned: true);
        for (var i = 0; i < SiteCount; i++)
        {
            var values = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).MvRegister<string>(Key).ValuesAsync();
            Assert.That(values, Is.EqualTo(new[] { perSiteValue[i] }),
                $"Site {i} observed a foreign write before the partition healed.");
        }

        await pump.HealAllAndDrainAsync(DrainTimeout);
        AssertConnectivity(pump, partitioned: false);

        var expected = new HashSet<string>(perSiteValue, StringComparer.Ordinal);

        for (var i = 0; i < SiteCount; i++)
        {
            var values = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).MvRegister<string>(Key).ValuesAsync();
            var actual = values.ToHashSet(StringComparer.Ordinal);

            Assert.That(actual, Is.EquivalentTo(expected),
                $"Site {i} did not converge to the concurrent-write frontier.");
        }
    }

    [Test]
    public async Task Sequential_writes_from_one_replica_then_concurrent_writes_keep_only_frontier()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;
        pump.Start();

        // Phase 1: site 0 authors two sequential writes under no
        // partition. The second write strictly supersedes the first on
        // the same replica (counter bump), so after drain every site
        // should observe exactly {"site-0-v2"}.
        var lattice0 = fixture.ClientOf(0).GetGrain<ILattice>(TreeName).MvRegister<string>(Key);
        await SetWithRetryAsync(fixture.ClientOf(0).GetGrain<ILattice>(TreeName), MultiSiteClusterFixture.ClusterIdFor(0), "site-0-v1");
        await SetWithRetryAsync(fixture.ClientOf(0).GetGrain<ILattice>(TreeName), MultiSiteClusterFixture.ClusterIdFor(0), "site-0-v2");
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Phase 2: isolate site 2 and have sites 1 and 2 write
        // concurrently behind the partition. Site 1 sees site 0's
        // "site-0-v2" before writing (the pump has already delivered
        // it), so when site 1 writes "site-1-v1" its dot context
        // includes site 0's counter. Site 2 also sees "site-0-v2"
        // (the partition was healed before phase 2 started), then
        // gets isolated; its write "site-2-v1" is concurrent with
        // site 1's because the pump cannot deliver between them.
        pump.IsolateSite(2);
        Assert.That(pump.IsPartitioned(1, 2), Is.True);
        Assert.That(pump.IsPartitioned(2, 1), Is.True);
        Assert.That(pump.IsPartitioned(0, 1), Is.False);
        Assert.That(pump.IsPartitioned(1, 0), Is.False);

        var write1 = Task.Run(() => SetWithRetryAsync(fixture.ClientOf(1).GetGrain<ILattice>(TreeName), MultiSiteClusterFixture.ClusterIdFor(1), "site-1-v1"));
        var write2 = Task.Run(() => SetWithRetryAsync(fixture.ClientOf(2).GetGrain<ILattice>(TreeName), MultiSiteClusterFixture.ClusterIdFor(2), "site-2-v1"));
        await Task.WhenAll(write1, write2);

        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Expected frontier: {"site-1-v1", "site-2-v1"}. Site 0's
        // "site-0-v2" is strictly dominated because both site 1 and
        // site 2's dots observed its counter, so the merge supersedes
        // the site 0 entry on every replica.
        var expected = new HashSet<string>(new[] { "site-1-v1", "site-2-v1" }, StringComparer.Ordinal);

        for (var i = 0; i < SiteCount; i++)
        {
            var values = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).MvRegister<string>(Key).ValuesAsync();
            var actual = values.ToHashSet(StringComparer.Ordinal);

            Assert.That(actual, Is.EquivalentTo(expected),
                $"Site {i} did not converge to the frontier after supersession.");
        }
    }

    private static void AssertConnectivity(ChaosDeliveryPump pump, bool partitioned)
    {
        for (var sender = 0; sender < SiteCount; sender++)
        {
            for (var receiver = 0; receiver < SiteCount; receiver++)
            {
                if (sender == receiver)
                {
                    continue;
                }

                Assert.That(pump.IsPartitioned(sender, receiver), Is.EqualTo(partitioned),
                    $"Delivery edge {sender}->{receiver} must be {(partitioned ? "partitioned" : "healed")}.");
            }
        }
    }

    /// <summary>
    /// Wraps <see cref="MvRegisterAccessor{T}.SetAsync"/> in a bounded
    /// retry loop. A single CAS-budget exhaustion under chaos
    /// contention is not a correctness failure - the chaos pump is
    /// concurrently merging foreign-origin states onto the same key,
    /// racing the local CAS loop. Retry from the call site, mirroring
    /// what a real application would do.
    /// </summary>
    private static async Task SetWithRetryAsync(ILattice lattice, string replicaId, string value)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                await lattice.MvRegister<string>(Key).SetAsync(replicaId, value);
                return;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CAS budget exhausted", StringComparison.Ordinal))
            {
                await Task.Delay(5 * (attempt + 1));
            }
        }

        await lattice.MvRegister<string>(Key).SetAsync(replicaId, value);
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.MvRegister, SiteCount);
        public ChaosDeliveryPump Pump { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            Pump = new ChaosDeliveryPump(Fixture, TreeName);
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
