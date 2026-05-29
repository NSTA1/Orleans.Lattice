using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.OrMap"/>
/// dispatch path. Three sites issue concurrent writes against a single
/// <c>OrMap&lt;string, PnCounter&gt;</c> key while a partition isolates
/// one site mid-workload; after the partition heals and the delivery
/// pump drains, every site must observe the same map. The map's merge
/// semantics are add-wins on per-key entries with element-wise PnCounter
/// merge on values: any map key written under a fresh dot on any site
/// survives unless every observed dot for it has been tombstoned by a
/// later observed remove; per-key PnCounter values converge to the
/// algebraic sum of authored deltas across every replica.
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.OrMap</c> on every silo AND registers the
/// <c>(string, PnCounter)</c> shape on each silo's
/// <see cref="CrdtShapeRegistry"/>, so the producer side stamps the
/// typed delta on the WAL via <c>LatticeDeltaContext</c> and the
/// receiver routes through <c>ReplicationApplier</c>'s
/// <c>ApplyStateMergeAsync&lt;OrMap&lt;TKey, TValue&gt;&gt;</c> path -
/// the full mode-declaration to producer-dispatch to receiver-merge
/// pipeline this matrix exists to pin.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class OrMapConvergenceChaosTests
{
    private const string TreeName = "chaos-ormap";
    private const string Key = "k";
    private const int SiteCount = 3;
    private const int MapKeysPerSite = 3;
    private const long IncrementsPerMapKey = 2;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Concurrent_or_map_writes_across_three_sites_under_partition_converge()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Each site authors a disjoint family of map keys
        // "site-i-mapkey-N" for N in [0, MapKeysPerSite). Each entry's
        // value is a PnCounter initialised to IncrementsPerMapKey via
        // local increments from the authoring site's replica id, so the
        // expected per-key value is well-defined regardless of the
        // partition timing: PnCounter merge is the sum of per-replica
        // counter deltas, and every map key is authored by exactly one
        // site's replica id.

        // Isolate site 2 mid-run; sites 0/1 see each other through the
        // pump while site 2's writes accumulate behind the partition.
        var addTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            var replicaId = MultiSiteClusterFixture.ClusterIdFor(siteIdx);
            addTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < MapKeysPerSite; n++)
                {
                    var mapKey = $"site-{siteIdx}-mapkey-{n}";
                    var counter = new PnCounter();
                    for (var k = 0; k < IncrementsPerMapKey; k++)
                    {
                        counter.Increment(replicaId);
                    }
                    await SetWithRetryAsync(lattice, mapKey, replicaId, counter);

                    if (siteIdx == 0 && n == MapKeysPerSite / 3)
                    {
                        pump.IsolateSite(2);
                    }
                    if (siteIdx == 0 && n == (2 * MapKeysPerSite) / 3)
                    {
                        pump.HealSite(2);
                    }
                }
            });
        }

        await Task.WhenAll(addTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Expected: every authored (site, mapkey) is present, and its
        // PnCounter value equals IncrementsPerMapKey.
        var expectedMapKeys = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < SiteCount; i++)
        {
            for (var n = 0; n < MapKeysPerSite; n++)
            {
                expectedMapKeys.Add($"site-{i}-mapkey-{n}");
            }
        }

        for (var i = 0; i < SiteCount; i++)
        {
            var observed = await fixture.ClientOf(i)
                .GetGrain<ILattice>(TreeName)
                .OrMap<string, PnCounter>(Key)
                .GetAsync();

            var observedKeys = observed.Keys().ToHashSet(StringComparer.Ordinal);
            Assert.That(observedKeys, Is.EquivalentTo(expectedMapKeys),
                $"Site {i} did not converge to the union of authored map keys.");

            foreach (var mapKey in expectedMapKeys)
            {
                var pn = observed.Get(mapKey);
                Assert.That(pn, Is.Not.Null,
                    $"Site {i} missing map key '{mapKey}' after drain.");
                Assert.That(pn!.Value, Is.EqualTo(IncrementsPerMapKey),
                    $"Site {i} PnCounter at map key '{mapKey}' converged to {pn.Value}, expected {IncrementsPerMapKey}.");
            }
        }
    }

    /// <summary>
    /// Wraps <see cref="OrMapAccessor{TKey, TValue}.SetAsync"/> in a
    /// bounded retry loop. CAS-budget exhaustion under chaos contention
    /// is not a correctness failure - the chaos pump is concurrently
    /// merging foreign-origin states onto the same key, racing the
    /// local CAS loop. Retry from the call site, mirroring what
    /// <c>MvRegisterConvergenceChaosTests.SetWithRetryAsync</c> does.
    /// </summary>
    private static async Task SetWithRetryAsync(ILattice lattice, string mapKey, string replicaId, PnCounter value)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                await lattice.OrMap<string, PnCounter>(Key).SetAsync(mapKey, replicaId, value);
                return;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CAS budget exhausted", StringComparison.Ordinal))
            {
                await Task.Delay(5 * (attempt + 1));
            }
        }

        await lattice.OrMap<string, PnCounter>(Key).SetAsync(mapKey, replicaId, value);
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.OrMap, SiteCount);
        public ChaosDeliveryPump Pump { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            // OR-Map requires per-(tree, TKey, TValue) shape registration
            // on every silo before any producer write resolves. The
            // closed-shape modes (OrSet, PnCounter, VersionVector,
            // MvRegister) don't need this because their descriptors are
            // unambiguous, but the parameterised OR-Map shape carries
            // the chosen value-CRDT type and must be registered explicitly.
            Fixture.RegisterOrMapShape<string, PnCounter>(TreeName);
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
