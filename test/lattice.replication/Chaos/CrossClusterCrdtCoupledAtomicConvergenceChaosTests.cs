using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage for typed CRDT mutations coupled into <b>cross-tree</b>
/// atomic writes under fault injection. Each site stages a same-key CRDT
/// mutation (a PN-counter increment or an OR-Set add) and commits it in one
/// all-or-nothing cross-tree atomic write alongside a sibling
/// last-writer-wins (LWW) entry, while the inter-site delivery topology for
/// both trees is partitioned and healed mid-workload.
/// <para>
/// The cross-tree atomic-write path rides the two-phase prepared / terminal
/// replication path, which carries each staged typed delta and its merge
/// mode through to the receiver. The receiver folds the per-replica delta
/// into its current visible state on the saga's terminal commit. So
/// concurrent same-key staged-CRDT atomic writes from every site must
/// converge by the per-replica typed-delta <em>union</em> - a PN-counter to
/// the sum of every site's increments, an OR-Set to the union of every
/// site's added elements - identical to the live (non-atomic) accessor path,
/// rather than collapsing to last-writer-wins of the merged states. The
/// coupled LWW sibling tree must retain its cross-tree all-or-nothing
/// visibility: after a full drain every authored saga's CRDT contribution
/// and its sibling LWW key are both present on every site.
/// </para>
/// <para>
/// This is the convergent-union counterpart to
/// <see cref="CrossClusterCrossTreeAtomicVisibilityChaosTests"/>, which pins
/// the disjoint-key all-or-nothing visibility invariant for value-only
/// (LWW) cross-tree batches. Here both invariants are exercised together:
/// the CRDT tree converges by union while its LWW sibling stays
/// all-or-nothing.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CrossClusterCrdtCoupledAtomicConvergenceChaosTests
{
    private const string CounterTree = "chaos-crdt-pncounter";
    private const string SetTree = "chaos-crdt-orset";
    private const string LwwTree = "chaos-crdt-lww-sibling";
    private const string CounterKey = "votes";
    private const string SetKey = "members";
    private const int SiteCount = 2;
    private const int SagasPerSite = 6;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Concurrent_same_key_pncounter_atomic_writes_converge_to_the_sum_with_lww_sibling_all_or_nothing()
    {
        await using var runner = new TestRunner(CounterTree, LatticeMergeMode.PnCounter);
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var crdtPump = runner.CrdtPump;
        var lwwPump = runner.LwwPump;

        var workloadTasks = new Task[SiteCount];
        for (var site = 0; site < SiteCount; site++)
        {
            var siteIdx = site;
            var factory = fixture.ClientOf(siteIdx);
            var counter = factory.GetGrain<ILattice>(CounterTree);
            workloadTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    // A distinct replica id per increment keeps each staged
                    // delta independent of the local saga commit's
                    // read-your-writes visibility: a fresh replica component
                    // always advances to 1 regardless of whether the prior
                    // commit is locally visible yet, so the per-replica delta
                    // union is a deterministic count of every authored
                    // increment rather than a same-replica accumulation that
                    // races local commit visibility.
                    var replicaId = $"{MultiSiteClusterFixture.ClusterIdFor(siteIdx)}-r{n:D2}";
                    var staged = await counter.PnCounter(CounterKey).StageIncrementAsync(replicaId, 1);
                    var lwwKey = $"s{siteIdx}-lww{n:D2}";
                    await factory.BeginAtomicWrite($"pncsaga-s{siteIdx}-{n:D2}")
                        .ForTree(CounterTree).Set(staged)
                        .ForTree(LwwTree).Set(lwwKey, Bytes($"v-{siteIdx}-{n}"))
                        .CommitAsync();

                    CyclePartitions(siteIdx, n, crdtPump, lwwPump);
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await crdtPump.HealAllAndDrainAsync(DrainTimeout);
        await lwwPump.HealAllAndDrainAsync(DrainTimeout);

        // Union convergence: every site's PN-counter equals the count of
        // every site's increments (each authored increment advances a
        // distinct replica component by 1, so the per-replica union sums to
        // the total number of staged increments).
        var expectedSum = (long)SiteCount * SagasPerSite;
        for (var receiver = 0; receiver < SiteCount; receiver++)
        {
            var value = await fixture.ClientOf(receiver)
                .GetGrain<ILattice>(CounterTree)
                .PnCounter(CounterKey)
                .ValueAsync();
            Assert.That(value, Is.EqualTo(expectedSum),
                $"Site {receiver} PN-counter '{CounterKey}' must converge to the per-replica "
                + $"delta union (sum = {expectedSum}) after a full drain, not last-writer-wins.");
        }

        // Sibling LWW tree retains cross-tree all-or-nothing: after the drain
        // every authored saga's LWW key is present on every site.
        await AssertLwwSiblingFullyVisibleAsync(fixture);
    }

    [Test]
    public async Task Concurrent_same_key_orset_atomic_writes_converge_to_the_membership_union_with_lww_sibling_all_or_nothing()
    {
        await using var runner = new TestRunner(SetTree, LatticeMergeMode.OrSet);
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var crdtPump = runner.CrdtPump;
        var lwwPump = runner.LwwPump;

        var workloadTasks = new Task[SiteCount];
        for (var site = 0; site < SiteCount; site++)
        {
            var siteIdx = site;
            var replicaId = MultiSiteClusterFixture.ClusterIdFor(siteIdx);
            var factory = fixture.ClientOf(siteIdx);
            var set = factory.GetGrain<ILattice>(SetTree);
            workloadTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    var element = Bytes($"s{siteIdx}-e{n:D2}");
                    var staged = await set.OrSet(SetKey).StageAddAsync(element, replicaId);
                    var lwwKey = $"s{siteIdx}-lww{n:D2}";
                    await factory.BeginAtomicWrite($"orsetsaga-s{siteIdx}-{n:D2}")
                        .ForTree(SetTree).Set(staged)
                        .ForTree(LwwTree).Set(lwwKey, Bytes($"v-{siteIdx}-{n}"))
                        .CommitAsync();

                    CyclePartitions(siteIdx, n, crdtPump, lwwPump);
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await crdtPump.HealAllAndDrainAsync(DrainTimeout);
        await lwwPump.HealAllAndDrainAsync(DrainTimeout);

        // Membership union convergence: every site observes every element
        // added by any site - if a staged add were dropped on the prepared
        // path, that site's member would be lost.
        for (var receiver = 0; receiver < SiteCount; receiver++)
        {
            var observed = await fixture.ClientOf(receiver)
                .GetGrain<ILattice>(SetTree)
                .OrSet(SetKey)
                .GetAsync();
            for (var author = 0; author < SiteCount; author++)
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    var element = Bytes($"s{author}-e{n:D2}");
                    Assert.That(observed.Contains(element), Is.True,
                        $"Site {receiver} OR-Set '{SetKey}' is missing element authored by "
                        + $"site {author} saga {n} after a full drain; the staged add must "
                        + $"survive the prepared-path union.");
                }
            }
        }

        await AssertLwwSiblingFullyVisibleAsync(fixture);
    }

    /// <summary>
    /// Drives an independent partition / heal cycle on both trees' pumps from
    /// site 0's workload loop, staggering the two trees so a receiver
    /// routinely observes one tree's terminal before the sibling's.
    /// </summary>
    private static void CyclePartitions(int siteIdx, int n, ChaosDeliveryPump crdtPump, ChaosDeliveryPump lwwPump)
    {
        if (siteIdx != 0)
        {
            return;
        }

        if (n == SagasPerSite / 3)
        {
            crdtPump.IsolateSite(1);
        }
        if (n == SagasPerSite / 2)
        {
            lwwPump.IsolateSite(0);
        }
        if (n == (2 * SagasPerSite) / 3)
        {
            crdtPump.HealSite(1);
            lwwPump.HealSite(0);
        }
    }

    private static async Task AssertLwwSiblingFullyVisibleAsync(MultiSiteClusterFixture fixture)
    {
        for (var receiver = 0; receiver < SiteCount; receiver++)
        {
            var lww = fixture.ClientOf(receiver).GetGrain<ILattice>(LwwTree);
            for (var author = 0; author < SiteCount; author++)
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    var lwwKey = $"s{author}-lww{n:D2}";
                    Assert.That(await lww.GetAsync(lwwKey), Is.Not.Null,
                        $"Site {receiver} is missing sibling LWW key '{lwwKey}' authored by "
                        + $"site {author} after a full drain; the cross-tree atomic write must "
                        + $"commit the CRDT contribution and its LWW sibling together.");
                }
            }
        }
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private sealed class TestRunner(string crdtTree, LatticeMergeMode crdtMode) : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(
            LatticeMergeMode.LwwRegister,
            SiteCount,
            configureClient: o => o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                [crdtTree] = crdtMode,
                [LwwTree] = LatticeMergeMode.LwwRegister,
            },
            treeModes: new Dictionary<string, LatticeMergeMode>
            {
                [crdtTree] = crdtMode,
                [LwwTree] = LatticeMergeMode.LwwRegister,
            });

        public ChaosDeliveryPump CrdtPump { get; private set; } = null!;
        public ChaosDeliveryPump LwwPump { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            CrdtPump = new ChaosDeliveryPump(Fixture, crdtTree);
            LwwPump = new ChaosDeliveryPump(Fixture, LwwTree);
            CrdtPump.Start();
            LwwPump.Start();
        }

        public async ValueTask DisposeAsync()
        {
            if (CrdtPump is not null)
            {
                await CrdtPump.DisposeAsync();
            }
            if (LwwPump is not null)
            {
                await LwwPump.DisposeAsync();
            }
            await Fixture.DisposeAsync();
        }
    }
}
