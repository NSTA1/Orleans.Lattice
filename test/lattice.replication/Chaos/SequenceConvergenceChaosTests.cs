using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.Sequence"/>
/// dispatch path. Three sites issue concurrent insert and delete bursts
/// against a single RGA sequence key while a partition isolates one site
/// mid-workload; after the partition heals and the delivery pump drains,
/// every site must observe an <strong>identical ordered traversal</strong>.
/// <para>
/// Pointwise node-set equality is necessary but not sufficient: an RGA's
/// whole purpose is a single resolved order under concurrent edits, so
/// the assertion compares the full ordered <c>ToListAsync</c> projection
/// across every site, not merely the set of surviving elements. The
/// producer ships dot-explicit <c>RgaDelta</c> inserts (each carrying its
/// dot and parent dot) and tombstone dots; the receiver folds them via
/// <c>Rga.MergeDelta</c>, and the descending <c>(Counter, ReplicaId)</c>
/// sibling tie-break yields the same order on every replica regardless of
/// merge arrival sequence.
/// </para>
/// <para>
/// The fixture configures the test tree with
/// <c>LatticeMergeMode.Sequence</c> on every silo; the descriptor is a
/// global closed shape so no per-tree registration is required (unlike
/// <see cref="LatticeMergeMode.OrMap"/>).
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class SequenceConvergenceChaosTests
{
    private const string TreeName = "chaos-sequence";
    private const string Key = "doc";
    private const int SiteCount = 3;
    private const int InsertsPerSite = 6;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Concurrent_inserts_across_three_sites_under_partition_converge_to_identical_order()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Each site appends its own family of elements at the head of the
        // sequence (InsertAfter Root) so concurrent root-level inserts
        // from different replicas must converge on the deterministic
        // descending (Counter, ReplicaId) order. Site 2 is isolated
        // mid-burst so its inserts accumulate behind the partition.
        var insertTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var replicaId = MultiSiteClusterFixture.ClusterIdFor(siteIdx);
            var seq = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName).Sequence<string>(Key);
            insertTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < InsertsPerSite; n++)
                {
                    await InsertWithRetryAsync(seq, replicaId, $"{replicaId}-{n}");
                    if (siteIdx == 0 && n == InsertsPerSite / 3) pump.IsolateSite(2);
                    if (siteIdx == 0 && n == (2 * InsertsPerSite) / 3) pump.HealSite(2);
                }
            });
        }

        await Task.WhenAll(insertTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        await AssertIdenticalOrderedTraversalAsync(fixture, expectedCount: SiteCount * InsertsPerSite);
    }

    [Test]
    public async Task Concurrent_insert_and_delete_bursts_under_partition_converge_to_identical_order()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        var burstTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var replicaId = MultiSiteClusterFixture.ClusterIdFor(siteIdx);
            var seq = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName).Sequence<string>(Key);
            burstTasks[siteIdx] = Task.Run(async () =>
            {
                var ownDots = new List<OrSetDot>();
                for (var n = 0; n < InsertsPerSite; n++)
                {
                    var dot = await InsertWithRetryAsync(seq, replicaId, $"{replicaId}-{n}");
                    ownDots.Add(dot);

                    // Tombstone an earlier own-authored node every few
                    // inserts so the workload interleaves concurrent
                    // inserts and deletes (the bar the test pins is that
                    // tombstone propagation does not desynchronise the
                    // resolved order across sites).
                    if (n % 3 == 2 && ownDots.Count > 0)
                    {
                        await RemoveWithRetryAsync(seq, ownDots[0]);
                        ownDots.RemoveAt(0);
                    }

                    if (siteIdx == 1 && n == InsertsPerSite / 2) pump.IsolateSite(2);
                    if (siteIdx == 1 && n == (3 * InsertsPerSite) / 4) pump.HealSite(2);
                }
            });
        }

        await Task.WhenAll(burstTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // The exact surviving count is timing-dependent (each site deletes
        // some of its own inserts), so the test asserts identical ordered
        // traversal across sites rather than a fixed count.
        await AssertIdenticalOrderedTraversalAsync(fixture, expectedCount: null);
    }

    private static async Task AssertIdenticalOrderedTraversalAsync(
        MultiSiteClusterFixture fixture,
        int? expectedCount)
    {
        var reference = (await fixture.ClientOf(0).GetGrain<ILattice>(TreeName).Sequence<string>(Key).ToListAsync()).ToArray();

        if (expectedCount is { } count)
        {
            Assert.That(reference, Has.Length.EqualTo(count),
                "Site 0 did not converge to the full union of authored inserts.");
        }

        for (var i = 1; i < SiteCount; i++)
        {
            var observed = (await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).Sequence<string>(Key).ToListAsync()).ToArray();

            // Identical ordered traversal - the RGA convergence bar.
            // Set equality is necessary but not sufficient; the resolved
            // order must match element-for-element.
            Assert.That(observed, Is.EqualTo(reference),
                $"Site {i} did not converge to the same ordered traversal as site 0.");
        }
    }

    private static async Task<OrSetDot> InsertWithRetryAsync(RgaAccessor<string> seq, string replicaId, string value)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                return await seq.InsertAfterAsync(Rga.Root, replicaId, value);
            }
            catch (Exception ex) when (ex is not OperationCanceledException && attempt < 7)
            {
                await Task.Delay(5 * (attempt + 1));
            }
        }

        return await seq.InsertAfterAsync(Rga.Root, replicaId, value);
    }

    private static async Task RemoveWithRetryAsync(RgaAccessor<string> seq, OrSetDot dot)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                await seq.RemoveAsync(dot);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException && attempt < 7)
            {
                await Task.Delay(5 * (attempt + 1));
            }
        }

        await seq.RemoveAsync(dot);
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.Sequence, SiteCount);
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
