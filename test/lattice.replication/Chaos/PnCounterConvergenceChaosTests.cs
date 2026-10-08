using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.PnCounter"/>
/// dispatch path. Three sites issue concurrent increments and decrements
/// against a single counter key while a partition isolates one site
/// mid-workload; after the partition heals and the delivery pump drains,
/// every site must read the same total - the algebraic sum across all
/// sites' authored deltas.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class PnCounterConvergenceChaosTests
{
    private const string TreeName = "chaos-pncounter";
    private const string Key = "k";
    private const int SiteCount = 3;
    private const int IncrementsPerSite = 30;
    private const int DecrementsPerSite = 10;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_increments_across_three_sites_under_partition_sum_correctly()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        var workloadTasks = new Task[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            var siteIdx = i;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            workloadTasks[siteIdx] = Task.Run(async () =>
            {
                var replicaId = MultiSiteClusterFixture.ClusterIdFor(siteIdx);
                for (var n = 0; n < IncrementsPerSite; n++)
                {
                    await IncrementWithRetryAsync(lattice, replicaId);

                    if (siteIdx == 1 && n == IncrementsPerSite / 3)
                    {
                        pump.IsolateSite(0);
                    }

                    if (siteIdx == 1 && n == (2 * IncrementsPerSite) / 3)
                    {
                        pump.HealSite(0);
                    }

                    // Micro-yield so the per-edge pumps' foreign-origin writes
                    // interleave with the local increments on the single
                    // contended key. (IncrementAsync reads once and applies one
                    // delta - it has no CAS loop or attempt budget - so this
                    // only paces the interleaving.)
                    await Task.Delay(1);
                }

                for (var n = 0; n < DecrementsPerSite; n++)
                {
                    await DecrementWithRetryAsync(lattice, replicaId);
                    await Task.Delay(1);
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        var expected = SiteCount * (IncrementsPerSite - DecrementsPerSite);
        for (var i = 0; i < SiteCount; i++)
        {
            var value = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).PnCounter(Key).ValueAsync();
            Assert.That(value, Is.EqualTo(expected),
                $"Site {i} did not converge to the expected algebraic sum.");
        }
    }

    /// <summary>
    /// Wraps <see cref="Orleans.Lattice.PnCounterAccessor.IncrementAsync(string, long, System.Threading.CancellationToken, int)"/> in a bounded
    /// retry loop that backs off on an <see cref="InvalidOperationException"/>
    /// reporting an exhausted CAS budget. The accessor no longer has a CAS
    /// loop (it reads once and applies one delta), so on the current path the
    /// filter is not expected to match; the wrapper is kept as a harmless
    /// guard.
    /// </summary>
    private static async Task IncrementWithRetryAsync(ILattice lattice, string replicaId)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                await lattice.PnCounter(Key).IncrementAsync(replicaId, 1);
                return;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CAS budget exhausted", StringComparison.Ordinal))
            {
                await Task.Delay(5 * (attempt + 1));
            }
        }

        // Final attempt - let the exception propagate if it still fails.
        await lattice.PnCounter(Key).IncrementAsync(replicaId, 1);
    }

    private static async Task DecrementWithRetryAsync(ILattice lattice, string replicaId)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            try
            {
                await lattice.PnCounter(Key).DecrementAsync(replicaId, 1);
                return;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("CAS budget exhausted", StringComparison.Ordinal))
            {
                await Task.Delay(5 * (attempt + 1));
            }
        }

        await lattice.PnCounter(Key).DecrementAsync(replicaId, 1);
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.PnCounter, SiteCount);
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
