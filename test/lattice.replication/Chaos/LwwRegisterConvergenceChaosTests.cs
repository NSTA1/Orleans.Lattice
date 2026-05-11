using Orleans.Lattice.BPlusTree.Grains;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Convergence chaos test for the <see cref="LatticeMergeMode.LwwRegister"/>
/// dispatch path. Three sites issue concurrent point writes against a
/// single key while a partition isolates one site mid-workload; after
/// the partition heals and the delivery pump drains, every site must
/// observe identical <see cref="VersionedValue.Value"/> and
/// <see cref="VersionedValue.Version"/> — the lexicographic
/// <c>(HLC, originClusterId)</c> winner. LWW under concurrent writes
/// is deterministic by design: the highest <see cref="HybridLogicalClock"/>
/// wins, with origin id breaking ties, so every site that has seen the
/// same set of authored writes converges to the same final state.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class LwwRegisterConvergenceChaosTests
{
    private const string TreeName = "chaos-lww";
    private const string Key = "k";
    private const int SiteCount = 3;
    private const int WritesPerSite = 40;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

    [Test]
    public async Task Concurrent_writes_across_three_sites_under_partition_pick_lexicographic_max()
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
                for (var n = 0; n < WritesPerSite; n++)
                {
                    var payload = Encoding.UTF8.GetBytes($"site-{siteIdx}-write-{n:D3}");
                    await lattice.SetAsync(Key, payload);

                    if (siteIdx == 2 && n == WritesPerSite / 4)
                    {
                        pump.IsolateSite(1);
                    }

                    if (siteIdx == 2 && n == (3 * WritesPerSite) / 4)
                    {
                        pump.HealSite(1);
                    }
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Pull every site's final state and assert pointwise equality.
        // LWW convergence is deterministic on (HLC, origin), so all sites
        // — having seen the same union of authored writes — must agree.
        var states = new VersionedValue[SiteCount];
        for (var i = 0; i < SiteCount; i++)
        {
            states[i] = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).GetWithVersionAsync(Key);
        }

        for (var i = 1; i < SiteCount; i++)
        {
            Assert.Multiple(() =>
            {
                Assert.That(states[i].Value, Is.EqualTo(states[0].Value),
                    $"Site {i} value diverges from site 0.");
                Assert.That(states[i].Version, Is.EqualTo(states[0].Version),
                    $"Site {i} HLC diverges from site 0.");
            });
        }
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(LatticeMergeMode.LwwRegister, SiteCount);
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
