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
/// <see cref="VersionedValue.Version"/> - the lexicographic
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
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(30);

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

        // The drain reports every edge cursor has reached its sender's WAL
        // tail, but under CI load a just-applied foreign write can still be
        // re-emitting onward: applying a remote write appends a new local WAL
        // entry (carrying the foreign origin) that a sibling edge may not have
        // shipped yet when the drain returns. The background pumps keep
        // delivering after the drain, so poll until every site agrees rather
        // than sampling once. LWW convergence is deterministic on (HLC,
        // origin), so a genuine non-convergence still fails the assertion
        // below once the budget elapses - the poll only removes the race, it
        // does not mask a real divergence.
        var states = await SampleUntilConvergedAsync(fixture, ConvergenceTimeout);

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

    /// <summary>
    /// Polls every site's <see cref="VersionedValue"/> for <see cref="Key"/>
    /// until all sites agree with site 0 (value bytes and
    /// <see cref="HybridLogicalClock"/>) or <paramref name="timeout"/> elapses,
    /// returning the last sampled snapshot either way. The chaos pumps keep
    /// delivering in the background after
    /// <see cref="ChaosDeliveryPump.HealAllAndDrainAsync"/> returns, so this
    /// lets in-flight re-emission settle before the caller asserts. On timeout
    /// it returns the final (still-divergent) snapshot so the caller's
    /// assertion surfaces the pointwise divergence detail.
    /// </summary>
    private static async Task<VersionedValue[]> SampleUntilConvergedAsync(
        MultiSiteClusterFixture fixture, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        VersionedValue[] states;
        do
        {
            states = new VersionedValue[SiteCount];
            for (var i = 0; i < SiteCount; i++)
            {
                states[i] = await fixture.ClientOf(i).GetGrain<ILattice>(TreeName).GetWithVersionAsync(Key);
            }

            var converged = true;
            for (var i = 1; i < SiteCount && converged; i++)
            {
                converged = BytesEqual(states[i].Value, states[0].Value)
                    && states[i].Version.Equals(states[0].Version);
            }

            if (converged)
            {
                return states;
            }

            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        return states;
    }

    private static bool BytesEqual(byte[]? a, byte[]? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null)
        {
            return false;
        }

        return a.AsSpan().SequenceEqual(b);
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
