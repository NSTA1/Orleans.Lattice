using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos test for cross-cluster saga atomic visibility. Three sites
/// concurrently author local <see cref="ILattice.SetManyAtomicAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}}, System.Threading.CancellationToken)"/>
/// sagas while the inter-site delivery topology is partitioned and
/// healed mid-workload. Each saga writes a fixed number of keys under
/// a deterministic per-saga prefix, so the post-drain assertion has a
/// closed-form predicate: on every site, every saga's keys must be
/// either <em>all</em> visible (commit-visible on this site) or
/// <em>all</em> absent (saga has not yet shipped or aborted) - never
/// a partial subset. This is the receiver-side atomic-visibility
/// invariant under the chaos pump's randomised partition cycling.
/// <para>
/// The chaos pump ships every WAL entry from the sender's change feed
/// to each peer's <see cref="ReplicationApplier"/>, including prepare
/// entries (Set/Delete with <c>IsPrepared=true</c>) and terminal
/// entries (<c>TxCommit</c>/<c>TxAbort</c> ops). The applier's
/// receiver-side seam routes prepares through
/// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/> and
/// terminals through <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/>,
/// so the full source-to-receiver atomic-visibility pipeline is under
/// test here without bypassing any production code path.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CrossClusterAtomicVisibilityChaosTests
{
    private const string TreeName = "chaos-atomic-visibility";
    private const int SiteCount = 3;
    private const int SagasPerSite = 6;
    private const int KeysPerSaga = 4;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Concurrent_cross_cluster_sagas_under_partition_remain_atomically_visible_on_every_site()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pump = runner.Pump;

        // Plan every site's saga authors. Each saga writes KeysPerSaga
        // keys under a unique prefix so the post-drain check can
        // recover saga membership from the key namespace.
        var sagaKeys = new string[SiteCount, SagasPerSite][];
        for (var site = 0; site < SiteCount; site++)
        {
            for (var sagaIdx = 0; sagaIdx < SagasPerSite; sagaIdx++)
            {
                var keys = new string[KeysPerSaga];
                for (var k = 0; k < KeysPerSaga; k++)
                {
                    keys[k] = $"s{site}-saga{sagaIdx:D2}-k{k}";
                }
                sagaKeys[site, sagaIdx] = keys;
            }
        }

        // Author SagasPerSite sagas concurrently on each site. The
        // partition cycling is driven from site 0's loop so it runs
        // exactly once per fixture and is deterministic relative to
        // the workload's progress (rather than wall-clock).
        var workloadTasks = new Task[SiteCount];
        for (var site = 0; site < SiteCount; site++)
        {
            var siteIdx = site;
            var lattice = fixture.ClientOf(siteIdx).GetGrain<ILattice>(TreeName);
            workloadTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    var entries = new List<KeyValuePair<string, byte[]>>(KeysPerSaga);
                    foreach (var key in sagaKeys[siteIdx, n])
                    {
                        var payload = Encoding.UTF8.GetBytes($"v-{siteIdx}-{n}-{key}");
                        entries.Add(new KeyValuePair<string, byte[]>(key, payload));
                    }

                    await lattice.SetManyAtomicAsync(entries);

                    // Mid-workload partition cycle: site 0 isolates site 2
                    // for the middle third of its workload, simulating a
                    // network outage spanning multiple in-flight sagas.
                    if (siteIdx == 0 && n == SagasPerSite / 3)
                    {
                        pump.IsolateSite(2);
                    }

                    if (siteIdx == 0 && n == (2 * SagasPerSite) / 3)
                    {
                        pump.HealSite(2);
                    }
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await pump.HealAllAndDrainAsync(DrainTimeout);

        // Atomic-visibility invariant: on every receiver site, every
        // authored saga's KeysPerSaga keys must be either ALL visible
        // OR ALL absent. A partial-subset visibility on any site is a
        // saga-atomicity violation.
        for (var receiverIdx = 0; receiverIdx < SiteCount; receiverIdx++)
        {
            var lattice = fixture.ClientOf(receiverIdx).GetGrain<ILattice>(TreeName);
            for (var authorSite = 0; authorSite < SiteCount; authorSite++)
            {
                for (var sagaIdx = 0; sagaIdx < SagasPerSite; sagaIdx++)
                {
                    var keys = sagaKeys[authorSite, sagaIdx];
                    var presentCount = 0;
                    foreach (var key in keys)
                    {
                        var value = await lattice.GetAsync(key);
                        if (value is not null)
                        {
                            presentCount++;
                        }
                    }

                    Assert.That(
                        presentCount == 0 || presentCount == KeysPerSaga,
                        Is.True,
                        $"Site {receiverIdx} observed PARTIAL saga visibility for author={authorSite} saga={sagaIdx}: {presentCount}/{KeysPerSaga} keys visible. Atomic visibility was violated.");
                }
            }
        }

        // Strong post-drain assertion: every authored saga is a local
        // commit (no aborts in this workload), so after a full drain
        // every site must observe every saga's keys present. This
        // pins the universal-reader-isolation property: the receiver
        // sees the same all-or-nothing as the source.
        for (var receiverIdx = 0; receiverIdx < SiteCount; receiverIdx++)
        {
            var lattice = fixture.ClientOf(receiverIdx).GetGrain<ILattice>(TreeName);
            for (var authorSite = 0; authorSite < SiteCount; authorSite++)
            {
                for (var sagaIdx = 0; sagaIdx < SagasPerSite; sagaIdx++)
                {
                    foreach (var key in sagaKeys[authorSite, sagaIdx])
                    {
                        var value = await lattice.GetAsync(key);
                        Assert.That(
                            value,
                            Is.Not.Null,
                            $"Site {receiverIdx} did not observe key {key} from author={authorSite} saga={sagaIdx} after drain. Cross-cluster saga commit failed to propagate.");
                    }
                }
            }
        }

        Assert.That(
            pump.PumpErrors,
            Is.Empty,
            $"Chaos delivery pump surfaced {pump.PumpErrors.Count} errors during the run. First: {(pump.PumpErrors.TryPeek(out var first) ? first.Message : "<none>")}");
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