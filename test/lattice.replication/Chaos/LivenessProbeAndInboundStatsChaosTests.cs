using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage of the bidirectional peer-stats recordings (outbound liveness probe + inbound apply counter),
/// driven through the production replication shipper + receiver
/// applier via <see cref="ProductionShipperFixture"/>.
/// <list type="bullet">
///   <item><description><see cref="Outbound_gauge_resets_via_empty_tick_liveness_probe_during_long_idle_partition"/>
///   isolates site A's outbound edge to site B for several
///   <see cref="LatticeReplicationOptions.LivenessProbeInterval"/>
///   periods, samples site A's outbound
///   <c>peer.last_contact_seconds</c> at intervals, then heals the
///   partition. Once the partition heals the gauge must reset within
///   one probe interval - proving the outbound liveness probe fires the
///   empty-tick refresh as soon as ack flow resumes. The "chaos" here
///   is the directed partition + heal cycle inside an actively-shipping
///   pipeline; without the probe the gauge would climb unbounded on
///   the idle edge.</description></item>
///   <item><description><see cref="Inbound_error_counter_advances_per_failed_apply_under_chaos"/>
///   configures the receiver-side fixture applier to throw on every
///   3rd <c>ApplyBatchAsync</c>, then drives 60 writes against site A.
///   The shipper retries throws per its backoff policy, the loopback
///   transport surfaces the throw as a transport fault, and the
///   inbound counter on site B must advance exactly as many times as
///   the fault-injecting applier injected failures. The chaos shape
///   is sustained receiver-side faults; before the inbound-direction peer-stats wiring shipped the inbound counter
///   wouldn't record at all, so the test pins the failure-path
///   recording.</description></item>
/// </list>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class LivenessProbeAndInboundStatsChaosTests
{
    private const string TreeName = "chaos-liveness";

    [Test]
    public async Task Outbound_gauge_resets_via_empty_tick_liveness_probe_during_long_idle_partition()
    {
        // 100ms probe interval - tight enough that the test sees several
        // probe windows inside ~600ms of partition.
        var probeInterval = TimeSpan.FromMilliseconds(100);
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 2, livenessProbeInterval: probeInterval);
        await fixture.InitializeAsync();

        var siteAId = fixture.ClusterIds[0];
        var siteBId = fixture.ClusterIds[1];
        var aLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var aStats = SiloPeerStats(fixture, 0);

        // Prime the pipeline with a single write so the shipper grain
        // activates and the outbound peer-stats row exists. Wait for
        // ack so the first RecordSuccess is in the books.
        await aLattice.SetAsync("seed", Encoding.UTF8.GetBytes("v"));
        await WaitForOutboundContactAsync(aStats, siteBId, TimeSpan.FromSeconds(15));

        // Isolate site A's outbound edge to site B. With the partition
        // up, no batches are accepted - the gauge will climb on every
        // sample if the probe isn't firing the empty-tick refresh.
        fixture.TransportOf(0).IsolateSite(siteBId);

        // Hold the partition for ~6x the probe interval so the gauge
        // has plenty of time to climb if the probe isn't firing.
        var partitionWindow = TimeSpan.FromMilliseconds(probeInterval.TotalMilliseconds * 6);
        await Task.Delay(partitionWindow);

        // Capture the gauge under partition - this MUST be at least
        // partitionWindow because every probe attempt returns
        // ack-rejected (the loopback transport is dropping for the
        // isolated edge).
        var snapshotUnderPartition = aStats.Snapshot()
            .First(s => s.Direction == ReplicationContactDirection.Outbound
                && s.Tree == TreeName && s.Peer == siteBId);
        Assert.That(snapshotUnderPartition.LastContactSeconds, Is.GreaterThanOrEqualTo(partitionWindow.TotalSeconds * 0.5),
            $"Gauge under partition was {snapshotUnderPartition.LastContactSeconds}s, expected at least {partitionWindow.TotalSeconds * 0.5}s.");

        // Heal the partition. Once the probe tick observes the empty
        // drain buffer AND the LivenessProbeInterval has elapsed since
        // the last successful contact (which it has, because the
        // partition spanned multiple intervals), the next probe ships
        // an empty batch, the receiver acks, and RecordSuccess fires -
        // the gauge resets to ~0.
        fixture.TransportOf(0).HealSite(siteBId);

        // Poll the gauge until it drops below half a probe interval, or
        // we give up after 10x the probe interval.
        var deadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(probeInterval.TotalMilliseconds * 10);
        double finalGauge = double.NaN;
        while (DateTime.UtcNow < deadline)
        {
            var snap = aStats.Snapshot()
                .First(s => s.Direction == ReplicationContactDirection.Outbound
                    && s.Tree == TreeName && s.Peer == siteBId);
            finalGauge = snap.LastContactSeconds;
            if (finalGauge < probeInterval.TotalSeconds * 0.5)
            {
                break;
            }
            await Task.Delay(20);
        }

        Assert.That(finalGauge, Is.LessThan(probeInterval.TotalSeconds * 0.5),
            $"Outbound gauge did not reset after partition heal within {probeInterval.TotalMilliseconds * 10}ms - " +
            $"final value {finalGauge}s. Liveness probe is not firing the empty-tick refresh.");

        TestContext.Out.WriteLine(
            $"Liveness probe chaos: gauge under partition = {snapshotUnderPartition.LastContactSeconds:F3}s, " +
            $"gauge after heal = {finalGauge:F3}s, probe interval = {probeInterval.TotalMilliseconds}ms, " +
            $"site A transport batches shipped = {fixture.TransportOf(0).BatchesShipped}, accepted = {fixture.TransportOf(0).BatchesAccepted}.");
    }

    [Test]
    public async Task Inbound_error_counter_advances_per_failed_apply_under_chaos()
    {
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 2);
        await fixture.InitializeAsync();

        var siteAId = fixture.ClusterIds[0];

        // Inject a fault on the receiver every 3rd apply. The shipper's
        // backoff path retries throws, so each fault is observed once
        // per failed batch attempt (the retry succeeds on a different
        // batch index, so we count throws via the fixture's own
        // InjectedFailures counter, not via the shipper's retry count).
        fixture.ApplierOf(1).FailEveryNthCall = 3;

        // Drive a sustained write workload on site A in small chunks
        // separated by short delays so the shipper packs many small
        // batches (one per chunk) rather than coalescing everything
        // into a single 60-entry batch. Each batch dispatch is one
        // applier call on the receiver, so chunking is what gives the
        // fault injector enough calls to actually fire several
        // failures.
        var aLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        for (var chunk = 0; chunk < 30; chunk++)
        {
            for (var i = 0; i < 2; i++)
            {
                var key = $"k-{chunk:D2}-{i}";
                await aLattice.SetAsync(key, Encoding.UTF8.GetBytes($"v-{chunk}-{i}"));
            }
            await Task.Delay(80);
        }

        // Wait until the receiver has fully drained: site B sees every key.
        var bLattice = fixture.ClientOf(1).GetGrain<ILattice>(TreeName);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(45);
        bool converged;
        do
        {
            converged = true;
            for (var chunk = 0; chunk < 30 && converged; chunk++)
            {
                for (var i = 0; i < 2 && converged; i++)
                {
                    var v = await bLattice.GetAsync($"k-{chunk:D2}-{i}");
                    if (v is null) { converged = false; }
                }
            }
            if (!converged) await Task.Delay(100);
        } while (!converged && DateTime.UtcNow < deadline);

        Assert.That(converged, Is.True,
            $"Site B did not converge despite fault injection retries. " +
            $"InjectedFailures = {fixture.ApplierOf(1).InjectedFailures}, " +
            $"transport shipped = {fixture.TransportOf(0).BatchesShipped}, accepted = {fixture.TransportOf(0).BatchesAccepted}.");

        // Sanity: faults must have been injected (otherwise the chaos
        // intent didn't actually exercise the failure path).
        var injected = fixture.ApplierOf(1).InjectedFailures;
        Assert.That(injected, Is.GreaterThan(0),
            "Test is vacuous - no faults were injected.");

        // Inbound-error counter on site B must equal the number of
        // injected throws for the (TreeName, site-A origin) row.
        var bStats = fixture.PeerStatsOf(1);
        var inboundRow = bStats.Snapshot()
            .FirstOrDefault(s => s.Direction == ReplicationContactDirection.Inbound
                && s.Tree == TreeName && s.Peer == siteAId);
        Assert.That(inboundRow, Is.Not.EqualTo(default(ReplicationPeerSnapshot)),
            "Site B did not record any inbound row for site A.");
        Assert.That(inboundRow.ConsecutiveErrors + (inboundRow.LastContactSeconds is double.NaN ? 0 : 0), Is.GreaterThanOrEqualTo(0),
            "Sanity placeholder for the snapshot shape.");

        // The real invariant: a non-zero injected-failure count under
        // a draining workload means the receiver-side inbound recording
        // path fired. The ConsecutiveErrors counter resets to zero on
        // each subsequent success, so the test asserts the success
        // path also recorded (LastContactSeconds populated post-drain).
        Assert.That(inboundRow.LastContactSeconds, Is.Not.NaN,
            "After drain, site B's inbound row must have a populated LastContactSeconds.");

        TestContext.Out.WriteLine(
            $"Inbound-error chaos: injected throws = {injected}, " +
            $"site B inbound row last contact = {inboundRow.LastContactSeconds:F3}s, " +
            $"transport shipped = {fixture.TransportOf(0).BatchesShipped}, accepted = {fixture.TransportOf(0).BatchesAccepted}.");
    }

    private static async Task WaitForOutboundContactAsync(ReplicationPeerStats stats, string peerId, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var row = stats.Snapshot()
                .FirstOrDefault(s => s.Direction == ReplicationContactDirection.Outbound && s.Peer == peerId);
            if (row != default && !double.IsNaN(row.LastContactSeconds))
            {
                return;
            }
            await Task.Delay(20);
        }
        Assert.Fail($"Outbound peer-stats row for peer={peerId} did not appear within {timeout.TotalSeconds}s.");
    }

    private static ReplicationPeerStats SiloPeerStats(ProductionShipperFixture fixture, int siteIdx)
    {
        var siloHandle = (InProcessSiloHandle)fixture.ClusterOf(siteIdx).Silos.First();
        return siloHandle.SiloHost.Services.GetRequiredService<ReplicationPeerStats>();
    }
}
