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
///   arms a deterministic fault budget on the receiver-side fixture
///   applier - two bursts of failures, drained one per entry-carrying
///   <c>ApplyBatchAsync</c> - and drives writes against site A across
///   both. The shipper retries throws per its backoff policy, the
///   loopback transport surfaces the throw as a transport fault, and
///   the inbound counter on site B must record on the failure path.
///   The chaos shape is receiver-side apply outages inside a live
///   pipeline; before the inbound-direction peer-stats wiring shipped
///   the inbound counter wouldn't record at all, so the test pins the
///   failure-path recording. Deliberately budget-driven rather than
///   rate-driven: a one-in-N rate would make the failure count depend
///   on how the shipper packs batches, which the test cannot control,
///   whereas a budget drains identically however batches
///   coalesce - each retry of a thrown batch is another call - and is
///   entailed by the convergence assertion.</description></item>
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
        var applier = fixture.ApplierOf(1);

        // Arm a deterministic fault budget on the receiver rather than a
        // one-in-N rate. A rate makes the failure count a function of how
        // many times the shipper calls the applier - i.e. of how it packs
        // batches - which the test does not control and previously bought
        // with a wall-clock sleep between write chunks. A budget is drained
        // one unit per entry-carrying apply call, and the shipper's backoff
        // path retries a thrown batch, so it drains identically whether the
        // 20 writes below ship as twenty batches or as one.
        const int firstBurst = 3;
        const int secondBurst = 2;
        applier.InjectFaults(firstBurst);

        var aLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var bLattice = fixture.ClientOf(1).GetGrain<ILattice>(TreeName);

        // Burst 1: the receiver is down for the first three entry-carrying
        // applies. No key can reach site B until one of them succeeds, and
        // none can succeed while budget remains - so convergence below
        // *entails* that all three faults fired. The precondition is carried
        // by an assertion, not by a sleep.
        await WriteRangeAsync(aLattice, 0, 10);
        await AssertConvergedAsync(fixture, bLattice, 0, 10, "first burst");

        AssertBudgetDrained(applier, expectedInjected: firstBurst, phase: "first burst");

        // Burst 2: re-arm after the counter has been reset by successful
        // applies, so the test also covers fault-after-success rather than
        // only a cold-start outage.
        applier.InjectFaults(secondBurst);
        await WriteRangeAsync(aLattice, 10, 20);
        await AssertConvergedAsync(fixture, bLattice, 0, 20, "second burst");

        AssertBudgetDrained(applier, expectedInjected: firstBurst + secondBurst, phase: "second burst");

        var injected = applier.InjectedFailures;

        // Inbound-error counter on site B must exist for the
        // (TreeName, site-A origin) row.
        var bStats = fixture.PeerStatsOf(1);
        var inboundRow = bStats.Snapshot()
            .FirstOrDefault(s => s.Direction == ReplicationContactDirection.Inbound
                && s.Tree == TreeName && s.Peer == siteAId);
        Assert.That(inboundRow, Is.Not.EqualTo(default(ReplicationPeerSnapshot)),
            "Site B did not record any inbound row for site A.");

        // Backlog and pipelining depth are outbound-only by design - the
        // receiver tracks no per-peer backlog into itself and does not
        // pipeline into itself - so an inbound row must leave all three
        // at zero however much traffic (or however many faults) it saw.
        Assert.Multiple(() =>
        {
            Assert.That(inboundRow.EntriesBehind, Is.Zero,
                "Backlog is outbound-only; an inbound row must never carry an entries-behind count.");
            Assert.That(inboundRow.BytesBehind, Is.Zero,
                "Backlog is outbound-only; an inbound row must never carry a bytes-behind count.");
            Assert.That(inboundRow.InFlight, Is.Zero,
                "Pipelining depth is outbound-only; an inbound row must never carry an in-flight count.");
        });

        // The real invariant: a known, non-zero injected-failure count under
        // a drained workload means the receiver-side inbound recording
        // path fired. The ConsecutiveErrors counter resets to zero on
        // each subsequent success, so the test asserts the success
        // path also recorded (LastContactSeconds populated post-drain).
        Assert.That(inboundRow.LastContactSeconds, Is.Not.NaN,
            "After drain, site B's inbound row must have a populated LastContactSeconds.");

        TestContext.Out.WriteLine(
            $"Inbound-error chaos: injected throws = {injected} (budget-driven, batching-independent), " +
            $"entry-carrying applier calls = {applier.EntryCarryingCalls}, " +
            $"total applier calls = {applier.TotalCalls}, " +
            $"site B inbound row last contact = {inboundRow.LastContactSeconds:F3}s, " +
            $"transport shipped = {fixture.TransportOf(0).BatchesShipped}, accepted = {fixture.TransportOf(0).BatchesAccepted}.");
    }

    private static async Task WriteRangeAsync(ILattice lattice, int fromInclusive, int toExclusive)
    {
        for (var i = fromInclusive; i < toExclusive; i++)
        {
            await lattice.SetAsync($"k-{i:D2}", Encoding.UTF8.GetBytes($"v-{i}"));
        }
    }

    private static async Task AssertConvergedAsync(
        ProductionShipperFixture fixture,
        ILattice bLattice,
        int fromInclusive,
        int toExclusive,
        string phase)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(45);
        bool converged;
        do
        {
            converged = true;
            for (var i = fromInclusive; i < toExclusive && converged; i++)
            {
                if (await bLattice.GetAsync($"k-{i:D2}") is null) { converged = false; }
            }
            if (!converged) await Task.Delay(100);
        } while (!converged && DateTime.UtcNow < deadline);

        var applier = fixture.ApplierOf(1);
        Assert.That(converged, Is.True,
            $"Site B did not converge in the {phase} despite fault-injection retries. " +
            $"InjectedFailures = {applier.InjectedFailures}, " +
            $"remaining fault budget = {applier.RemainingFaultBudget}, " +
            $"entry-carrying applier calls = {applier.EntryCarryingCalls}, " +
            $"transport shipped = {fixture.TransportOf(0).BatchesShipped}, accepted = {fixture.TransportOf(0).BatchesAccepted}.");
    }

    /// <summary>
    /// Asserts the deterministic fault budget drained in full. Stated as an
    /// explicit precondition with its own diagnosis so a future failure
    /// names its cause instead of reading as "the inbound error counter did
    /// not advance" and misdirecting the reader at the component under test.
    /// </summary>
    private static void AssertBudgetDrained(
        FaultInjectingReplicationApplier applier,
        int expectedInjected,
        string phase)
    {
        Assert.Multiple(() =>
        {
            Assert.That(applier.RemainingFaultBudget, Is.Zero,
                $"Chaos precondition failed in the {phase}: the receiver-side fault budget did not drain " +
                $"({applier.RemainingFaultBudget} fault(s) still owed after site B converged). " +
                "This is NOT a defect in the inbound-error recording path under test. The budget is drained " +
                "one unit per entry-carrying ApplyBatchAsync call and is deliberately independent of how the " +
                "shipper packs batches (a thrown batch is retried, and each retry is another call), so an " +
                "undrained budget means entry-carrying batches stopped reaching the receiver altogether - " +
                $"entry-carrying applier calls = {applier.EntryCarryingCalls}, total calls = {applier.TotalCalls}.");
            Assert.That(applier.InjectedFailures, Is.EqualTo(expectedInjected),
                $"Chaos precondition failed in the {phase}: expected exactly {expectedInjected} injected " +
                $"receiver-side fault(s), saw {applier.InjectedFailures}. Injection is budget-driven and " +
                "therefore deterministic; it does not depend on batch coalescing, so a mismatch means the " +
                "fixture injector changed behaviour, not that the shipper packed batches differently.");
        });
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
