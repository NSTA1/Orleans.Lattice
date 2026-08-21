using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Integration.Tests;

/// <summary>
/// The durable active-active integration suite: eight high-value scenarios
/// exercising cross-cluster replication against real Azure Table Storage
/// (Azurite) for Orleans grain state, reminders, and the Lattice WAL, using a
/// single shared <see cref="DurableActiveActiveClusterFixture"/> across the
/// whole class. Every scenario writes to its own pre-minted tree id, so no
/// scenario needs to reset any other scenario's data and test ordering never
/// matters.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("AzureStorageEmulator")]
[NonParallelizable]
public sealed class DurableActiveActiveTests
{
    private DurableActiveActiveClusterFixture _fixture = null!;

    /// <summary>Stands up both sites once for the whole class. Self-skips (inconclusive) when Azurite is unreachable.</summary>
    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _fixture = new DurableActiveActiveClusterFixture();
        await _fixture.InitializeAsync();
    }

    /// <summary>Tears down both sites and deletes the run's Azure tables.</summary>
    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    /// <summary>
    /// Normalizes transport fault state between tests - heals every
    /// partition and releases/clears any pending one-shot gate or
    /// reject-after-apply flag - without stopping either site, so leftover
    /// fault injection from one test can never bleed into the next.
    /// </summary>
    [TearDown]
    public Task TearDownAsync() => _fixture.NormalizeAfterScenarioAsync();

    /// <summary>
    /// Scenario 1: sender crash before and after acknowledgement.
    /// <para>
    /// Part A (lost ack after apply): Site A writes a key; the transport is
    /// armed to report the ack as rejected exactly once even though Site B
    /// already applied it, forcing Site A's shipper to retry. Site A is then
    /// cold-restarted before it has recorded the (already-lost) ack. The
    /// retried delivery on restart is a harmless duplicate apply - the
    /// LWW-register merge on Site B is idempotent for the same value - so
    /// Site B's value and Site A's eventual, once-converged read must match.
    /// </para>
    /// <para>
    /// Part B (normal acked write, then sender restart): Site A writes a
    /// second key with no fault injected, waits for it to converge onto Site
    /// B, then cold-restarts Site A. Nothing is lost, and the delivery count
    /// for that key's tree does not grow again after the restart because the
    /// shipper's durable cursor already advanced past it before the restart.
    /// </para>
    /// </summary>
    [Test]
    public async Task Sender_crash_before_and_after_acknowledgement_preserves_writes_and_tolerates_duplicate_apply()
    {
        var treeId = _fixture.SenderCrashTreeId;
        var keyLostAck = "lost-ack-key";
        var valueLostAck = Encode("lost-ack-value");

        FaultInjectingReplicationTransport.ScheduleRejectAndPartitionAfterApplyOnce(treeId);
        await _fixture.TreeOn(Site.A, treeId).SetAsync(keyLostAck, valueLostAck);

        // The apply already landed on Site B despite the rejected ack; Site
        // A's shipper does not yet know that, so it will retry once it comes
        // back up.
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync(keyLostAck);
                return onB is not null && onB.AsSpan().SequenceEqual(valueLostAck);
            },
            "Site B applies the value despite the injected rejected ack");

        await _fixture.ColdRestartSiteAsync(Site.A);
        _fixture.Heal(Site.A, Site.B);

        // The retried delivery (a harmless duplicate apply of the same LWW
        // value) must still leave Site B converged, and Site A - once its
        // shipper catches back up post-restart - must read the same value.
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await _fixture.TreeOn(Site.A, treeId).GetAsync(keyLostAck);
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync(keyLostAck);
                return onA is not null && onA.AsSpan().SequenceEqual(valueLostAck)
                    && onB is not null && onB.AsSpan().SequenceEqual(valueLostAck);
            },
            "Site A and Site B both read the lost-ack value after Site A's cold restart");

        // Part B: a normal acked write, then a sender restart. Nothing is
        // lost and the shipper does not re-deliver an already-cursor-past
        // entry after restart.
        var keyAcked = "acked-key";
        var valueAcked = Encode("acked-value");
        var acceptedBefore = FaultInjectingReplicationTransport.AcceptedAckCount(treeId);
        await _fixture.TreeOn(Site.A, treeId).SetAsync(keyAcked, valueAcked);

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync(keyAcked);
                return onB is not null && onB.AsSpan().SequenceEqual(valueAcked);
            },
            "Site B applies the normally-acked value");

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            () => Task.FromResult(FaultInjectingReplicationTransport.AcceptedAckCount(treeId) > acceptedBefore),
            "the sender receives a positive acknowledgement for the normal write");

        // A later accepted delivery is a sequencing barrier: the shipper
        // cannot send it until it has advanced past the first acknowledged
        // batch.
        await _fixture.TreeOn(Site.A, treeId).SetAsync("ack-barrier", Encode("ack-barrier-value"));
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            () => Task.FromResult(FaultInjectingReplicationTransport.AcceptedAckCount(treeId) > acceptedBefore + 1),
            "the shipper advances through a later acknowledged batch");

        var deliveriesBeforeRestart = FaultInjectingReplicationTransport.DeliveryCount(treeId);

        await _fixture.ColdRestartSiteAsync(Site.A);

        // Give the post-restart driver a full poll cycle to (not) redeliver,
        // then assert both no data loss and no re-delivery of the
        // already-shipped entry.
        await Task.Delay(TimeSpan.FromSeconds(2));

        var onAAfterRestart = await _fixture.TreeOn(Site.A, treeId).GetAsync(keyAcked);
        var onBAfterRestart = await _fixture.TreeOn(Site.B, treeId).GetAsync(keyAcked);
        Assert.That(onAAfterRestart is not null && onAAfterRestart.AsSpan().SequenceEqual(valueAcked), Is.True, "Site A retains the acked write across its own restart");
        Assert.That(onBAfterRestart is not null && onBAfterRestart.AsSpan().SequenceEqual(valueAcked), Is.True, "Site B retains the acked write across Site A's restart");
        Assert.That(
            FaultInjectingReplicationTransport.DeliveryCount(treeId),
            Is.EqualTo(deliveriesBeforeRestart),
            "the already-shipped, cursor-advanced entry must not be re-delivered after the sender's restart");
    }

    /// <summary>
    /// Scenario 2: receiver crash during apply. A one-shot gate is armed for
    /// the tree so the next delivery resolves the receiver-side applier,
    /// signals entry, and blocks at the apply boundary. While the send is
    /// parked at the gate, Site B is cold-restarted; releasing the stale
    /// receiver-bound send forces the sender's production retry path. The
    /// write must still converge with no data loss.
    /// </summary>
    [Test]
    public async Task Receiver_crash_during_apply_retries_and_converges_after_restart()
    {
        var treeId = _fixture.ReceiverCrashTreeId;
        var key = "gated-key";
        var value = Encode("gated-value");

        var gate = FaultInjectingReplicationTransport.ScheduleGateBeforeApplyOnce(treeId);
        var writeTask = _fixture.TreeOn(Site.A, treeId).SetAsync(key, value);

        await gate.Entered.WaitAsync(TimeSpan.FromSeconds(30));

        await _fixture.ColdRestartSiteAsync(Site.B);

        gate.Release();

        await writeTask;

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync(key);
                return onB is not null && onB.AsSpan().SequenceEqual(value);
            },
            "Site B eventually applies the value that was in flight when it was cold-restarted");

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await _fixture.TreeOn(Site.A, treeId).GetAsync(key);
                return onA is not null && onA.AsSpan().SequenceEqual(value);
            },
            "Site A still reads its own written value after the receiver-side gate/restart");
    }

    /// <summary>
    /// Scenario 3: one site is cold-restarted while the peer continues to
    /// write; once the restarted site is back, it catches up on everything
    /// it missed.
    /// </summary>
    [Test]
    public async Task One_site_stopped_while_peer_continues_writing_catches_up_after_cold_start()
    {
        var treeId = _fixture.OneSiteRestartTreeId;

        await _fixture.TreeOn(Site.A, treeId).SetAsync("before-restart", Encode("before"));
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () => (await _fixture.TreeOn(Site.B, treeId).GetAsync("before-restart")) is not null,
            "Site B observes the pre-restart write before Site B goes down");

        await _fixture.StopSiteAsync(Site.B);

        // Site A keeps writing while Site B is down; these writes queue up
        // in Site A's WAL/shipper for Site B to catch up on once it returns.
        await _fixture.TreeOn(Site.A, treeId).SetAsync("during-restart-1", Encode("during-1"));
        await _fixture.TreeOn(Site.A, treeId).SetAsync("during-restart-2", Encode("during-2"));

        await _fixture.StartSiteAsync(Site.B);

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var v1 = await _fixture.TreeOn(Site.B, treeId).GetAsync("during-restart-1");
                var v2 = await _fixture.TreeOn(Site.B, treeId).GetAsync("during-restart-2");
                return v1 is not null && v1.AsSpan().SequenceEqual(Encode("during-1"))
                    && v2 is not null && v2.AsSpan().SequenceEqual(Encode("during-2"));
            },
            "Site B catches up on both writes made while it was cold-restarted");
    }

    /// <summary>
    /// Scenario 4: both sites are cold-restarted. Data written before the
    /// restart survives on both sides, and a fresh bidirectional write pair
    /// afterwards still converges.
    /// </summary>
    [Test]
    public async Task Both_sites_cold_restarted_preserve_old_data_and_converge_new_bidirectional_writes()
    {
        var treeId = _fixture.BothSitesRestartTreeId;

        await _fixture.TreeOn(Site.A, treeId).SetAsync("durable-key", Encode("durable-value"));
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () => (await _fixture.TreeOn(Site.B, treeId).GetAsync("durable-key")) is not null,
            "Site B observes the pre-restart write before both sites go down");

        await _fixture.ColdRestartSiteAsync(Site.A);
        await _fixture.ColdRestartSiteAsync(Site.B);

        var onAAfterRestart = await _fixture.TreeOn(Site.A, treeId).GetAsync("durable-key");
        var onBAfterRestart = await _fixture.TreeOn(Site.B, treeId).GetAsync("durable-key");
        Assert.That(onAAfterRestart?.AsSpan().SequenceEqual(Encode("durable-value")), Is.True, "Site A retains the durable value across its own cold restart");
        Assert.That(onBAfterRestart?.AsSpan().SequenceEqual(Encode("durable-value")), Is.True, "Site B retains the durable value across its own cold restart");

        await _fixture.TreeOn(Site.A, treeId).SetAsync("post-restart-from-a", Encode("from-a"));
        await _fixture.TreeOn(Site.B, treeId).SetAsync("post-restart-from-b", Encode("from-b"));

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync("post-restart-from-a");
                return onB is not null && onB.AsSpan().SequenceEqual(Encode("from-a"));
            },
            "Site B receives Site A's post-restart write");

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await _fixture.TreeOn(Site.A, treeId).GetAsync("post-restart-from-b");
                return onA is not null && onA.AsSpan().SequenceEqual(Encode("from-b"));
            },
            "Site A receives Site B's post-restart write");
    }

    /// <summary>
    /// Scenario 5: bidirectional partition with concurrent distinct-key
    /// writes on both sides while partitioned, a restart of one site while
    /// still partitioned, then heal and converge.
    /// </summary>
    [Test]
    public async Task Bidirectional_partition_with_concurrent_writes_and_restart_heals_and_converges()
    {
        var treeId = _fixture.PartitionRestartTreeId;

        _fixture.Partition(Site.A, Site.B);
        _fixture.Partition(Site.B, Site.A);

        await _fixture.TreeOn(Site.A, treeId).SetAsync("from-a-key", Encode("from-a-value"));
        await _fixture.TreeOn(Site.B, treeId).SetAsync("from-b-key", Encode("from-b-value"));

        // While still partitioned, cold-restart Site A; its unshipped write
        // must survive the restart entirely locally (no replication
        // possible while partitioned).
        await _fixture.ColdRestartSiteAsync(Site.A);

        var onAOwnWriteAfterRestart = await _fixture.TreeOn(Site.A, treeId).GetAsync("from-a-key");
        Assert.That(onAOwnWriteAfterRestart?.AsSpan().SequenceEqual(Encode("from-a-value")), Is.True);

        // Still partitioned: neither side should have observed the other's write.
        var onBWhilePartitioned = await _fixture.TreeOn(Site.B, treeId).GetAsync("from-a-key");
        var onAWhilePartitioned = await _fixture.TreeOn(Site.A, treeId).GetAsync("from-b-key");
        Assert.That(onBWhilePartitioned, Is.Null, "Site B must not see Site A's write while the partition holds");
        Assert.That(onAWhilePartitioned, Is.Null, "Site A must not see Site B's write while the partition holds");

        _fixture.HealAll();

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync("from-a-key");
                return onB is not null && onB.AsSpan().SequenceEqual(Encode("from-a-value"));
            },
            "Site B converges on Site A's write once healed");

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await _fixture.TreeOn(Site.A, treeId).GetAsync("from-b-key");
                return onA is not null && onA.AsSpan().SequenceEqual(Encode("from-b-value"));
            },
            "Site A converges on Site B's write once healed");
    }

    /// <summary>
    /// Scenario 6: WAL GC across a restart cannot trim entries that have not
    /// yet been shipped to the (partitioned) peer. Site A is partitioned from
    /// Site B, a write is made, and <see cref="ILatticeWalGc.RunOnceAsync"/>
    /// is run - once before, once after a cold restart of Site A, still
    /// partitioned both times - asserting zero unsafe trims and an unchanged
    /// oldest-available HLC. Healing then lets the entry ship and converge
    /// normally.
    /// </summary>
    [Test]
    public async Task Wal_gc_across_restart_does_not_trim_unshipped_entries()
    {
        var treeId = _fixture.WalGcTreeId;

        // Establish a positive peer cursor first. Without one, WAL GC is
        // deliberately a no-op and cannot prove that a lagging peer pins the
        // unshipped suffix.
        await _fixture.TreeOn(Site.A, treeId).SetAsync("shipped-key", Encode("shipped-value"));
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () => (await _fixture.TreeOn(Site.B, treeId).GetAsync("shipped-key")) is not null,
            "Site B receives the prefix entry used to establish its cursor");

        HybridLogicalClock? acknowledgedPrefixCursor = null;
        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var registry = _fixture.ServicesFor(Site.A).GetRequiredService<IWalCursorRegistry>();
                acknowledgedPrefixCursor = await registry.GetMinCursorAsync(treeId);
                return acknowledgedPrefixCursor is { } cursor && cursor > HybridLogicalClock.Zero;
            },
            "Site A observes a positive safe trim frontier");

        // Keep that known-safe frontier stable for the pre-restart phase even
        // if the short-lived leaf reporter deactivates. This test consumer is
        // intentionally process-local and disappears on cold restart.
        var siteAServices = _fixture.ServicesFor(Site.A);
        var cursorRegistry = siteAServices.GetRequiredService<IWalCursorRegistry>();
        await cursorRegistry.ReportCursorAsync(
            treeId,
            "integration-lagging-peer",
            acknowledgedPrefixCursor!.Value);

        var gcServicesBefore = new ServiceCollection()
            .AddSingleton(siteAServices.GetRequiredService<IWalStorageProvider>())
            .BuildServiceProvider();
        var gcBefore = new LatticeWalGc(
            gcServicesBefore,
            cursorRegistry,
            siteAServices.GetRequiredService<IOptionsMonitor<LatticeOptions>>());
        var reportForShippedPrefix = await gcBefore.RunOnceAsync(treeId);
        Assert.That(reportForShippedPrefix.MinCursor, Is.Not.Null.And.GreaterThan(HybridLogicalClock.Zero));
        Assert.That(reportForShippedPrefix.EntriesTrimmed, Is.GreaterThan(0), "the acknowledged prefix should be trim-eligible under the positive cursor");

        _fixture.Partition(Site.A, Site.B);

        await _fixture.TreeOn(Site.A, treeId).SetAsync("unshipped-key", Encode("unshipped-value"));

        var introspectionBefore = _fixture.ServicesFor(Site.A).GetRequiredService<ILatticeWalIntrospection>();

        var oldestUnshipped = await introspectionBefore.GetOldestAvailableHlcAsync(treeId);
        Assert.That(oldestUnshipped, Is.Not.Null);

        var reportBefore = await gcBefore.RunOnceAsync(treeId);
        Assert.That(reportBefore.EntriesTrimmed, Is.Zero, "GC must not trim the unshipped suffix while the peer is partitioned");
        Assert.That(await introspectionBefore.GetOldestAvailableHlcAsync(treeId), Is.EqualTo(oldestUnshipped));

        await _fixture.ColdRestartSiteAsync(Site.A);

        var restartedSiteAServices = _fixture.ServicesFor(Site.A);
        var restartedCursorRegistry = restartedSiteAServices.GetRequiredService<IWalCursorRegistry>();
        await restartedCursorRegistry.ReportCursorAsync(
            treeId,
            "integration-lagging-peer",
            acknowledgedPrefixCursor.Value);
        var gcServicesAfter = new ServiceCollection()
            .AddSingleton(restartedSiteAServices.GetRequiredService<IWalStorageProvider>())
            .BuildServiceProvider();
        var gcAfter = new LatticeWalGc(
            gcServicesAfter,
            restartedCursorRegistry,
            restartedSiteAServices.GetRequiredService<IOptionsMonitor<LatticeOptions>>());
        var introspectionAfter = restartedSiteAServices.GetRequiredService<ILatticeWalIntrospection>();

        var reportAfter = await gcAfter.RunOnceAsync(treeId);
        Assert.That(reportAfter.MinCursor, Is.Not.Null.And.GreaterThan(HybridLogicalClock.Zero));
        Assert.That(reportAfter.EntriesTrimmed, Is.Zero, "the still-unshipped entry must remain untrimmed after the sender's own cold restart");

        var oldestAfter = await introspectionAfter.GetOldestAvailableHlcAsync(treeId);
        Assert.That(oldestAfter, Is.EqualTo(oldestUnshipped), "the oldest available WAL entry must not move past the peer's unshipped suffix");

        _fixture.Heal(Site.A, Site.B);

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync("unshipped-key");
                return onB is not null && onB.AsSpan().SequenceEqual(Encode("unshipped-value"));
            },
            "the previously unshippable entry ships and converges once healed");
    }

    /// <summary>
    /// Scenario 7: the shipper cursor and the receiver high-water mark both
    /// recover correctly across a restart of both sites. A lost ack creates
    /// a duplicate-delivery opportunity; after both sites cold restart, every
    /// written item is present exactly once in final state (a duplicate
    /// delivery attempt is tolerated, not required to be suppressed at the
    /// transport layer - the CRDT merge makes it a no-op), and a new write
    /// made after the restarts still progresses normally.
    /// </summary>
    [Test]
    public async Task Shipper_cursor_and_receiver_hwm_recover_across_restart_without_data_loss()
    {
        var treeId = _fixture.CursorHwmTreeId;
        var expected = Enumerable.Range(0, 8)
            .ToDictionary(i => $"item-{i}", i => Encode($"value-{i}"), StringComparer.Ordinal);

        FaultInjectingReplicationTransport.ScheduleRejectAndPartitionAfterApplyOnce(treeId);
        foreach (var (key, value) in expected)
        {
            await _fixture.TreeOn(Site.A, treeId).SetAsync(key, value);
        }

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            () => Task.FromResult(FaultInjectingReplicationTransport.DeliveryCount(treeId) >= 1),
            "Site B applies the first batch despite the injected rejected ack");

        await _fixture.ColdRestartSiteAsync(Site.A);
        await _fixture.ColdRestartSiteAsync(Site.B);
        _fixture.Heal(Site.A, Site.B);

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            () => Task.FromResult(FaultInjectingReplicationTransport.DeliveryCount(treeId) >= 2),
            "the unacknowledged delivery is replayed after restart");

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                foreach (var (key, value) in expected)
                {
                    var onA = await _fixture.TreeOn(Site.A, treeId).GetAsync(key);
                    var onB = await _fixture.TreeOn(Site.B, treeId).GetAsync(key);
                    if (onA is null || !onA.AsSpan().SequenceEqual(value)
                        || onB is null || !onB.AsSpan().SequenceEqual(value))
                    {
                        return false;
                    }
                }

                return true;
            },
            "both sites retain every item after both cold restart");

        var countOnA = await _fixture.TreeOn(Site.A, treeId).CountAsync();
        var countOnB = await _fixture.TreeOn(Site.B, treeId).CountAsync();
        Assert.Multiple(() =>
        {
            Assert.That(countOnA, Is.EqualTo(expected.Count));
            Assert.That(countOnB, Is.EqualTo(expected.Count));
        });

        await _fixture.TreeOn(Site.B, treeId).SetAsync("post-recovery-key", Encode("post-recovery-value"));

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await _fixture.TreeOn(Site.A, treeId).GetAsync("post-recovery-key");
                return onA is not null && onA.AsSpan().SequenceEqual(Encode("post-recovery-value"));
            },
            "a new write made after both restarts still progresses and converges");
    }

    /// <summary>
    /// Scenario 8: replication resumes after a restart with no manual
    /// shipper wake. A backlog accumulates while Site B is offline; Site A
    /// is then also cold-restarted while Site B is still down. Once Site B
    /// is restored, the suite performs no extra writes and calls no internal
    /// wake API - convergence must complete purely through the production
    /// driver/reminder path (the periodic ship-phase timer and the
    /// reminder-driven maintenance grain), proving the system is
    /// self-healing without operator intervention.
    /// </summary>
    [Test]
    public async Task Replication_resumes_after_restart_without_manual_shipper_wake()
    {
        var treeId = _fixture.NoWakeTreeId;

        await _fixture.StopSiteAsync(Site.B);

        await _fixture.TreeOn(Site.A, treeId).SetAsync("backlog-key-1", Encode("backlog-value-1"));
        await _fixture.TreeOn(Site.A, treeId).SetAsync("backlog-key-2", Encode("backlog-value-2"));

        // Site A itself is also cold-restarted while Site B remains down, so
        // the backlog must survive entirely on Site A's durable WAL/state
        // and resume shipping purely from the production driver once both
        // sites are back - no test code re-primes or wakes anything.
        await _fixture.ColdRestartSiteAsync(Site.A);

        await _fixture.StartSiteAsync(Site.B);

        await DurableActiveActiveClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var v1 = await _fixture.TreeOn(Site.B, treeId).GetAsync("backlog-key-1");
                var v2 = await _fixture.TreeOn(Site.B, treeId).GetAsync("backlog-key-2");
                return v1 is not null && v1.AsSpan().SequenceEqual(Encode("backlog-value-1"))
                    && v2 is not null && v2.AsSpan().SequenceEqual(Encode("backlog-value-2"));
            },
            "the backlog accumulated while Site B was offline ships and converges purely via the production driver/reminder path");
    }

    private static byte[] Encode(string value) => Encoding.UTF8.GetBytes(value);
}
