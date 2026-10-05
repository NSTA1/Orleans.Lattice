using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for issue #4464: the causal apply buffer must neither
/// strand nor lose a parked entry. One test per mechanism the replication
/// model reproduces (lost wakeup, volatile buffer, and - with the bootstrap
/// coordinator fixtures - the pin regressing the vector and not draining),
/// plus the cross-silo strand the durable per-tree buffer grain also closes,
/// and the durability contract of that grain (park written before the ack,
/// drained entries removed only after their apply, overflow dead-lettered
/// before removal).
/// </summary>
public partial class ReplicationApplierTests
{
    private static WalRecord BlockedOnSiteC(string key, long ticks, long dependsOnTicks = 50) =>
        SetEntry(key, Hlc(ticks)) with { VectorClock = Vector((OriginC, Hlc(dependsOnTicks))) };

    // Mechanism 1 (EventualConvergenceParkLostWakeup): the entry reads the
    // vector, finds its dependency unmet, and before it parks a concurrent
    // apply meets the dependency and runs its drain against an empty buffer.
    // The park must re-check, or the entry waits for an advance that may
    // never come.
    [Test]
    public async Task ApplyAsync_parked_entry_whose_dependency_was_met_before_the_insert_is_drained_by_the_park()
    {
        var h = CreateCausalHarness();
        var staleCheck = new TaskCompletionSource<CausalDependencyVerdict[]>();
        var noneLost = new HashSet<(string, HybridLogicalClock)>();
        h.Hwm.CheckDependenciesAsync(Arg.Any<IReadOnlyList<VersionVector>>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => staleCheck.Task,
                call => Task.FromResult(CausalDependencyTestDouble.Verdicts(h.Vc, noneLost, (IReadOnlyList<VersionVector>)call[0])));

        var blocked = h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        Assert.That(blocked.IsCompleted, Is.False, "Held at its dependency check.");

        // The dependency applies and drains while the entry is between its
        // check and its park.
        var satisfier = await h.Applier.ApplyAsync(SetEntry("dep", Hlc(50), OriginC));
        Assert.That(satisfier.Applied, Is.True);

        staleCheck.SetResult([CausalDependencyVerdict.Unmet]);
        var parked = await blocked;

        Assert.That(parked.Applied, Is.False, "It took the park branch.");
        await h.Apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
        Assert.That(h.BufferState.State.Entries, Is.Empty);
    }

    // Mechanism 2 (EventualConvergenceVolatileCausalBuffer): a parked entry is
    // acknowledged, so it must survive a receiver restart and be drained after
    // the restart re-arms the buffer.
    [Test]
    public async Task ApplyAsync_parked_entry_survives_a_restart_and_drains_when_its_dependency_arrives()
    {
        var h = CreateCausalHarness();
        var parked = await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        Assert.That(parked.Applied, Is.False);
        Assert.That(h.BufferState.State.Entries, Has.Count.EqualTo(1), "Durable before the ack.");

        // Restart: a fresh applier singleton and a fresh buffer activation
        // reloading the same durable state.
        var restarted = new ReplicationApplier(h.Factory, h.Monitor, replicationContext: new AnyTreeLwwContext());
        CausalBufferTestWiring.Wire(h.Factory, restarted, h.Monitor, Tree, h.BufferState);

        await restarted.ApplyAsync(SetEntry("dep", Hlc(50), OriginC));

        await h.Apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
        Assert.That(h.BufferState.State.Entries, Is.Empty);
    }

    // Mechanism 2, quiescent form: the dependency was already met before the
    // restart (the drain never ran), and nothing more arrives for the tree.
    // The re-arm on reactivation - the maintenance tick, modelled here by a
    // direct DrainAsync on the reloaded grain - drains it.
    [Test]
    public async Task Reloaded_buffer_drains_an_entry_whose_dependency_was_met_before_a_restart_without_further_traffic()
    {
        var h = CreateCausalHarness();
        await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        h.Vc.Entries[OriginC] = Hlc(50);

        var restarted = new ReplicationApplier(h.Factory, h.Monitor, replicationContext: new AnyTreeLwwContext());
        var (grain, _) = CausalBufferTestWiring.Wire(h.Factory, restarted, h.Monitor, Tree, h.BufferState);

        var remaining = await grain.DrainAsync();

        Assert.That(remaining, Is.Zero);
        await h.Apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
    }

    // First-touch re-arm: after a restart, the first advance on a silo drains
    // even though no park happened on this silo.
    [Test]
    public async Task ApplyAsync_first_advance_after_a_restart_drains_entries_parked_before_it()
    {
        var h = CreateCausalHarness();
        await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        h.Vc.Entries[OriginC] = Hlc(50);

        var restarted = new ReplicationApplier(h.Factory, h.Monitor, replicationContext: new AnyTreeLwwContext());
        CausalBufferTestWiring.Wire(h.Factory, restarted, h.Monitor, Tree, h.BufferState);

        await restarted.ApplyAsync(SetEntry("unrelated", Hlc(7), "site-d"));

        await h.Apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
    }

    // Cross-silo: the buffer is per tree, not per silo. An entry parked
    // through silo 2 is drained by the maintenance tick even when the
    // dependency is applied on silo 1, whose hint says the buffer is empty.
    [Test]
    public async Task Entry_parked_through_one_silo_drains_after_its_dependency_applies_on_another()
    {
        var h = CreateCausalHarness();
        var silo1 = h.Applier;
        var silo2 = new ReplicationApplier(h.Factory, h.Monitor, replicationContext: new AnyTreeLwwContext());

        await silo1.ApplyAsync(SetEntry("warm", Hlc(1), "site-d")); // silo 1 touches the tree: buffer empty.
        await silo2.ApplyAsync(BlockedOnSiteC("k", 100));          // parked through silo 2.
        await silo1.ApplyAsync(SetEntry("dep", Hlc(50), OriginC));  // dependency applies on silo 1.

        var remaining = await h.Factory.GetGrain<ICausalApplyBufferGrain>(Tree).DrainAsync(); // maintenance tick

        Assert.That(remaining, Is.Zero);
        await h.Apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
    }

    // A drained entry is removed durably only after its apply returned: a
    // crash between the two leaves it parked, and the next drain re-applies it
    // (idempotent at the leaf - see ReplicationApplyIntegrationTests).
    [Test]
    public async Task Drain_that_fails_to_persist_the_removal_keeps_the_entry_and_re_applies_it_on_the_next_drain()
    {
        var h = CreateCausalHarness();
        await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        h.Vc.Entries[OriginC] = Hlc(50);
        h.BufferState.ThrowOnWrite = new InvalidOperationException("crash before the removal is durable");
        var grain = h.Factory.GetGrain<ICausalApplyBufferGrain>(Tree);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DrainAsync());
        Assert.That(h.BufferState.State.Entries, Has.Count.EqualTo(1), "Still durable.");

        Assert.That(await grain.DrainAsync(), Is.Zero);
        await h.Apply.Received(2).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
        Assert.That(h.BufferState.State.Entries, Is.Empty);
    }

    // A park is durable before ParkAsync returns: a failed write fails the
    // park (the applier then fails the delivery, so the sender re-sends), and
    // nothing is acknowledged while held only in memory.
    [Test]
    public async Task ApplyAsync_fails_the_delivery_when_the_park_cannot_be_written()
    {
        var h = CreateCausalHarness();
        h.BufferState.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100)));
        Assert.That(h.BufferState.State.Entries, Is.Empty);

        // The re-send parks durably.
        var reshipped = await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        Assert.Multiple(() =>
        {
            Assert.That(reshipped.Applied, Is.False);
            Assert.That(h.BufferState.State.Entries, Has.Count.EqualTo(1));
        });
    }

    // Overflow drops an acknowledged entry - a deliberate bound - and the
    // dead-letter enqueue happens before the write that removes it.
    [Test]
    public async Task Overflow_dead_letters_the_evicted_entry_before_its_removal_is_persisted()
    {
        var h = CreateCausalHarness(new LatticeReplicationOptions { ClusterId = LocalCluster, CausalBufferMaxEntries = 1 });
        await h.Applier.ApplyAsync(BlockedOnSiteC("first", 100));
        var removalPersistedAfterDeadLetter = (bool?)null;
        h.BufferState.OnAfterWrite = s =>
        {
            if (!s.Entries.Exists(p => p.Entry.Key == "first"))
            {
                removalPersistedAfterDeadLetter ??= h.Dlq.ReceivedCalls().Any();
            }
        };

        await h.Applier.ApplyAsync(BlockedOnSiteC("second", 101));

        Assert.That(removalPersistedAfterDeadLetter, Is.True);
        await h.Dlq.Received(1).EnqueueAsync(
            Arg.Is<WalRecord>(e => e.Key == "first"),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonHlcSkew,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Overflow_keeps_the_evicted_entry_durable_when_the_dead_letter_enqueue_fails()
    {
        var h = CreateCausalHarness(new LatticeReplicationOptions { ClusterId = LocalCluster, CausalBufferMaxEntries = 1 });
        await h.Applier.ApplyAsync(BlockedOnSiteC("first", 100));
        h.Dlq.EnqueueAsync(default, default!, default, default!, default).ReturnsForAnyArgs(
            Task.FromException<long>(new TimeoutException("dlq down")));

        Assert.ThrowsAsync<TimeoutException>(async () => await h.Applier.ApplyAsync(BlockedOnSiteC("second", 101)));

        Assert.That(h.BufferState.State.Entries.Select(p => p.Entry.Key), Is.EqualTo(new[] { "first" }));
        Assert.That(await h.Factory.GetGrain<ICausalApplyBufferGrain>(Tree).CountAsync(), Is.EqualTo(1));
    }
}
