using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage for <see cref="SagaWriteFenceGrain"/> (issue #1173), the
/// durable group-atomic write-fence and shipping-pause primitive. Drives engage,
/// the local-flip write unblock, the abort/compensation full lift, the
/// deadline-based write-fence self-lift, group-atomic multi-tree fan-out, and -
/// the core safety property - that shipping resume is gated on observed global
/// saga completion (the laggard case), not on a single local flip.
/// </summary>
[TestFixture]
public class SagaWriteFenceGrainTests
{
    private const string SagaId = "saga-cutover-1";
    private const int ShardCount = 2;

    private sealed class Harness
    {
        public required SagaWriteFenceGrain Grain { get; init; }
        public required FakePersistentState<SagaWriteFenceState> State { get; init; }
        public required IShardRootGrain Shard { get; init; }
        public required IReplicationShipperGrain Shipper { get; init; }
        public required ITreeReceiveFenceGrain Receive { get; init; }
        public required FakeSagaCompletionSource Completion { get; init; }
    }

    private static Harness CreateGrain(IEnumerable<string> peers)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-write-fence", SagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ShardCount));

        var shard = Substitute.For<IShardRootGrain>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        var receive = Substitute.For<ITreeReceiveFenceGrain>();

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        factory.GetGrain<ITreeReceiveFenceGrain>(Arg.Any<string>()).Returns(receive);

        var topology = new FakeReplicationTopology(peers);
        var completion = new FakeSagaCompletionSource();

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.CurrentValue.Returns(new LatticeOptions());

        var state = new FakePersistentState<SagaWriteFenceState>();
        var grain = new SagaWriteFenceGrain(
            context, reminders, NullLogger<SagaWriteFenceGrain>.Instance,
            state, shardCounts, topology, factory, completion, options);

        return new Harness
        {
            Grain = grain,
            State = state,
            Shard = shard,
            Shipper = shipper,
            Receive = receive,
            Completion = completion,
        };
    }

    private static SagaWriteFenceRequest Request(params string[] trees) => new()
    {
        SagaId = SagaId,
        Trees = [.. trees],
        CoordinatorClusterId = "site-home",
        FenceWindowSeconds = 0,
    };

    [Test]
    public async Task Engage_fences_every_shard_and_pauses_shipping_and_receive()
    {
        var h = CreateGrain(["peer-a"]);

        await h.Grain.EngageAsync(Request("orders"));

        // Write fence engaged on every shard of the tree.
        await h.Shard.Received(ShardCount).EngageWriteFenceAsync(SagaId, Arg.Any<long>());
        // Shipping paused for every (tree, peer).
        await h.Shipper.Received(1).PauseShippingAsync(SagaId, Arg.Any<CancellationToken>());
        // Inbound apply paused for the tree.
        await h.Receive.Received(1).PauseAsync(SagaId);

        var snap = await h.Grain.GetSnapshotAsync();
        Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.Engaged));
        Assert.That(snap.WritesUnblocked, Is.False);
        Assert.That(snap.ShippingResumed, Is.False);
    }

    [Test]
    public async Task Engage_rejects_a_request_whose_saga_id_does_not_match_the_key()
    {
        var h = CreateGrain(["peer-a"]);
        var mismatched = new SagaWriteFenceRequest
        {
            SagaId = "some-other-saga",
            Trees = ["orders"],
            CoordinatorClusterId = "site-home",
        };

        Assert.That(() => h.Grain.EngageAsync(mismatched), Throws.InstanceOf<ArgumentException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task UnblockWrites_lifts_the_write_fence_only_and_keeps_shipping_paused()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));

        await h.Grain.UnblockWritesAsync();

        // Write fence lifted on every shard...
        await h.Shard.Received(ShardCount).LiftWriteFenceAsync(SagaId);
        // ...but shipping and receive stay paused (no resume yet).
        await h.Shipper.DidNotReceive().ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        await h.Receive.DidNotReceive().ResumeAsync(SagaId);

        var snap = await h.Grain.GetSnapshotAsync();
        Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.WritesUnblocked));
        Assert.That(snap.WritesUnblocked, Is.True);
        Assert.That(snap.ShippingResumed, Is.False);
    }

    [Test]
    public async Task Laggard_does_not_resume_shipping_until_global_completion()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));
        await h.Grain.UnblockWritesAsync(); // local flip already happened

        // Not yet globally complete: a poll must NOT resume shipping. This is
        // the core safety property - an early-flipping cluster must not
        // re-advance from a laggard's still-advanced post-cut entries.
        h.Completion.Complete = false;
        var stillPaused = await h.Grain.PollResumeAsync();

        await h.Shipper.DidNotReceive().ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        await h.Receive.DidNotReceive().ResumeAsync(SagaId);
        Assert.That(stillPaused.ShippingResumed, Is.False);
        Assert.That(stillPaused.Phase, Is.EqualTo(SagaWriteFencePhase.WritesUnblocked));

        // Every participant has now flipped: the next poll resumes shipping.
        h.Completion.Complete = true;
        var resumed = await h.Grain.PollResumeAsync();

        await h.Shipper.Received(1).ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        await h.Receive.Received(1).ResumeAsync(SagaId);
        Assert.That(resumed.ShippingResumed, Is.True);
        Assert.That(resumed.Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
    }

    [Test]
    public async Task Write_fence_self_lifts_on_deadline_but_shipping_stays_paused()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));

        // Force the cutover deadline into the past and keep the saga incomplete.
        h.State.State.FenceDeadlineTicks = DateTime.UtcNow.AddSeconds(-1).Ticks;
        h.Completion.Complete = false;

        var snap = await h.Grain.PollResumeAsync();

        // Write fence self-lifted (never strand writes)...
        await h.Shard.Received(ShardCount).LiftWriteFenceAsync(SagaId);
        Assert.That(snap.WritesUnblocked, Is.True);
        Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.WritesUnblocked));
        // ...but shipping is still gated on global completion.
        await h.Shipper.DidNotReceive().ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        Assert.That(snap.ShippingResumed, Is.False);
    }

    [Test]
    public async Task Lift_fully_releases_write_fence_shipping_and_receive()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));

        // Abort/compensation terminal decision: nothing post-cut exists to
        // re-propagate, so the whole fence lifts immediately.
        await h.Grain.LiftAsync();

        await h.Shard.Received(ShardCount).LiftWriteFenceAsync(SagaId);
        await h.Shipper.Received(1).ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        await h.Receive.Received(1).ResumeAsync(SagaId);

        var snap = await h.Grain.GetSnapshotAsync();
        Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
        Assert.That(snap.WritesUnblocked, Is.True);
        Assert.That(snap.ShippingResumed, Is.True);
    }

    [Test]
    public async Task Group_atomic_multi_tree_fences_and_lifts_every_tree_together()
    {
        var h = CreateGrain(["peer-a"]);

        await h.Grain.EngageAsync(Request("orders", "orders-index"));

        // Both trees fenced together: shard engage runs once per shard per tree.
        await h.Shard.Received(ShardCount * 2).EngageWriteFenceAsync(SagaId, Arg.Any<long>());
        await h.Receive.Received(2).PauseAsync(SagaId);

        var engaged = await h.Grain.GetSnapshotAsync();
        Assert.That(engaged.Trees, Is.EquivalentTo(new[] { "orders", "orders-index" }));

        // And lifted together on the terminal decision.
        await h.Grain.LiftAsync();
        await h.Shard.Received(ShardCount * 2).LiftWriteFenceAsync(SagaId);
        await h.Receive.Received(2).ResumeAsync(SagaId);
    }

    [Test]
    public async Task Poll_after_engage_resumes_when_completion_is_already_observed()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(Request("orders"));

        // Commit path where global completion is observed on the first poll,
        // without a prior explicit local write unblock.
        h.Completion.Complete = true;
        var snap = await h.Grain.PollResumeAsync();

        await h.Shipper.Received(1).ResumeShippingAsync(SagaId, Arg.Any<CancellationToken>());
        Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
        Assert.That(h.Completion.ProbeCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task Crash_restart_recovers_durable_state_and_self_lifts()
    {
        // Simulate a crash mid-fence: a first activation engages, then a fresh
        // activation is built over the SAME persisted state (as after a crash).
        var first = CreateGrain(["peer-a"]);
        await first.Grain.EngageAsync(Request("orders"));

        // Rebuild a new grain over the persisted state, with the deadline in the
        // past to model a coordinator that never returned.
        first.State.State.FenceDeadlineTicks = DateTime.UtcNow.AddSeconds(-1).Ticks;

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-write-fence", SagaId));
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ShardCount));
        var shard = Substitute.For<IShardRootGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationShipperGrain>());
        factory.GetGrain<ITreeReceiveFenceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<ITreeReceiveFenceGrain>());
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.CurrentValue.Returns(new LatticeOptions());

        var recovered = new SagaWriteFenceGrain(
            context, reminders, NullLogger<SagaWriteFenceGrain>.Instance,
            first.State, shardCounts, new FakeReplicationTopology(["peer-a"]),
            factory, new FakeSagaCompletionSource(), options);

        // The recovered activation still sees the engaged fence and, on its
        // recovery poll, self-lifts the write fence so the tree is never
        // stranded.
        var snap = await recovered.PollResumeAsync();

        await shard.Received(ShardCount).LiftWriteFenceAsync(SagaId);
        Assert.That(snap.WritesUnblocked, Is.True);
    }
}
