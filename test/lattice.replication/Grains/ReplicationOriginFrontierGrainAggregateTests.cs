using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The origin frontier's shipped aggregate and per-tree caps (issue #4586 part 2b):
/// generation ordering, caps that bound the effective watermark, the minimum
/// generation a lift installs, and what is persisted when.
/// </summary>
[TestFixture]
public sealed class ReplicationOriginFrontierGrainAggregateTests
{
    private const string Origin = "site-c";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public async Task A_newer_generation_replaces_the_aggregate_even_downwards_and_an_older_one_is_ignored()
    {
        var grain = HighWaterMarkTestGrains.Frontier(Origin);
        await grain.RecordLowWatermarkAsync(Hlc(50), generation: 1, CancellationToken.None);

        var newer = await grain.RecordLowWatermarkAsync(Hlc(20), generation: 2, CancellationToken.None);
        var older = await grain.RecordLowWatermarkAsync(Hlc(90), generation: 1, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(newer, Is.True);
            Assert.That(older, Is.False, "an older generation may count a tree's coverage from before its lineage changed");
            Assert.That(await grain.GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(20)));
        });
    }

    [Test]
    public async Task A_tree_cap_bounds_the_effective_watermark_and_the_dependency_check()
    {
        var grain = HighWaterMarkTestGrains.Frontier(Origin);
        await grain.RecordLowWatermarkAsync(Hlc(100), generation: 0, CancellationToken.None);

        await grain.SetTreeCapAsync("t1", Hlc(30), CancellationToken.None);
        await grain.SetTreeCapAsync("t2", Hlc(60), CancellationToken.None);

        var effective = await grain.GetLowWatermarkAsync(CancellationToken.None);
        var verdicts = await grain.CheckAsync([Hlc(29), Hlc(30), Hlc(80)], CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(effective, Is.EqualTo(Hlc(30)));
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Met));
            Assert.That(verdicts[1], Is.EqualTo(CausalDependencyVerdict.Unmet), "strict: the cap itself is not covered");
            Assert.That(verdicts[2], Is.EqualTo(CausalDependencyVerdict.Unmet), "the aggregate alone may count t1's lost coverage");
        });
    }

    [Test]
    public async Task Lifting_a_cap_installs_its_generation_as_the_oldest_accepted()
    {
        var grain = HighWaterMarkTestGrains.Frontier(Origin);
        await grain.RecordLowWatermarkAsync(Hlc(100), generation: 3, CancellationToken.None);
        await grain.SetTreeCapAsync("t1", HybridLogicalClock.Zero, CancellationToken.None);

        await grain.LiftTreeCapAsync("t1", generation: 5, CancellationToken.None);
        var stale = await grain.RecordLowWatermarkAsync(Hlc(200), generation: 4, CancellationToken.None);
        var afterLift = await grain.GetLowWatermarkAsync(CancellationToken.None);
        var fresh = await grain.RecordLowWatermarkAsync(Hlc(40), generation: 5, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(afterLift, Is.EqualTo(HybridLogicalClock.Zero),
                "the generation-3 aggregate predates the tree's re-cover, so it no longer counts");
            Assert.That(stale, Is.False, "a late batch from before the re-cover is ignored");
            Assert.That(fresh, Is.True);
            Assert.That(await grain.GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(40)));
        });
    }

    [Test]
    public async Task Lifting_a_tree_that_holds_no_cap_changes_nothing()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);
        await grain.RecordLowWatermarkAsync(Hlc(100), generation: 3, CancellationToken.None);
        var writes = state.WriteCount;

        await grain.LiftTreeCapAsync("t1", generation: 9, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writes));
            Assert.That(state.State.MinGeneration, Is.Zero);
            Assert.That(await grain.GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(100)));
        });
    }

    [Test]
    public async Task A_cap_and_a_lift_are_durable_before_they_return()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        await grain.SetTreeCapAsync("t1", Hlc(7), CancellationToken.None);
        var capped = state.WriteCount;
        await grain.LiftTreeCapAsync("t1", generation: 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(capped, Is.EqualTo(1));
            Assert.That(state.WriteCount, Is.EqualTo(2));
            Assert.That(state.State.TreeCaps, Is.Empty);
            Assert.That(state.State.MinGeneration, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_cap_whose_write_fails_does_not_take_effect_and_the_failure_propagates()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState> { ThrowOnWrite = new InvalidOperationException("store down") };
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        Assert.That(() => grain.SetTreeCapAsync("t1", Hlc(7), CancellationToken.None), Throws.InstanceOf<InvalidOperationException>());
        Assert.That(state.State.TreeCaps, Is.Empty);
    }

    [Test]
    public async Task A_lift_whose_write_fails_keeps_the_cap_and_the_old_generation()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);
        await grain.SetTreeCapAsync("t1", Hlc(7), CancellationToken.None);
        state.ThrowOnWrite = new InvalidOperationException("store down");

        Assert.That(() => grain.LiftTreeCapAsync("t1", generation: 4, CancellationToken.None), Throws.InstanceOf<InvalidOperationException>());
        Assert.Multiple(() =>
        {
            Assert.That(state.State.TreeCaps["t1"], Is.EqualTo(Hlc(7)));
            Assert.That(state.State.MinGeneration, Is.Zero);
        });
    }

    [Test]
    public async Task A_raised_aggregate_is_not_written_on_every_shipment_but_is_written_at_deactivation()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        for (var i = 1; i <= 20; i++)
        {
            await grain.RecordLowWatermarkAsync(Hlc(i), generation: 0, CancellationToken.None);
        }

        var whileShipping = state.WriteCount;
        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(whileShipping, Is.LessThan(20), "a lagging stored aggregate only delays");
            Assert.That(state.WriteCount, Is.GreaterThan(whileShipping));
            Assert.That(state.State.AggregateLowWatermark, Is.EqualTo(Hlc(20)));
        });
    }

    [Test]
    public void Cap_arguments_are_validated()
    {
        var grain = HighWaterMarkTestGrains.Frontier(Origin);

        Assert.Multiple(() =>
        {
            Assert.That(() => grain.SetTreeCapAsync("", Hlc(1), CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => grain.LiftTreeCapAsync(null!, 1, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
        });
    }
}
