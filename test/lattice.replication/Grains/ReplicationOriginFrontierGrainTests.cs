using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>The receiver's per-origin causal frontier (issue #4586).</summary>
[TestFixture]
public sealed class ReplicationOriginFrontierGrainTests
{
    private const string Origin = "site-c";
    private const string Tree = "t1";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public async Task The_low_watermark_only_rises()
    {
        var grain = HighWaterMarkTestGrains.Frontier(Origin);

        var first = await grain.RecordLowWatermarkAsync(Hlc(10), CancellationToken.None);
        var lower = await grain.RecordLowWatermarkAsync(Hlc(5), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(lower, Is.False);
        });
        Assert.That(await grain.GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(10)));
    }

    [Test]
    public async Task A_write_a_source_still_holds_stays_unmet_below_the_low_watermark()
    {
        var factory = Substitute.For<IGrainFactory>();
        var buffer = Substitute.For<ICausalApplyBufferGrain>();
        buffer.IsHoldingAsync(Origin, Hlc(4)).Returns(true);
        factory.GetGrain<ICausalApplyBufferGrain>(Tree, Arg.Any<string?>()).Returns(buffer);
        var grain = HighWaterMarkTestGrains.Frontier(Origin, factory);
        await grain.SetHeldAsync(ReplicationOriginFrontierGrain.BufferSource(Tree), [Hlc(4)], CancellationToken.None);
        await grain.RecordLowWatermarkAsync(Hlc(10), CancellationToken.None);

        var verdicts = await grain.CheckAsync([Hlc(4), Hlc(5)], CancellationToken.None);

        Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Unmet, CausalDependencyVerdict.Met }));
    }

    [Test]
    public async Task A_listing_its_source_no_longer_holds_is_dropped_and_the_write_is_met()
    {
        var factory = Substitute.For<IGrainFactory>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.IsHoldingAsync(Origin, Hlc(4), Arg.Any<CancellationToken>()).Returns(false);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree, Arg.Any<string?>()).Returns(dlq);
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, factory, state);
        await grain.SetHeldAsync(ReplicationOriginFrontierGrain.DeadLetterSource(Tree), [Hlc(4)], CancellationToken.None);
        await grain.RecordLowWatermarkAsync(Hlc(10), CancellationToken.None);

        var verdicts = await grain.CheckAsync([Hlc(4)], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Met }), "a crash left the listing behind; the source is authoritative");
            Assert.That(state.State.HeldBySource, Is.Empty, "the stale listing is dropped durably");
        });
    }

    [Test]
    public async Task Lost_marks_are_durable_idempotent_and_win()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        await grain.RecordLostAsync([Hlc(4)], CancellationToken.None);
        await grain.RecordLostAsync([Hlc(4)], CancellationToken.None);
        await grain.RecordLowWatermarkAsync(Hlc(10), CancellationToken.None);
        var reactivated = HighWaterMarkTestGrains.Frontier(Origin, state: state);
        var verdicts = await reactivated.CheckAsync([Hlc(4)], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Lost }));
        });
    }

    [Test]
    public void A_failed_write_rolls_the_lost_set_back()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState> { ThrowOnWrite = new InvalidOperationException("down") };
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.RecordLostAsync([Hlc(4)], CancellationToken.None));

        Assert.That(state.State.Lost, Is.Empty);
    }

    [Test]
    public void A_failed_write_rolls_the_held_set_back()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState> { ThrowOnWrite = new InvalidOperationException("down") };
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.SetHeldAsync("b|t", [Hlc(4)], CancellationToken.None));

        Assert.That(state.State.HeldBySource, Is.Empty);
    }

    [Test]
    public async Task An_empty_held_set_removes_the_source()
    {
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        var grain = HighWaterMarkTestGrains.Frontier(Origin, state: state);

        await grain.SetHeldAsync("b|t", [Hlc(4)], CancellationToken.None);
        await grain.SetHeldAsync("b|t", [Hlc(4)], CancellationToken.None);
        await grain.SetHeldAsync("b|t", Array.Empty<HybridLogicalClock>(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.HeldBySource, Is.Empty);
            Assert.That(state.WriteCount, Is.EqualTo(2), "an unchanged set is not rewritten");
        });
    }

    [Test]
    public void Arguments_are_validated()
    {
        var grain = HighWaterMarkTestGrains.Frontier(Origin);

        Assert.Multiple(() =>
        {
            Assert.That(() => grain.SetHeldAsync("", [], CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => grain.SetHeldAsync("b|t", null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => grain.RecordLostAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => grain.CheckAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }
}
