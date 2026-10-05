using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The dependency check on the high-water-mark grain (issues #4586, #4603): a
/// dependency names exactly one write. It is met when the tree recorded that
/// write as applied, and otherwise decided by the origin's frontier - lost
/// marks included - never by the per-origin maximum HLC.
/// </summary>
public partial class ReplicationHighWaterMarkGrainTests
{
    [Test]
    public async Task A_recorded_identity_is_met_and_a_higher_high_water_mark_alone_is_not()
    {
        var grain = CreateGrain();
        await grain.AdvanceAppliedAsync(OriginA, Hlc(50), [Hlc(50)], advanceHighWaterMark: true, CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync(
            [Vector((OriginA, Hlc(50))), Vector((OriginA, Hlc(40)))],
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Met), "the exact write was applied");
            Assert.That(verdicts[1], Is.EqualTo(CausalDependencyVerdict.Unmet),
                "the high-water mark is above 40, but the write at 40 never arrived (#1060)");
        });
    }

    [Test]
    public async Task ResetAppliedIdentitiesAsync_forgets_the_record_but_keeps_the_high_water_mark()
    {
        var grain = CreateGrain();
        await grain.AdvanceAppliedAsync(OriginA, Hlc(50), [Hlc(50)], advanceHighWaterMark: true, CancellationToken.None);

        await grain.ResetAppliedIdentitiesAsync(CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync([Vector((OriginA, Hlc(50)))], CancellationToken.None);
        var hwm = await grain.GetAsync(OriginA, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Unmet),
                "after a lineage change the write may no longer be in the tree");
            Assert.That(hwm, Is.EqualTo(Hlc(50)), "the reset is about identities, not the shipping cursor");
        });
    }

    [Test]
    public async Task PinSnapshotAsync_forgets_the_applied_identity_record()
    {
        var grain = CreateGrain();
        await grain.AdvanceAppliedAsync(OriginA, Hlc(50), [Hlc(50)], advanceHighWaterMark: true, CancellationToken.None);

        await grain.PinSnapshotAsync(Hlc(10), Vector((OriginA, Hlc(10))), CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync([Vector((OriginA, Hlc(50)))], CancellationToken.None);
        Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Unmet), "a re-seed replaces the tree's contents");
    }

    [Test]
    public void ResetAppliedIdentitiesAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(() => grain.ResetAppliedIdentitiesAsync(cts.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task AdvanceAppliedAsync_records_identities_and_advances_only_when_asked()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);

        var withoutAdvance = await grain.AdvanceAppliedAsync(OriginA, Hlc(30), [Hlc(30)], advanceHighWaterMark: false, CancellationToken.None);
        var withAdvance = await grain.AdvanceAppliedAsync(OriginA, Hlc(31), [Hlc(31)], advanceHighWaterMark: true, CancellationToken.None);
        var verdicts = await grain.CheckDependenciesAsync([Vector((OriginA, Hlc(30)), (OriginA, Hlc(31)))], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(withoutAdvance, Is.False);
            Assert.That(withAdvance, Is.True);
            Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(31)));
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Met));
        });
    }

    [Test]
    public async Task The_identity_record_forgets_the_oldest_past_its_capacity()
    {
        var grain = HighWaterMarkTestGrains.Real(options: new LatticeReplicationOptions { CausalAppliedIdentityCapacity = 2 });
        await grain.AdvanceAppliedAsync(OriginA, Hlc(3), [Hlc(1), Hlc(2), Hlc(3)], advanceHighWaterMark: false, CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync(
            [Vector((OriginA, Hlc(1))), Vector((OriginA, Hlc(3)))],
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Unmet), "evicted, and no low watermark covers it");
            Assert.That(verdicts[1], Is.EqualTo(CausalDependencyVerdict.Met));
        });
    }

    [Test]
    public async Task A_forgotten_identity_is_met_once_the_origin_low_watermark_passes_it()
    {
        var frontiers = new Dictionary<string, ReplicationOriginFrontierGrain>(StringComparer.Ordinal);
        var grain = HighWaterMarkTestGrains.Real(grainFactory: HighWaterMarkTestGrains.FrontierFactory(frontiers));
        await grain.CheckDependenciesAsync([Vector((OriginA, Hlc(7)))], CancellationToken.None);

        await frontiers[OriginA].RecordLowWatermarkAsync(Hlc(8), generation: 0, CancellationToken.None);
        var verdicts = await grain.CheckDependenciesAsync([Vector((OriginA, Hlc(7))), Vector((OriginA, Hlc(8)))], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Met), "every write of A below 8 was acknowledged and none is held");
            Assert.That(verdicts[1], Is.EqualTo(CausalDependencyVerdict.Unmet), "the low watermark is strict");
        });
    }

    [Test]
    public async Task A_lost_dependency_wins_over_met_and_unmet_whatever_its_position()
    {
        var grain = CreateGrain();
        await grain.AdvanceAppliedAsync(OriginA, Hlc(99), [Hlc(99)], advanceHighWaterMark: true, CancellationToken.None);
        await grain.RecordLostAsync(OriginB, Hlc(7), CancellationToken.None);

        var verdicts = await grain.CheckDependenciesAsync(
            [Vector((OriginA, Hlc(99)), (OriginB, Hlc(7))), Vector((OriginA, Hlc(98)), (OriginB, Hlc(7)))],
            CancellationToken.None);

        Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Lost, CausalDependencyVerdict.Lost }));
    }

    [Test]
    public async Task RecordLostAsync_marks_the_write_on_the_origin_frontier_for_every_tree()
    {
        var frontiers = new Dictionary<string, ReplicationOriginFrontierGrain>(StringComparer.Ordinal);
        var factory = HighWaterMarkTestGrains.FrontierFactory(frontiers);
        var treeOne = HighWaterMarkTestGrains.Real(grainFactory: factory, treeId: "one");
        var treeTwo = HighWaterMarkTestGrains.Real(grainFactory: factory, treeId: "two");

        await treeOne.RecordLostAsync(OriginA, Hlc(7), CancellationToken.None);
        var verdicts = await treeTwo.CheckDependenciesAsync([Vector((OriginA, Hlc(7)))], CancellationToken.None);

        Assert.That(verdicts, Is.EqualTo(new[] { CausalDependencyVerdict.Lost }),
            "a dependency names an origin's write, not a tree");
    }

    [Test]
    public void Lost_and_dependency_methods_validate_arguments()
    {
        var grain = CreateGrain();

        Assert.Multiple(() =>
        {
            Assert.That(() => grain.RecordLostAsync(null!, Hlc(1), CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => grain.CheckDependenciesAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => grain.AdvanceAppliedAsync(OriginA, Hlc(1), null!, false, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => grain.AdvanceAppliedAsync(null!, Hlc(1), [], false, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
        });
    }
}
