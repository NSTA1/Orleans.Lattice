using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeReshardGrain.GetProgressAsync"/>
/// (issue 3958): the reshard records where it started, so its progress is
/// measured from there rather than from zero, and reports its target on every
/// status read.
/// </summary>
public partial class TreeReshardGrainTests
{
    [Test]
    public async Task GetProgressAsync_reports_nothing_in_flight_for_an_idle_coordinator()
    {
        var (grain, _, _, _) = CreateGrain();

        var progress = await grain.GetProgressAsync();

        Assert.That(progress, Is.EqualTo(new ReshardProgress(false, 0, 0)));
    }

    [Test]
    public async Task ReshardAsync_records_the_shard_count_the_tree_started_from()
    {
        var (grain, state, _, _) = CreateGrain(physicalShardCount: 2);

        await grain.ReshardAsync(6);

        var progress = await grain.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.State.StartShardCount, Is.EqualTo(2));
            Assert.That(progress.InProgress, Is.True);
            Assert.That(progress.TargetShardCount, Is.EqualTo(6));
            Assert.That(progress.StartShardCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void ReshardAsync_reverts_the_recorded_start_when_the_intent_write_fails()
    {
        var state = new FakePersistentState<TreeReshardState> { State = new TreeReshardState { StartShardCount = 9 } };
        var (grain, _, _, _) = CreateGrain(physicalShardCount: 2, existingState: state);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ReshardAsync(6));

        Assert.That(state.State.StartShardCount, Is.EqualTo(9));
    }

    [Test]
    public async Task GetProgressAsync_survives_reactivation()
    {
        var state = new FakePersistentState<TreeReshardState>();
        var (first, _, _, _) = CreateGrain(physicalShardCount: 2, existingState: state);
        await first.ReshardAsync(6);

        var (second, _, _, _) = CreateGrain(physicalShardCount: 2, existingState: state);

        var progress = await second.GetProgressAsync();
        Assert.That(progress, Is.EqualTo(new ReshardProgress(true, 6, 2)));
    }

    [Test]
    public async Task GetProgressAsync_reports_an_unrecorded_start_as_zero_for_legacy_state()
    {
        var state = new FakePersistentState<TreeReshardState>
        {
            State = new TreeReshardState { InProgress = true, Phase = ReshardPhase.Migrating, TargetShardCount = 8 },
        };
        var (grain, _, _, _) = CreateGrain(existingState: state);

        var progress = await grain.GetProgressAsync();

        Assert.That(progress, Is.EqualTo(new ReshardProgress(true, 8, 0)));
    }
}
