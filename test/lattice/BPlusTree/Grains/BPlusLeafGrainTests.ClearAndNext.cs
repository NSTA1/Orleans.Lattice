using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Clear_and_next_retires_a_pending_replay_without_waiting_for_it(bool forPurge)
    {
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();
        Assert.That(leaf.Grain.IsReplayPending, Is.True);

        await leaf.Grain.ClearGrainStateAndGetNextSiblingAsync(forPurge).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(leaf.Grain.IsReplayPending, Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Clear_and_next_returns_the_pre_clear_sibling_after_success(bool forPurge)
    {
        var next = GrainId.Create("leaf", "next");
        var state = new FakePersistentState<LeafNodeState>();
        state.State.NextSibling = next;
        var grain = CreateGrain(state);

        Assert.That(await grain.ClearGrainStateAndGetNextSiblingAsync(forPurge), Is.EqualTo(next));
        Assert.That(state.State.NextSibling, Is.Null, "The returned pointer must survive the row clear.");
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Clear_and_next_does_not_report_success_when_the_row_clear_fails(bool forPurge)
    {
        var state = new FakePersistentState<LeafNodeState> { ThrowOnClear = new InvalidOperationException("store fault") };
        var next = GrainId.Create("leaf", "next");
        state.State.NextSibling = next;
        var grain = CreateGrain(state);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.ClearGrainStateAndGetNextSiblingAsync(forPurge));
        Assert.That(state.State.NextSibling, Is.EqualTo(next));
    }
}
