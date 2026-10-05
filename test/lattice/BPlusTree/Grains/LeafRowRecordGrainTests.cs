using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="LeafRowRecordGrain"/> (issue #4654): the leaf's
/// proof, outside its own row, that the row was once written.
/// </summary>
[TestFixture]
public class LeafRowRecordGrainTests
{
    [Test]
    public async Task GetAsync_returns_null_when_nothing_was_recorded()
    {
        var state = new FakePersistentState<LeafRowRecordState> { RecordExistsValue = false };
        Assert.That(await new LeafRowRecordGrain(state).GetAsync(), Is.Null);
    }

    [Test]
    public async Task RecordAsync_persists_the_record_with_its_tree()
    {
        var state = new FakePersistentState<LeafRowRecordState> { RecordExistsValue = false };
        var grain = new LeafRowRecordGrain(state);

        await grain.RecordAsync("tree-a");
        state.RecordExistsValue = true;

        Assert.Multiple(async () =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That((await grain.GetAsync())?.TreeId, Is.EqualTo("tree-a"));
        });
    }

    [Test]
    public async Task RecordAsync_records_an_unbound_leaf()
    {
        var state = new FakePersistentState<LeafRowRecordState> { RecordExistsValue = false };
        var grain = new LeafRowRecordGrain(state);

        await grain.RecordAsync(null);
        state.RecordExistsValue = true;

        Assert.Multiple(async () =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1), "An unbound leaf's row is still a row whose loss must be noticed.");
            Assert.That(await grain.GetAsync(), Is.Not.Null);
        });
    }

    [Test]
    public async Task RecordAsync_that_changes_nothing_does_not_write()
    {
        var state = new FakePersistentState<LeafRowRecordState>();
        state.State.TreeId = "tree-a";
        var grain = new LeafRowRecordGrain(state);

        await grain.RecordAsync("tree-a");
        await grain.RecordAsync(null);

        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task RecordAsync_rebinding_to_another_tree_rewrites_the_record()
    {
        var state = new FakePersistentState<LeafRowRecordState>();
        state.State.TreeId = "tree-a";
        var grain = new LeafRowRecordGrain(state);

        await grain.RecordAsync("tree-b");

        Assert.That((await grain.GetAsync())?.TreeId, Is.EqualTo("tree-b"));
    }

    [Test]
    public void RecordAsync_that_fails_to_write_leaves_the_record_where_it_was()
    {
        var state = new FakePersistentState<LeafRowRecordState>();
        state.State.TreeId = "tree-a";
        state.ThrowOnWrite = new InvalidOperationException("storage down");
        var grain = new LeafRowRecordGrain(state);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.RecordAsync("tree-b"));
        Assert.That(state.State.TreeId, Is.EqualTo("tree-a"));
    }

    [Test]
    public async Task ClearAsync_removes_the_record()
    {
        var state = new FakePersistentState<LeafRowRecordState>();
        state.State.TreeId = "tree-a";
        var grain = new LeafRowRecordGrain(state);

        await grain.ClearAsync();
        state.RecordExistsValue = false;

        Assert.That(await grain.GetAsync(), Is.Null);
    }
}
