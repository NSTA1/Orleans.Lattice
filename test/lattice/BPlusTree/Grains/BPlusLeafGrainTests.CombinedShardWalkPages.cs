using System.Text;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the combined seams the shard root's bounded range-delete,
/// range-clock and snapshot-baseline freeze walks use, so each hop costs one
/// leaf grain call instead of two or three. Each must answer exactly what the
/// separate payload call plus the separate chain-link reads answered.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId ShardWalkPageSibling = GrainId.Create("leaf", "shard-walk-page-next");

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetKeysAndNextSiblingAsync_preserves_range_and_visibility(bool tail)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "k";
        var grain = CreateGrain(state);
        await grain.SetAsync("b", [1]);
        await grain.SetAsync("c", [2]);
        await grain.SetAsync("d", [3]);
        await grain.DeleteAsync("c");
        if (!tail) await grain.SetNextSiblingAsync(ShardWalkPageSibling);

        var page = await grain.GetKeysAndNextSiblingAsync("b", "d");

        Assert.Multiple(async () =>
        {
            Assert.That(page.Keys, Is.EqualTo(new[] { "b" }));
            Assert.That(page.Keys, Is.EqualTo(await grain.GetKeysAsync("b", "d")));
            Assert.That(page.Link.KeyRange, Is.EqualTo((await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange));
            Assert.That(page.Link.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RebuildProjectionAndNextSiblingAsync_returns_pre_reset_topology(bool tail)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "k";
        var grain = CreateGrain(state);
        await grain.SetAsync("b", [1]);
        if (!tail) await grain.SetNextSiblingAsync(ShardWalkPageSibling);
        var bounds = (await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange;
        var next = await grain.GetNextSiblingAsync();

        var link = await grain.RebuildProjectionAndNextSiblingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(link.KeyRange, Is.EqualTo(bounds));
            Assert.That(link.NextSibling, Is.EqualTo(next));
            Assert.That(grain.EntriesForTest, Is.Empty);
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1));
        });
    }

    [Test]
    public void RebuildProjectionAndNextSiblingAsync_propagates_failed_reset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var failure = new IOException("reset persist failed");
        state.ThrowOnWrite = failure;

        Assert.That(async () => await grain.RebuildProjectionAndNextSiblingAsync(),
            Throws.Exception.SameAs(failure));
    }

    [Test]
    public async Task RebuildProjectionAndNextSiblingAsync_retires_a_wedged_replay_instead_of_waiting()
    {
        using var leaf = new WedgedReplayLeaf();
        await leaf.ActivateAndParkAsync();

        await leaf.Grain.RebuildProjectionAndNextSiblingAsync().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(-1));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetProjectionCheckpointAndNextSiblingAsync_matches_metadata_reads(bool tail)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "k";
        state.State.ProjectionCheckpointOffset = 42;
        var grain = CreateGrain(state);
        if (!tail) await grain.SetNextSiblingAsync(ShardWalkPageSibling);

        var page = await grain.GetProjectionCheckpointAndNextSiblingAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(page.CheckpointOffset, Is.EqualTo(42L));
            Assert.That(page.Link.KeyRange, Is.EqualTo((await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange));
            Assert.That(page.Link.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
        });
    }

    [Test]
    public async Task DeleteRangeAndNextSiblingAsync_deletes_like_the_separate_call_and_carries_the_link()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "k";
        var grain = CreateGrain(state);
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("3"));
        await grain.SetNextSiblingAsync(ShardWalkPageSibling);

        var page = await grain.DeleteRangeAndNextSiblingAsync("b", "d");

        Assert.Multiple(async () =>
        {
            Assert.That(page.Result.Deleted, Is.EqualTo(2));
            Assert.That(await grain.GetAsync("b"), Is.Null);
            Assert.That(await grain.GetAsync("c"), Is.Null);
            Assert.That(await grain.GetAsync("e"), Is.Not.Null);
            Assert.That(page.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
            Assert.That(page.NextSibling, Is.EqualTo(ShardWalkPageSibling));
            Assert.That(page.KeyRange, Is.EqualTo((await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange));
            Assert.That(page.KeyRange.HighKeyExclusive, Is.EqualTo("k"));
        });
    }

    [Test]
    public async Task DeleteRangeAndNextSiblingAsync_reports_null_at_the_chain_tail()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var page = await grain.DeleteRangeAndNextSiblingAsync("a", "z");

        Assert.Multiple(() =>
        {
            Assert.That(page.Result.Deleted, Is.EqualTo(1));
            Assert.That(page.NextSibling, Is.Null);
        });
    }

    [Test]
    public async Task DeleteRangeAndNextSiblingAsync_on_an_empty_leaf_still_carries_the_link()
    {
        var grain = CreateGrain();
        await grain.SetNextSiblingAsync(ShardWalkPageSibling);

        var page = await grain.DeleteRangeAndNextSiblingAsync("a", "z");

        Assert.Multiple(() =>
        {
            Assert.That(page.Result.Deleted, Is.EqualTo(0));
            Assert.That(page.NextSibling, Is.EqualTo(ShardWalkPageSibling));
        });
    }

    [Test]
    public async Task GetClockAndNextSiblingAsync_matches_the_separate_calls()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "f";
        state.State.HighKeyExclusive = "m";
        var grain = CreateGrain(state);
        await grain.SetAsync("g", Encoding.UTF8.GetBytes("1"));
        await grain.SetNextSiblingAsync(ShardWalkPageSibling);

        var page = await grain.GetClockAndNextSiblingAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(page.Clock, Is.EqualTo(await grain.GetClockAsync()));
            Assert.That(page.Clock, Is.Not.EqualTo(default(HybridLogicalClock)), "the write ticked the clock");
            Assert.That(page.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
            Assert.That(page.NextSibling, Is.EqualTo(ShardWalkPageSibling));
            Assert.That(page.KeyRange, Is.EqualTo((await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange));
            Assert.That(page.KeyRange.LowKeyInclusive, Is.EqualTo("f"));
        });
    }

    [Test]
    public async Task GetClockAndNextSiblingAsync_reports_null_at_the_chain_tail()
    {
        var grain = CreateGrain();

        var page = await grain.GetClockAndNextSiblingAsync();

        Assert.That(page.NextSibling, Is.Null);
    }

    [Test]
    public async Task FreezeProjectionAndNextSiblingAsync_freezes_like_the_separate_call_and_carries_the_link()
    {
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 4, Entries: []));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetNextSiblingAsync(ShardWalkPageSibling);

        var page = await grain.FreezeProjectionAndNextSiblingAsync(CancellationToken.None);
        var separate = await grain.FreezeProjectionAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(page.Freeze.Rows.Select(r => r.Key), Is.EqualTo(separate.Rows.Select(r => r.Key)));
            Assert.That(page.Freeze.Rows.Select(r => r.Key), Does.Contain("k1"));
            Assert.That(page.Freeze.FrontierPerPartition, Is.EqualTo(separate.FrontierPerPartition));
            Assert.That(page.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
            Assert.That(page.NextSibling, Is.EqualTo(ShardWalkPageSibling));
        });
    }

    [Test]
    public async Task FreezeProjectionAndNextSiblingAsync_reports_null_at_the_chain_tail()
    {
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 0, Entries: []));

        var page = await grain.FreezeProjectionAndNextSiblingAsync(CancellationToken.None);

        Assert.That(page.NextSibling, Is.Null);
    }

    [Test]
    public void FreezeProjectionAndNextSiblingAsync_honours_a_pre_cancelled_token()
    {
        var (grain, _) = CreateFrozenBaselineGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.FreezeProjectionAndNextSiblingAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
