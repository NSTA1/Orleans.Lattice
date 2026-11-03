using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// What the old rightmost leaf hands a shard's bulk graft when it narrows, beyond its
/// rows: the read gates of the rows it relocates (issue #5755), and how far it had
/// replayed the WAL (issue #5752).
/// <para>
/// A bulk graft moves rows between leaves just as a split and a span forward do, so
/// it owes the same transfer of saga isolation: a key prepared on the old leaf, or a
/// migrated row a shadow marker gates there, would otherwise reach its appended leaf
/// ungated, and a committed-without-backstop read there would serve the pre-saga
/// value while a sibling key reads committed (the #3117 split). These mirror
/// <c>BPlusLeafGrainTests.SplitShadowTransfer</c>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId GraftFirst = GrainId.Create("leaf", "graft-first");
    private static readonly GrainId GraftSecond = GrainId.Create("leaf", "graft-second");

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusLeafGrain First, IBPlusLeafGrain Second)
        CreateGraftDonor()
    {
        var first = Substitute.For<IBPlusLeafGrain>();
        var second = Substitute.For<IBPlusLeafGrain>();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state,
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain> { [GraftFirst] = first, [GraftSecond] = second });
        return (grain, state, first, second);
    }

    /// <summary>The appended leaves: <c>[m, r)</c> and <c>[r, open)</c>.</summary>
    private static List<GraftEntry> GraftTargets() =>
    [
        new GraftEntry { SeparatorKey = "m", LeafId = GraftFirst },
        new GraftEntry { SeparatorKey = "r", LeafId = GraftSecond },
    ];

    [Test]
    public async Task Graft_narrow_moves_shadow_markers_to_the_appended_leaf_each_row_relocates_to()
    {
        var (grain, _, first, second) = CreateGraftDonor();
        foreach (var key in new[] { "a", "m", "q", "z" })
            await grain.SetAsync(key, Encoding.UTF8.GetBytes(key));

        // A saga claims a key the old leaf keeps and three that relocate, two to the
        // first appended leaf and one to the second.
        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "a", "m", "q", "z" });

        var result = await grain.NarrowHighKeyAsync("m", GraftFirst, GraftTargets());

        Assert.That(result?.Rows.Select(r => r.Key), Is.EqualTo(new[] { "m", "q", "z" }), "precondition");
        var onFirst = ShadowMarksOn(first);
        var onSecond = ShadowMarksOn(second);
        Assert.Multiple(() =>
        {
            Assert.That(onFirst, Has.Count.EqualTo(1));
            Assert.That(onFirst.Single().Txid, Is.EqualTo(txid));
            Assert.That(onFirst.Single().Keys, Is.EquivalentTo(new[] { "m", "q" }), "the first appended leaf gates the rows it receives");
            Assert.That(onSecond.Single().Keys, Is.EquivalentTo(new[] { "z" }), "the second appended leaf gates the row it receives");
        });
    }

    [Test]
    public async Task Graft_narrow_moves_locally_prepared_saga_buckets_for_relocated_keys()
    {
        var (grain, _, _, second) = CreateGraftDonor();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        // A same-shard prepare has no explicit shadow marker of its own: its isolation
        // is the pending bucket, which the relocation leaves behind on the old leaf.
        var txid = Guid.NewGuid();
        await PreparedSetForSplitAsync(grain, txid, "z", Encoding.UTF8.GetBytes("prepared"));

        await grain.NarrowHighKeyAsync("m", GraftFirst, GraftTargets());

        var marks = ShadowMarksOn(second);
        Assert.That(marks, Has.Count.EqualTo(1));
        Assert.That(marks[0].Txid, Is.EqualTo(txid));
        Assert.That(marks[0].Keys, Is.EquivalentTo(new[] { "z" }));
    }

    [Test]
    public async Task Graft_narrow_marks_no_appended_leaf_when_no_saga_state_exists()
    {
        var (grain, _, first, second) = CreateGraftDonor();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));

        await grain.NarrowHighKeyAsync("m", GraftFirst, GraftTargets());

        Assert.Multiple(() =>
        {
            Assert.That(ShadowMarksOn(first), Is.Empty);
            Assert.That(ShadowMarksOn(second), Is.Empty);
        });
    }

    [Test]
    public async Task Graft_narrow_marks_no_appended_leaf_when_no_claimed_key_relocates()
    {
        var (grain, _, first, second) = CreateGraftDonor();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        await grain.MarkSagaShadowAsync(Guid.NewGuid(), new[] { "a" });

        await grain.NarrowHighKeyAsync("m", GraftFirst, GraftTargets());

        Assert.Multiple(() =>
        {
            Assert.That(ShadowMarksOn(first), Is.Empty);
            Assert.That(ShadowMarksOn(second), Is.Empty);
        });
    }

    [Test]
    public async Task NarrowHighKey_reports_the_replay_frontier_it_had_when_it_narrowed()
    {
        // Issue #5752: the appended leaves may start reading no higher than this, so a
        // record of their range the old leaf never applied is replayed by them.
        var (grain, state, _, _) = CreateGraftDonor();
        state.State.ProjectionCheckpointOffset = 30;
        state.State.ProjectionCheckpointOffsetAssigned = true;
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("1"));

        var result = await grain.NarrowHighKeyAsync("m", GraftFirst, GraftTargets());

        Assert.That(result?.ReadFrontier, Is.Not.Null.And.Not.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(result!.ReadFrontier![0], Is.EqualTo(30), "the checkpoint, inclusive, as the leaf replays above it");
            Assert.That(result.ReadFrontier.Skip(1), Is.All.EqualTo(-1), "a partition never replayed reports none");
        });
    }

    [Test]
    public async Task NarrowHighKey_reports_its_own_replay_frontier_when_a_duplicate_activation_has_appended_past_it()
    {
        // The reviewer's trigger for #5752: a duplicate activation of the old leaf
        // appends a record of its range after this activation last replayed, and this
        // activation never applies it. The frontier it reports is its own, so the
        // appended leaves start below that record rather than at the WAL head.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = 3;
        state.State.ProjectionCheckpointOffsetAssigned = true;
        var commitLog = new FakeCommitLogWriter();
        var stubs = new Dictionary<GrainId, IBPlusLeafGrain>
        {
            [GraftFirst] = Substitute.For<IBPlusLeafGrain>(),
            [GraftSecond] = Substitute.For<IBPlusLeafGrain>(),
        };
        var original = CreateGrain(state, commitLog: commitLog, leafStubs: stubs);
        var duplicate = CreateGrain(state, commitLog: commitLog, leafStubs: stubs);
        for (var i = 0; i < 8; i++)
            await duplicate.SetAsync($"z{i}", Encoding.UTF8.GetBytes("dup"));
        var dupOffset = commitLog.Appended.Count - 1;

        var result = await original.NarrowHighKeyAsync("m", GraftFirst, GraftTargets());

        Assert.That(dupOffset, Is.GreaterThan(3), "precondition: the duplicate appended past the original's frontier");
        Assert.That(result?.ReadFrontier?[0], Is.EqualTo(3),
            "the original reports how far it replayed, below the duplicate's records, not how far the log has grown");
    }

    [Test]
    public async Task CopyRowsForBulkLoad_hands_back_every_row_and_the_frontier_read_before_them()
    {
        // Issue #5766: the bulk load caps its loaded leaves at this frontier.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = 30;
        state.State.ProjectionCheckpointOffsetAssigned = true;
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("z");

        var copied = await grain.CopyRowsForBulkLoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(copied.Rows.Select(r => r.Key), Is.EqualTo(new[] { "a", "z" }), "every row, tombstones included");
            Assert.That(copied.Rows.Single(r => r.Key == "z").IsTombstone, Is.True);
            Assert.That(copied.ReadFrontier![0], Is.EqualTo(30), "the in-memory checkpoint");
            Assert.That(copied.ReadFrontier.Skip(1), Is.All.EqualTo(-1), "a partition never replayed reports none");
        });
        Assert.That(await grain.GetAsync("a"), Is.Not.Null, "the copy changes nothing");
    }
}
