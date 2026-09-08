using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Executable specification of what
/// <see cref="LeafNodeState.ProjectionCheckpointOffset"/> actually means
/// (issue #2270).
///
/// <para>
/// The offset means "every WAL entry at or below this offset has been
/// SCANNED by this leaf's activation-time replay", NOT "applied". Replay
/// advances it over entries it deliberately skips as belonging to another
/// leaf's key range, because the advance in
/// <c>ReplayPartitionAsync</c> sits outside the
/// <c>ShouldApplyDuringReplay</c> filter.
/// </para>
///
/// <para>
/// That distinction is load-bearing rather than cosmetic, which is why it
/// is pinned by tests rather than only described in prose:
/// <c>LatticeWalGc</c> takes the MINIMUM of these offsets across
/// leaves as the WAL retention floor. Scanned-through semantics are safe
/// there because skipping only ever inflates the checkpoint of a leaf that
/// does NOT own the entry, while the one leaf that does own it cannot skip
/// it and so holds the minimum below that offset until it truly applies.
/// Narrowing the advance to applied-only entries would NOT be a
/// tightening: a leaf owning no key in a partition would never advance at
/// all, would re-scan that partition on every activation forever, and
/// would pin the WAL retention floor for the whole tree.
/// </para>
///
/// <para>
/// These tests therefore exist to make the sentence and the behaviour
/// comparable by CI rather than by a reader. Coverage was not absent
/// before #2270 - <c>Materialiser_filters_out_set_outside_owned_key_range</c>
/// and <c>Materialiser_filters_out_set_with_mismatching_shard_index</c>
/// both fail if the advance is moved inside the filter - but it was
/// incidental: each is a single-entry test NAMED for the filter, with the
/// checkpoint as a trailing assertion, so a failure reads as "the filter
/// broke" rather than "the checkpoint contract changed". Neither varies
/// the applied and scanned maxima independently, so neither can tell
/// "advanced over a skip" apart from "advanced to the head regardless".
/// The tests below name the contract and separate those cases.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Seeds a leaf that owns the half-open key range ["m", "n"), so that
    /// any key ordering outside it is skipped by the replay filter while
    /// still being scanned by the replay loop.
    /// </summary>
    private static Action<LeafNodeState> OwnsOnlyTheMRange() => s =>
    {
        s.LowKeyInclusive = "m";
        s.HighKeyExclusive = "n";
    };

    [Test]
    public async Task ProjectionCheckpointOffset_advances_over_entries_the_leaf_skips()
    {
        // Every entry in the slice belongs to another leaf's key range, so
        // the projection stays empty - and the checkpoint still reaches the
        // head. This is the whole of the "scanned through, not applied
        // through" claim in one assertion pair.
        var coord = BuildCoordinator(
            head: 3,
            new CommitLogSliceEntry(1, BuildCommittedSet("a1", Encoding.UTF8.GetBytes("v1"))),
            new CommitLogSliceEntry(2, BuildCommittedSet("b1", Encoding.UTF8.GetBytes("v2"))),
            new CommitLogSliceEntry(3, BuildCommittedSet("c1", Encoding.UTF8.GetBytes("v3"))));

        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord, seedState: OwnsOnlyTheMRange());

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(
                grain.EntriesForTest, Is.Empty,
                "the leaf owns none of these keys, so replay must apply nothing");
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(3),
                "the checkpoint records what was SCANNED. A leaf that owns nothing in "
                + "a partition must still reach the head, or it re-scans from its stale "
                + "checkpoint on every activation and pins the WAL GC retention floor "
                + "(LatticeWalGc.ComputeMaterialiserOffsetFloorAsync) for the whole tree.");
        });
    }

    [Test]
    public async Task ProjectionCheckpointOffset_advances_past_the_last_scanned_entry_not_the_last_applied_one()
    {
        // The sharp discriminator between the two readings. Exactly one
        // entry is owned, and it is the FIRST of three:
        //   applied-through  would leave the checkpoint at 1
        //   scanned-through  leaves it at 3
        // so this test fails loudly if the advance is ever moved inside the
        // ShouldApplyDuringReplay filter.
        var coord = BuildCoordinator(
            head: 3,
            new CommitLogSliceEntry(1, BuildCommittedSet("m1", Encoding.UTF8.GetBytes("mine"))),
            new CommitLogSliceEntry(2, BuildCommittedSet("b1", Encoding.UTF8.GetBytes("theirs"))),
            new CommitLogSliceEntry(3, BuildCommittedSet("c1", Encoding.UTF8.GetBytes("theirs"))));

        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord, seedState: OwnsOnlyTheMRange());

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.ContainsKey("m1"), Is.True, "the owned key must apply");
            Assert.That(grain.EntriesForTest.ContainsKey("b1"), Is.False, "an unowned key must not apply");
            Assert.That(grain.EntriesForTest.ContainsKey("c1"), Is.False, "an unowned key must not apply");
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(3),
                "scanned-through: the checkpoint passes offsets 2 and 3 even though the "
                + "leaf applied neither. Under an applied-through reading this would be 1.");
        });
    }

    [Test]
    public async Task ProjectionCheckpointOffset_does_not_advance_beyond_the_scanned_head()
    {
        // The complementary bound, so the pin above cannot be satisfied by a
        // checkpoint that simply runs ahead. Scanning stops at the head the
        // coordinator reported, and the checkpoint stops with it, even though
        // the leaf skipped everything it saw.
        var coord = BuildCoordinator(
            head: 2,
            new CommitLogSliceEntry(1, BuildCommittedSet("a1", Encoding.UTF8.GetBytes("v1"))),
            new CommitLogSliceEntry(2, BuildCommittedSet("b1", Encoding.UTF8.GetBytes("v2"))),
            new CommitLogSliceEntry(3, BuildCommittedSet("c1", Encoding.UTF8.GetBytes("v3"))));

        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord, seedState: OwnsOnlyTheMRange());

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest, Is.Empty);
            Assert.That(
                state.State.ProjectionCheckpointOffset, Is.EqualTo(2),
                "the checkpoint is bounded by the WAL head that replay actually read, "
                + "so 'scanned through' cannot be read as 'assume everything ahead'");
        });
    }
}
