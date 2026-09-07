using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the second declination in
/// <c>BPlusLeafGrain.TryUnlinkSuccessorAsync</c>: a leaf must refuse to unlink
/// the sibling its own in-flight split is about to move rows into.
/// <para>
/// This is issue #2160, and it is the same hazard as the compare-and-swap
/// above it arriving in the opposite order. <c>SplitAsync</c> persists
/// <c>SplitState</c>, <c>SplitKey</c>, <c>SplitSiblingId</c> and
/// <c>NextSibling</c> in one atomic block, so the new sibling becomes
/// chain-reachable the instant the intent lands. A reclaim walk that starts
/// after that instant therefore builds its plan naming the split sibling
/// ITSELF - so the compare-and-swap agrees (the plan's expected successor
/// genuinely IS the current successor) and, without this second check, the
/// fold proceeds.
/// </para>
/// <para>
/// The sibling cannot defend itself. <c>CompleteSplitAsync</c> seeds its key
/// range before awaiting <c>MergeEntriesAsync</c>, so in that window it is a
/// fresh grain with a declared range, zero rows, <c>SplitState.Unsplit</c> and
/// no moved-away seal - its own reclaim probe is legitimately clean. The only
/// evidence that it must not be touched lives on the splitting leaf, which is
/// why the guard has to live here.
/// </para>
/// <para>
/// These are unit tests on purpose. <c>LeafReclaimSplitRaceIntegrationTests</c>
/// covers the ordering the compare-and-swap catches, because that one can be
/// produced by driving real writes. This ordering cannot: reaching it through
/// the public surface means interleaving a reclaim pass inside the seed/merge
/// window of <c>CompleteSplitAsync</c>, and no instrument exists at that
/// boundary. So the state configuration is constructed directly and the
/// declination is asserted at the seam that makes it, which is the honest
/// scope for it.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId ReclaimSplitSibling = GrainId.Create("leaf", "split-sibling");
    private static readonly GrainId ReclaimPreSplitNext = GrainId.Create("leaf", "pre-split-next");
    private static readonly GrainId ReclaimBeyondSibling = GrainId.Create("leaf", "beyond");

    /// <summary>
    /// A leaf mid-split: the intent has persisted, so the new sibling is
    /// already the successor and is already chain-reachable, but the rows have
    /// not been merged into it yet.
    /// </summary>
    private static FakePersistentState<LeafNodeState> MidSplitLeafState()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "reclaim-under-split";
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "z";
        state.State.NextSibling = ReclaimSplitSibling;
        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = ReclaimSplitSibling;
        return state;
    }

    // --- The discriminator ---

    [Test]
    public async Task TryUnlinkSuccessor_declines_to_unlink_the_sibling_of_an_in_flight_split()
    {
        var state = MidSplitLeafState();
        var grain = CreateGrain(state);

        // Exactly what a reclaim pass that walked the chain AFTER the split
        // intent persisted would ask for: fold away the leaf it found as our
        // successor, which is the split sibling itself.
        var unlinked = await grain.TryUnlinkSuccessorAsync(
            ReclaimSplitSibling,
            ReclaimBeyondSibling,
            "z");

        Assert.That(unlinked, Is.False,
            "The leaf must refuse to unlink the sibling its own in-flight split is about "
            + "to merge rows into. The compare-and-swap above this check CANNOT catch this "
            + "ordering: the reclaim plan names the split sibling itself, so the expected "
            + "successor genuinely IS the current successor and the comparison agrees. "
            + "Accepting here unlinks a leaf that CompleteSplitAsync then merges rows into, "
            + "and because the retirement latch is a bare instance field with no persisted "
            + "counterpart, a deactivation in between clears it and the merge succeeds "
            + "silently onto an unreachable leaf. See issue #2160.");
    }

    [Test]
    public async Task A_declined_unlink_under_split_leaves_every_pointer_and_bound_untouched()
    {
        var state = MidSplitLeafState();
        var writesBefore = state.WriteCount;
        var grain = CreateGrain(state);

        await grain.TryUnlinkSuccessorAsync(ReclaimSplitSibling, ReclaimBeyondSibling, "z");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextSibling, Is.EqualTo(ReclaimSplitSibling),
                "The successor pointer must still name the split sibling; repointing past it "
                + "is the unlink this guard exists to refuse.");
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"),
                "The donor must not widen onto a range the split is still moving rows out of.");
            Assert.That(state.State.SplitKey, Is.EqualTo("m"),
                "The split boundary must survive: dropping it here would republish the donor "
                + "as owning keys the sibling is about to receive.");
            Assert.That(state.State.SplitSiblingId, Is.EqualTo(ReclaimSplitSibling),
                "The split must still know which sibling it is completing into.");
            Assert.That(state.State.SplitState, Is.EqualTo(SplitState.SplitInProgress),
                "Declining an unlink must not advance the split state machine.");
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
                "A declination must not persist at all. A write here would be a durable "
                + "no-op at best and a torn decision at worst.");
        });
    }

    // --- The guard: the declination must not disable ordinary reclaim ---

    [Test]
    public async Task TryUnlinkSuccessor_still_unlinks_when_no_split_is_in_flight()
    {
        var state = MidSplitLeafState();
        state.State.SplitState = SplitState.Unsplit;
        state.State.SplitKey = null;
        state.State.SplitSiblingId = null;
        state.State.NextSibling = ReclaimPreSplitNext;

        var grain = CreateGrain(state);

        var unlinked = await grain.TryUnlinkSuccessorAsync(
            ReclaimPreSplitNext,
            ReclaimBeyondSibling,
            "z");

        Assert.Multiple(() =>
        {
            Assert.That(unlinked, Is.True,
                "The new declination must be narrow. Empty-leaf reclaim is the entire point "
                + "of this epic, so a guard that quietly refused every fold would leave the "
                + "feature inert while every reclaim test still passed - the fold would "
                + "simply never happen and the chain would never shorten.");
            Assert.That(state.State.NextSibling, Is.EqualTo(ReclaimBeyondSibling),
                "An accepted unlink must actually repoint the successor.");
        });
    }

    [Test]
    public async Task TryUnlinkSuccessor_still_unlinks_while_a_split_targets_a_different_sibling()
    {
        // A split is in flight, but toward a DIFFERENT sibling than the one
        // the reclaim wants to fold. The guard keys on identity, not on the
        // mere presence of a split, so this must still be allowed - otherwise
        // any tree with a split anywhere in it would stop reclaiming.
        var state = MidSplitLeafState();
        state.State.NextSibling = ReclaimPreSplitNext;
        state.State.SplitSiblingId = ReclaimSplitSibling;

        var grain = CreateGrain(state);

        var unlinked = await grain.TryUnlinkSuccessorAsync(
            ReclaimPreSplitNext,
            ReclaimBeyondSibling,
            "z");

        Assert.That(unlinked, Is.True,
            "The guard must key on the sibling's IDENTITY, not on the presence of a split. "
            + "Declining whenever any split is in flight would suppress reclaim far beyond "
            + "the hazard and make the epic's feature effectively unreachable on a busy tree.");
    }

    // --- The silent arm, asserted at the seam that produces it ---

    [Test]
    public async Task The_retirement_latch_does_not_survive_reactivation_so_declining_is_the_only_defence()
    {
        // This is the arm that makes #2160 silent rather than loud, and it is
        // why the fix is a declination rather than making the latch durable.
        //
        // Had the unlink been accepted, the shard root would have latched the
        // split sibling via TryBeginRetirementAsync and CompleteSplitAsync
        // would then call MergeEntriesAsync on it. The hope is that the latch
        // makes that merge throw. It does not reliably: _reclaimRetired is a
        // bare instance field, so a deactivation between the latch and the
        // merge clears it, the merge is admitted, and the rows land on an
        // unlinked leaf with nothing thrown anywhere.
        //
        // Assert that property directly, so the reasoning above is pinned by a
        // test rather than left as a comment.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "reclaim-under-split";

        var mergeClock = HybridLogicalClock.Tick(default);
        var rows = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), mergeClock),
        };

        var retired = CreateGrain(state);
        Assert.That(await retired.TryBeginRetirementAsync(), Is.True,
            "precondition: an empty leaf with no blocking state must latch");

        Assert.ThrowsAsync<LeafRetiredException>(
            async () => await retired.MergeEntriesAsync(rows),
            "precondition: while the activation lives, the latch refuses the merge - this is "
            + "the LOUD arm, and it is the one the safety argument relies on");

        // Same durable state, new activation: exactly what a deactivation
        // between the latch and CompleteSplitAsync's merge produces, because
        // CompleteSplitAsync holds the sibling as a grain REFERENCE and the
        // call itself reactivates it.
        var reactivated = CreateGrain(state);

        Assert.DoesNotThrowAsync(
            async () => await reactivated.MergeEntriesAsync(rows),
            "The retirement latch does NOT survive reactivation, so it cannot be the defence "
            + "against #2160: the merge is admitted and the rows land silently on a leaf that "
            + "has already been unlinked from the chain. This is why the fix declines the "
            + "unlink up front rather than making _reclaimRetired durable - a durable latch "
            + "would only convert this silent arm into the loud arm, which is the other half "
            + "of #2160 (the stale split boundary), not a fix.");

        Assert.That(reactivated.EntriesForTest.ContainsKey("k"), Is.True,
            "and the row really is present on the reactivated leaf, so this is data landing "
            + "somewhere unreachable rather than a merge that quietly did nothing");
    }
}
