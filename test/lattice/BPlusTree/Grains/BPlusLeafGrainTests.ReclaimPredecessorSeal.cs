using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the third declination in
/// <c>BPlusLeafGrain.TryUnlinkSuccessorAsync</c>: a leaf carrying a moved-away
/// seal must refuse to widen itself over a successor's vacated range.
/// <para>
/// This is issue #2143, and it is an asymmetry rather than a race. The two
/// declinations above it, and <c>HasReclaimBlockingState</c> itself, all ask
/// whether the fold's VICTIM is safe to remove. The fold's whole purpose is to
/// make the fold's PREDECESSOR the legitimate owner of the range the victim
/// gives up, and until this guard nothing put the equivalent question to it.
/// </para>
/// <para>
/// The end state a missing guard produces is invisible to every structural
/// invariant the tree has: the leaf chain tiles perfectly, there are no routing
/// orphans, a routing descent lands on a leaf that correctly declares the key's
/// span, a cache probe reports the row present - and the read still returns
/// null, because <c>GetAsync</c> opens with
/// <c>if (IsKeyMovedAway(key)) return null</c>. That half is already proved
/// executably by <c>Get_returns_null_for_moved_away_slot_key</c> and its
/// siblings in <c>BPlusLeafGrainTests.MovedAwaySlots</c>, so it is cited here
/// rather than restated - a second test failing for the same reason would imply
/// the read gate is something this change introduced. What is new, and what
/// these tests cover, is that the fold can now no longer WALK a leaf into that
/// state.
/// </para>
/// <para>
/// The seal is hash-keyed, not range-keyed. <c>IsKeyMovedAway</c> resolves a
/// key to a virtual slot through <c>ShardMap.GetVirtualSlot</c> and tests
/// membership, so a sealed slot is a residue class scattered across the whole
/// keyspace rather than a contiguous span. That is why the guard tests the seal
/// whole instead of intersecting it with the absorbed range, and it is why
/// these tests derive their sealed slot from the key through the real hash
/// rather than picking a literal that could silently stop meaning anything.
/// </para>
/// <para>
/// These are unit tests on purpose, for the same reason
/// <c>BPlusLeafGrainTests.ReclaimUnderSplit</c> is. The configuration is latent
/// through the public surface - a leaf's <c>MovedAwaySlots</c> is written only
/// by <c>MarkSlotsMovedAwayAsync</c>, reached only through a shard split or
/// consolidation, and reclaim does not currently fold a sealed predecessor in
/// that configuration - so the state is constructed directly and the
/// declination is asserted at the seam that makes it, which is the honest scope
/// for it.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const int SealVirtualShardCount = 64;

    /// <summary>A key inside the range the fold would hand to the predecessor.</summary>
    private const string SealAbsorbedKey = "s-absorbed-key";

    private static readonly GrainId SealVictim = GrainId.Create("leaf", "seal-victim");
    private static readonly GrainId SealBeyondVictim = GrainId.Create("leaf", "seal-beyond");

    /// <summary>
    /// A predecessor as a reclaim pass finds it: it declares <c>[a, m)</c>,
    /// points at the victim that declares <c>[m, z)</c>, and the fold wants to
    /// widen it to <c>[a, z)</c>.
    /// </summary>
    private static FakePersistentState<LeafNodeState> SealPredecessorState()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "reclaim-predecessor-seal";
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "m";
        state.State.NextSibling = SealVictim;
        return state;
    }

    /// <summary>
    /// The slot <paramref name="key"/> hashes into, derived through the real
    /// <see cref="ShardMap.GetVirtualSlot"/> rather than hard-coded. A literal
    /// would still compile and still pass if the hash or the slot arithmetic
    /// changed, while no longer sealing the key under test - so the test would
    /// go quietly vacuous instead of red.
    /// </summary>
    private static int[] SealSlotsFor(string key) =>
        [ShardMap.GetVirtualSlot(key, SealVirtualShardCount)];

    // --- The discriminator ---

    [Test]
    public async Task TryUnlinkSuccessor_declines_when_the_predecessor_carries_a_moved_away_seal()
    {
        var state = SealPredecessorState();
        var grain = CreateGrain(state);

        await grain.MarkSlotsMovedAwayAsync(
            SealSlotsFor(SealAbsorbedKey),
            SealVirtualShardCount);

        // Exactly what ShardRootGrain.TryReclaimLeafAsync asks of the
        // predecessor once it has latched an empty victim: unlink the victim
        // and widen onto the range it gives up.
        var unlinked = await grain.TryUnlinkSuccessorAsync(
            SealVictim,
            SealBeyondVictim,
            "z");

        Assert.That(unlinked, Is.False,
            "A leaf carrying a moved-away seal must refuse to widen over a successor's "
            + "range. Neither declination above this one can catch it: the successor pointer "
            + "matches the reclaim's plan exactly, and no split is in flight. The evidence is "
            + "not on the victim at all - the victim is empty, unsealed, and a perfectly "
            + "legitimate reclaim candidate - it is on the leaf that is about to absorb its "
            + "range, which nothing was asking. See issue #2143.");
    }

    [Test]
    public async Task A_declined_unlink_under_a_predecessor_seal_leaves_every_pointer_and_bound_untouched()
    {
        var state = SealPredecessorState();
        var grain = CreateGrain(state);

        await grain.MarkSlotsMovedAwayAsync(
            SealSlotsFor(SealAbsorbedKey),
            SealVirtualShardCount);

        // Counted AFTER the seal, so the seal's own persist is not mistaken
        // for a write by the declination.
        var writesBefore = state.WriteCount;

        await grain.TryUnlinkSuccessorAsync(SealVictim, SealBeyondVictim, "z");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextSibling, Is.EqualTo(SealVictim),
                "The successor pointer must still name the victim; repointing past it is the "
                + "unlink this guard exists to refuse.");
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"),
                "The bound must not move. Widening it is what makes this leaf the owner of "
                + "keys its own seal will refuse to serve.");
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
                "A declination must not persist at all. A write here would be a durable "
                + "no-op at best and half a fold at worst.");
        });
    }

    // --- The guard: the declination must stay narrow ---

    [Test]
    public async Task TryUnlinkSuccessor_still_unlinks_when_the_predecessor_carries_no_seal()
    {
        var state = SealPredecessorState();
        var grain = CreateGrain(state);

        var unlinked = await grain.TryUnlinkSuccessorAsync(
            SealVictim,
            SealBeyondVictim,
            "z");

        Assert.Multiple(() =>
        {
            Assert.That(unlinked, Is.True,
                "The new declination must be narrow. Empty-leaf reclaim is the whole point of "
                + "this seam, so a guard that quietly refused every fold would leave the "
                + "feature inert while every other reclaim test still passed - the fold would "
                + "simply never happen and the chain would never shorten.");
            Assert.That(state.State.NextSibling, Is.EqualTo(SealBeyondVictim),
                "An accepted unlink must actually repoint the successor.");
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"),
                "and must actually widen onto the range the victim gave up.");
        });
    }

    [Test]
    public async Task A_lifted_seal_folds_again_because_the_guard_tests_the_slots_and_not_the_stamp()
    {
        // The liveness half of the fix, and the reason it needs a test of its
        // own rather than a comment.
        //
        // UnmarkSlotsMovedAwayAsync clears MovedAwaySlots when consolidation
        // drains the slots back, but DELIBERATELY retains
        // MovedAwayVirtualShardCount forever - the retained stamp is the wire
        // signal that lets a LeafCacheGrain tell "never sealed" from "seal just
        // lifted". So there is a permanent field sitting immediately beside the
        // one this guard tests.
        //
        // Keying the guard on that stamp reads at least as naturally and would
        // be permanent: every leaf that had ever sealed anything would refuse
        // to absorb a successor for the rest of its life, on precisely the
        // post-shard-split trees where this path is reachable at all, and
        // silently but for a debug log. This test is what fails if anyone makes
        // that swap - it is the only thing standing between a latent
        // correctness bug and a live liveness bug traded for it.
        var state = SealPredecessorState();
        var grain = CreateGrain(state);

        var slots = SealSlotsFor(SealAbsorbedKey);
        await grain.MarkSlotsMovedAwayAsync(slots, SealVirtualShardCount);

        var declinedWhileSealed = await grain.TryUnlinkSuccessorAsync(
            SealVictim,
            SealBeyondVictim,
            "z");
        Assert.That(declinedWhileSealed, Is.False,
            "precondition: while the seal is up the fold is declined");

        // The real lift, through the real consolidation entrypoint, rather than
        // by poking the field - so the test exercises the pairing that actually
        // occurs in production.
        await grain.UnmarkSlotsMovedAwayAsync(slots, SealVirtualShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.MovedAwaySlots, Is.Null,
                "precondition: lifting the last sealed slot empties the slot set");
            Assert.That(state.State.MovedAwayVirtualShardCount, Is.EqualTo(SealVirtualShardCount),
                "precondition: and the slot-space stamp is deliberately RETAINED, which is "
                + "exactly the trap - a guard keyed on this field would never lift");
        });

        var unlinked = await grain.TryUnlinkSuccessorAsync(
            SealVictim,
            SealBeyondVictim,
            "z");

        Assert.Multiple(() =>
        {
            Assert.That(unlinked, Is.True,
                "The declination must be TEMPORARY. Once consolidation has drained the slots "
                + "back this leaf is authoritative for them again, the hazard is gone, and the "
                + "leaf must fold once more with no operator action. A guard that outlived the "
                + "seal would trade a correctness bug that cannot fire today for a permanent, "
                + "silent liveness bug on every post-split tree.");
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"),
                "and the widen it was declining must actually happen once the seal is gone.");
        });
    }
}
