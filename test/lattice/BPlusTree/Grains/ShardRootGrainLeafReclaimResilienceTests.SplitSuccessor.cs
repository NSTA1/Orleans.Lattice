using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2160, the arm that survived its first fix: a reclaim pass that takes
/// the retirement latch on a leaf a split is about to merge rows into.
/// <para>
/// The declination inside <c>TryUnlinkSuccessorAsync</c> already refuses to
/// unlink an in-flight split's target, and it is correct. But the shard root
/// latches the victim BEFORE it asks the predecessor anything:
/// <c>TryBeginRetirementAsync</c>, then <c>TryUnlinkSuccessorAsync</c>, then
/// <c>AbandonRetirementAsync</c> on the declined arm. A freshly seeded split
/// sibling passes that latch every time - zero rows, <c>SplitState.Unsplit</c>,
/// no seal - so the guard saves the POINTER and loses the SPLIT.
/// </para>
/// <para>
/// What the latch costs is not hypothetical. <c>MergeEntriesAsync</c> calls
/// <c>EnterMutationScope()</c> as its second statement, which throws
/// <c>LeafRetiredException</c> while the latch is set. The shard root's
/// retirement backoff covers the write-dispatch path only, not the split, and
/// there is no <c>try</c>/<c>catch</c> at <c>CompleteSplitAsync</c>'s merge
/// call site - so the exception escapes and the tail of the division never
/// runs: no straggler sweep, no <c>HighKeyExclusive = splitKey</c>, no
/// <c>SplitInFlight = false</c>, and - because the separator is published by
/// the caller after <c>CompleteSplitAsync</c> returns - no parent separator
/// either. The sibling is left spliced into the chain, holding whatever
/// batches already merged, and unreachable by descent. That is an orphaned
/// leaf, by the mechanism this file's production counterpart documents.
/// </para>
/// <para>
/// The window is not a knife edge. The leaf mutation surface is
/// <c>[AlwaysInterleave]</c>, so the donor answers probes while its own
/// division is suspended at an await; the new sibling is unrouted, so the
/// fold's descent check reads it as an interrupted fold to finish rather than
/// declining; and <c>TryUnlinkSuccessorAsync</c> awaits <c>_splitGate</c>,
/// which the division holds for its whole duration - so the declining call
/// blocks behind the very merge it is about to decline, holding the latch open
/// across it.
/// </para>
/// <para>
/// So the assertion that matters is not "the fold declined". It is "the victim
/// was never latched". These tests assert the latch directly.
/// </para>
/// </summary>
public sealed partial class ShardRootGrainLeafReclaimResilienceTests
{
    /// <summary>
    /// Stages the interleaving: A is mid-division into B, which is exactly the
    /// state a probe of A reports between the sibling splice and the row merge.
    /// B is left as the harness builds it - empty, unsealed, no saga state -
    /// because that is the whole difficulty: nothing on B says "do not touch
    /// me", and nothing can.
    /// </summary>
    private static ReclaimHarness SplittingIntoSuccessorHarness()
    {
        var h = CreateHarness();
        h.SetProbe(h.LeafA, h.Probes[h.LeafA] with { SplitTargetSiblingId = h.LeafB });
        return h;
    }

    [Test]
    public async Task A_leaf_an_in_flight_split_is_about_to_merge_into_is_never_latched()
    {
        // THE discriminator for #2160's surviving arm. Declining the unlink is
        // not enough and never was: by the time the predecessor is asked, the
        // latch is already set on the one leaf the division is about to write
        // to, and the division dies on it.
        var h = SplittingIntoSuccessorHarness();

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.B.DidNotReceive().TryBeginRetirementAsync();

        Assert.That(reclaimed, Is.Zero,
            "a leaf an in-flight division is about to merge rows into is not a fold candidate");
    }

    [Test]
    public async Task The_declination_happens_before_anything_is_latched_or_asked()
    {
        // The decision has to be taken on evidence the walk ALREADY holds -
        // the predecessor's own probe - rather than by asking the predecessor
        // again after the fact. Asking is what the unlink does, and the unlink
        // is one grain call too late.
        var h = SplittingIntoSuccessorHarness();

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.Multiple(() =>
        {
            Assert.That(h.ChildIds, Does.Contain(h.LeafB),
                "the routing entry must be untouched");
            Assert.That(h.Probes[h.LeafA].NextSibling, Is.EqualTo(h.LeafB),
                "the chain must still run through the split's target");
            Assert.That(h.Probes[h.LeafA].HighKeyExclusive, Is.EqualTo("b"),
                "no widen may leak through: a predecessor claiming a range it does not route to "
                + "loses every write into it on the next projection rebuild");
        });

        await h.A.DidNotReceive().TryUnlinkSuccessorAsync(
            Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>());

        // Nothing was latched, so there is nothing to unlatch - and an
        // AbandonRetirementAsync here would be evidence that the latch WAS
        // taken and merely released, which is the defect, not the fix.
        await h.B.DidNotReceive().AbandonRetirementAsync();
    }

    [Test]
    public async Task A_split_that_targets_a_different_sibling_still_lets_the_fold_proceed()
    {
        // The guard keys on IDENTITY, not on the mere presence of a division.
        // Declining whenever any split is in flight anywhere would make reclaim
        // inert on precisely the busy trees it exists to tidy - and every other
        // test in this file would stay green while it happened.
        var h = CreateHarness();
        h.SetProbe(h.LeafA, h.Probes[h.LeafA] with { SplitTargetSiblingId = h.LeafC });

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1),
            "A is dividing into C, not into the leaf being folded, so the fold is safe and must proceed");
        await h.B.Received(1).TryBeginRetirementAsync();
    }

    [Test]
    public async Task A_completed_split_does_not_suppress_reclaim_forever()
    {
        // SplitSiblingId is NOT cleared when a division completes, so a probe
        // that published it ungated would refuse to fold B for the rest of A's
        // life. The probe publishes null once the division is no longer in
        // flight, and this is the test that keeps it that way: it is the exact
        // shape of issue #3265's ratchet defect, which was a dead equality test
        // on a monotone lattice doing the same damage from the other side.
        var h = CreateHarness();
        h.SetProbe(h.LeafA, h.Probes[h.LeafA] with { SplitTargetSiblingId = null });

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1),
            "no division is in flight, so the ordinary fold must still happen");
    }
}
