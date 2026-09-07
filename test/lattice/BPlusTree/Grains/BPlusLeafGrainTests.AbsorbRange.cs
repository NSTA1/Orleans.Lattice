using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the leaf-side range-absorption seam of empty-leaf chain
/// reclaim: <c>AbsorbSuccessorRangeAsync</c>, the split-boundary clear that
/// rides along with a widen, and the two blocking-state conditions a row
/// count cannot detect.
/// <para>
/// <c>AbsorbSuccessorRangeAsync</c> is the repair half of reclaim - the shard
/// root calls it when it finds two chain-adjacent leaves that have stopped
/// tiling the keyspace - and it is reached only by that repair, so no
/// end-to-end fold exercises it. Its contract is that the widen is
/// <em>monotonic</em>: widen only, never narrow. That is what makes it
/// idempotent, and idempotence is what lets a pass re-driven after a crash
/// converge instead of walking a leaf's bound backwards onto a range it has
/// since been given.
/// </para>
/// <para>
/// The revert arms matter for the same reason in the other direction. The WAL
/// materialiser filters by exactly this bound, so an activation left holding a
/// widened bound in memory that storage never accepted would claim - and
/// replay-filter for - a range no peer routes to it.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId AbsorbNextSibling = GrainId.Create("leaf", "absorb-next");
    private static readonly GrainId AbsorbBeyondSibling = GrainId.Create("leaf", "absorb-beyond");

    /// <summary>A plain bounded leaf <c>["a", "m")</c> with a successor.</summary>
    private static FakePersistentState<LeafNodeState> AbsorbLeafState()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "absorb-range";
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "m";
        state.State.NextSibling = AbsorbNextSibling;
        return state;
    }

    // --- AbsorbSuccessorRangeAsync: the monotonic widen ---

    [Test]
    public async Task AbsorbSuccessorRange_widens_the_high_bound_onto_the_vacated_range()
    {
        var state = AbsorbLeafState();
        var grain = CreateGrain(state);
        var writesBefore = state.WriteCount;

        await grain.AbsorbSuccessorRangeAsync("z");

        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"));
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore + 1),
            "the widen has to reach storage, because the WAL materialiser filters by this bound");
    }

    [Test]
    public async Task AbsorbSuccessorRange_widens_to_unbounded_when_the_successor_was_the_tail()
    {
        // A null argument means the absorbed successor was the chain tail, so
        // this leaf becomes unbounded to the right.
        var state = AbsorbLeafState();
        var grain = CreateGrain(state);

        await grain.AbsorbSuccessorRangeAsync(null);

        Assert.That(state.State.HighKeyExclusive, Is.Null);
    }

    [Test]
    public async Task AbsorbSuccessorRange_is_a_no_op_on_an_already_unbounded_leaf()
    {
        // A null high bound already means "unbounded to the right", so this
        // leaf covers whatever the successor covered and there is nothing to
        // widen - and nothing to persist.
        var state = AbsorbLeafState();
        state.State.HighKeyExclusive = null;
        var grain = CreateGrain(state);
        var writesBefore = state.WriteCount;

        await grain.AbsorbSuccessorRangeAsync("z");

        Assert.That(state.State.HighKeyExclusive, Is.Null,
            "an unbounded leaf must not be narrowed onto a finite bound");
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task AbsorbSuccessorRange_never_narrows_the_high_bound()
    {
        // The monotonicity guarantee, and the reason a re-driven pass
        // converges: a bound at or below the current one is refused outright.
        var state = AbsorbLeafState();
        var grain = CreateGrain(state);
        var writesBefore = state.WriteCount;

        await grain.AbsorbSuccessorRangeAsync("c");
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"), "a narrower bound is refused");

        await grain.AbsorbSuccessorRangeAsync("m");
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"), "an equal bound is a no-op");

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
            "neither refusal may cost a write");
    }

    [Test]
    public void AbsorbSuccessorRange_reverts_the_widen_when_the_persist_fails()
    {
        // Class B revert. Leaving the widened bound in memory while storage
        // still holds the narrow one would have this activation claim
        // ownership of - and replay-filter for - a range no peer routes to it.
        var state = AbsorbLeafState();
        var grain = CreateGrain(state);
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await grain.AbsorbSuccessorRangeAsync("z"),
            Throws.InstanceOf<InvalidOperationException>(),
            "a widen that did not reach storage must not be reported as applied");

        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"),
            "the in-memory bound must match what storage actually holds");
    }

    [Test]
    public async Task AbsorbSuccessorRange_that_fails_can_be_retried()
    {
        // The point of the revert: the leaf is left in a state a later pass
        // can re-drive, not wedged half-widened.
        var state = AbsorbLeafState();
        var grain = CreateGrain(state);
        state.ThrowOnWrite = new InvalidOperationException("storage briefly unavailable");

        Assert.That(async () => await grain.AbsorbSuccessorRangeAsync("z"),
            Throws.InstanceOf<InvalidOperationException>());

        // ThrowOnWrite self-clears after firing once, so this is the retry.
        await grain.AbsorbSuccessorRangeAsync("z");

        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"));
    }

    // --- The split boundary a widen absorbs ---

    [Test]
    public async Task AbsorbSuccessorRange_clears_a_split_boundary_the_widen_absorbed()
    {
        // A completed split left a boundary at "m" that forwards keys above it
        // to a sibling. Once this leaf owns up to "z" the boundary is stale,
        // and it must be dropped in the SAME persist as the widen so no
        // observer can see one without the other.
        var state = AbsorbLeafState();
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = AbsorbNextSibling;
        var grain = CreateGrain(state);

        await grain.AbsorbSuccessorRangeAsync("z");

        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"));
        Assert.That(state.State.SplitKey, Is.Null, "the absorbed boundary is stale and must go");
        Assert.That(state.State.SplitSiblingId, Is.Null,
            "the sibling id qualifies the key it belongs to and goes with it");
        Assert.That(state.State.SplitState, Is.EqualTo(SplitState.SplitComplete),
            "SplitState is a monotone lattice and is deliberately never driven backwards");
    }

    [Test]
    public async Task AbsorbSuccessorRange_keeps_a_split_boundary_the_widen_did_not_reach()
    {
        // A widen that stops at or below the boundary has not absorbed it: the
        // successor still owns the keys above it, so forwarding must continue.
        var state = AbsorbLeafState();
        state.State.HighKeyExclusive = "c";
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = AbsorbNextSibling;
        var grain = CreateGrain(state);

        await grain.AbsorbSuccessorRangeAsync("m");

        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
        Assert.That(state.State.SplitKey, Is.EqualTo("m"),
            "a boundary the widen only reached, never passed, is still live");
        Assert.That(state.State.SplitSiblingId, Is.EqualTo(AbsorbNextSibling));
    }

    [Test]
    public async Task AbsorbSuccessorRange_keeps_the_boundary_of_an_in_flight_split()
    {
        // While a split really is in flight there is no stale boundary to
        // clear - the keys above it genuinely belong elsewhere - and nulling
        // the key would break the "SplitInProgress implies SplitKey is not
        // null" invariant that the forwarding checks dereference. Since
        // CompareOrdinal(key, null) >= 0 for every non-null key, every write
        // would then be forwarded to the sibling.
        var state = AbsorbLeafState();
        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = AbsorbNextSibling;
        var grain = CreateGrain(state);

        await grain.AbsorbSuccessorRangeAsync("z");

        Assert.That(state.State.SplitKey, Is.EqualTo("m"),
            "an in-flight split's boundary must survive the widen");
        Assert.That(state.State.SplitSiblingId, Is.EqualTo(AbsorbNextSibling));
    }

    [Test]
    public void AbsorbSuccessorRange_reverts_the_cleared_boundary_with_the_bound()
    {
        // The boundary clear is part of the same decision as the widen, so a
        // failed persist must revert both or an observer could see one without
        // the other.
        var state = AbsorbLeafState();
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = AbsorbNextSibling;
        var grain = CreateGrain(state);
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await grain.AbsorbSuccessorRangeAsync("z"),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
        Assert.That(state.State.SplitKey, Is.EqualTo("m"),
            "the boundary reverts with the bound it was cleared alongside");
        Assert.That(state.State.SplitSiblingId, Is.EqualTo(AbsorbNextSibling));
    }

    // --- The same revert on the unlink path ---

    [Test]
    public void TryUnlinkSuccessor_reverts_the_unlink_and_the_widen_when_the_persist_fails()
    {
        // The unlink and the widen are deliberately one write. A failure has to
        // revert the whole decision: an activation that believes it has
        // absorbed a range storage says it has not would route and
        // replay-filter against a topology no peer shares.
        var state = AbsorbLeafState();
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = AbsorbNextSibling;
        var grain = CreateGrain(state);
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await grain.TryUnlinkSuccessorAsync(
                AbsorbNextSibling, AbsorbBeyondSibling, "z"),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(state.State.NextSibling, Is.EqualTo(AbsorbNextSibling),
            "the chain pointer reverts, so the folded leaf is still reachable");
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
        Assert.That(state.State.SplitKey, Is.EqualTo("m"));
        Assert.That(state.State.SplitSiblingId, Is.EqualTo(AbsorbNextSibling));
    }

    [Test]
    public async Task TryUnlinkSuccessor_applies_the_unlink_and_the_widen_together()
    {
        // Falsifies the revert test above: with no fault the same call really
        // does apply both halves in one write.
        var state = AbsorbLeafState();
        var grain = CreateGrain(state);
        var writesBefore = state.WriteCount;

        var unlinked = await grain.TryUnlinkSuccessorAsync(
            AbsorbNextSibling, AbsorbBeyondSibling, "z");

        Assert.That(unlinked, Is.True);
        Assert.That(state.State.NextSibling, Is.EqualTo(AbsorbBeyondSibling));
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("z"));
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore + 1),
            "one write, so there is no window in which routing and the declared span disagree");
    }

    // --- Blocking state a row count cannot see ---

    [Test]
    public async Task GetReclaimProbe_reports_an_in_flight_split_as_blocking()
    {
        // A split that has persisted its intent but not completed owns rows
        // mid-flight between this leaf and a sibling that may not exist yet.
        // The row count legitimately reads zero in that window, which is
        // exactly why the count alone cannot decide.
        var state = AbsorbLeafState();
        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = AbsorbNextSibling;
        var grain = CreateGrain(state);

        var probe = await grain.GetReclaimProbeAsync();

        Assert.That(probe.LiveRowCount, Is.Zero, "the row count cannot see the hazard");
        Assert.That(probe.HasBlockingState, Is.True, "but the probe must still refuse the leaf");
    }

    [Test]
    public async Task GetReclaimProbe_reports_a_moved_away_seal_as_blocking()
    {
        // The moved-away seal is deliberately sticky: it stops a donor
        // resurfacing an orphan snapshot for a slot that has migrated to
        // another shard. Deleting the leaf would delete the seal, and the seal
        // outliving the rows is the entire point of it.
        var state = AbsorbLeafState();
        state.State.MovedAwaySlots = [3];
        var grain = CreateGrain(state);

        var probe = await grain.GetReclaimProbeAsync();

        Assert.That(probe.LiveRowCount, Is.Zero);
        Assert.That(probe.HasBlockingState, Is.True);
    }

    [Test]
    public async Task GetReclaimProbe_reports_a_plain_empty_leaf_as_reclaimable()
    {
        // Falsifies both tests above: without either condition the same empty
        // leaf is reported as free to fold, so a True there is evidence about
        // the condition and not about the probe always refusing.
        var state = AbsorbLeafState();
        state.State.PrevSibling = GrainId.Create("leaf", "absorb-prev");
        var grain = CreateGrain(state);

        var probe = await grain.GetReclaimProbeAsync();

        Assert.That(probe.LiveRowCount, Is.Zero);
        Assert.That(probe.HasBlockingState, Is.False);
        Assert.That(probe.HighKeyExclusive, Is.EqualTo("m"));
        Assert.That(probe.NextSibling, Is.EqualTo(AbsorbNextSibling));
    }
}
