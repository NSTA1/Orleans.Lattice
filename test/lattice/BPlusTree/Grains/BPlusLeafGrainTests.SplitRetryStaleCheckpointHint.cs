using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3360: a split retry replays the WAL heads
/// captured at the original split, so by the time those heads are stamped as
/// projection-checkpoint hints the target may already have applied past them.
/// <para>
/// <c>ILeafProjection.SetCheckpointOffsetAsync</c> rejects a backward move by
/// throwing, so a stale hint that reaches it faults the enclosing batch. Both
/// hint-stamping call sites in <c>CompleteSplitAsync</c> - the sibling site
/// (via <see cref="IBPlusLeafGrain.SetCheckpointOffsetHintsAsync"/>) and the
/// donor site - must therefore drop a hint that no longer moves the target
/// forward. Issue 905 guarded the donor site inline and left the sibling site
/// 28 lines above it unguarded, which is the defect these tests pin.
/// </para>
/// <para>
/// Each test drives one of the two call sites through the shared seam, so a
/// future change that bypasses the seam at <em>either</em> site goes red here.
/// <c>CompleteSplitAsync</c> is reached by reflection because it is private,
/// but the property under test is the wiring, not the unit: the sibling test
/// routes the mock's hint call into a <em>real</em> non-fresh leaf, so the
/// production callee runs.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string StaleHintTreeId = "tree-stale-hint";

    /// <summary>
    /// Builds a leaf whose partition checkpoints have already advanced to
    /// <paramref name="checkpoints"/> - the shape a split sibling is in on a
    /// retry, having kept applying since it was born.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State)
        CreateLeafAtCheckpoints(string replicaId, long[]? checkpoints, IBPlusLeafGrain? siblingStub = null)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = StaleHintTreeId;
        if (checkpoints is not null)
        {
            state.State.ProjectionCheckpointOffset = checkpoints[0];
            state.State.ProjectionCheckpointOffsetAssigned = true;
            state.State.ProjectionCheckpointOffsetsByPartition = (long[])checkpoints.Clone();
        }

        return (CreateGrain(state, replicaId: replicaId, siblingStub: siblingStub), state);
    }

    /// <summary>
    /// Pre-stages a split on <paramref name="donorState"/> so the test can drive
    /// <c>CompleteSplitAsync</c> directly, bypassing <c>SplitAsync</c>'s
    /// sibling grain-id allocation (which calls an extension method NSubstitute
    /// cannot intercept). This is the same seam the recovery path drives.
    /// </summary>
    private static GrainId StageSplit(BPlusLeafGrain donor, FakePersistentState<LeafNodeState> donorState)
    {
        for (var i = 0; i < 8; i++)
        {
            donor.EntriesForTest[$"k{i:D2}"] = new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes($"v{i:D2}"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 + i },
            };
        }

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        donorState.State.TreeId = StaleHintTreeId;
        donorState.State.SplitState = SplitState.SplitInProgress;
        donorState.State.SplitKey = "k04";
        donorState.State.SplitSiblingId = siblingId;
        donorState.State.NextSibling = siblingId;
        return siblingId;
    }

    /// <summary>
    /// Builds the sibling stub every split-completion path calls into. The hint
    /// call is left unconfigured so a caller can route it wherever the test
    /// needs.
    /// </summary>
    private static IBPlusLeafGrain CreateSiblingStub()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        sibling.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        sibling.SetKeyRangeAsync(Arg.Any<string>(), Arg.Any<string?>()).Returns(Task.CompletedTask);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>()).Returns(Task.CompletedTask);
        sibling.SetNextSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        sibling.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        return sibling;
    }

    private static Task<SplitResult> InvokeCompleteSplitAsync(BPlusLeafGrain donor, long[]? walHeadsAtSplit)
    {
        var completeSplit = typeof(BPlusLeafGrain).GetMethod(
            "CompleteSplitAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.That(completeSplit, Is.Not.Null,
            "BPlusLeafGrain.CompleteSplitAsync was renamed; update this regression test.");
        return (Task<SplitResult>)completeSplit!.Invoke(donor, new object?[] { walHeadsAtSplit })!;
    }

    // -----------------------------------------------------------------
    // Call site A - the sibling, hinted over a grain reference.
    // -----------------------------------------------------------------

    [Test]
    public async Task CompleteSplit_retry_with_stale_heads_does_not_fault_on_a_non_fresh_sibling()
    {
        // Heads captured at the ORIGINAL split. A retry re-sends them
        // verbatim (CompleteSplitAsync only re-reads heads when the argument
        // is null), so they are far behind where the sibling has since got to.
        long[] headsAtSplit = { 19L, 19L };
        long[] siblingCheckpoints = { 8485L, 8485L };

        var (sibling, _) = CreateLeafAtCheckpoints("stale-hint-sibling", siblingCheckpoints);

        var siblingBefore = new long[headsAtSplit.Length];
        for (var p = 0; p < siblingBefore.Length; p++)
        {
            siblingBefore[p] = sibling.GetCurrentCheckpointForPartition(p);
            Assert.That(siblingBefore[p], Is.GreaterThan(headsAtSplit[p]),
                $"the sibling must start AHEAD of the stale head on partition {p}, "
                + "or this test cannot express the defect");
        }

        var siblingStub = CreateSiblingStub();
        // Route the one call under test into the REAL callee, so the
        // production hint seam runs rather than a mock swallowing it.
        siblingStub.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>())
            .Returns(call => ((IBPlusLeafGrain)sibling).SetCheckpointOffsetHintsAsync(call.ArgAt<long[]>(0)));

        var (donor, donorState) = CreateLeafAtCheckpoints(
            "stale-hint-donor", checkpoints: null, siblingStub: siblingStub);
        var siblingId = StageSplit(donor, donorState);

        var result = await InvokeCompleteSplitAsync(donor, headsAtSplit);

        Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
        await siblingStub.Received(1).SetCheckpointOffsetHintsAsync(Arg.Any<long[]>());

        // The stale hint must have been dropped, not applied: the sibling's
        // checkpoint is bound to what it was before the call, never to a
        // literal, so a sibling change to the seeding path cannot silently
        // turn this assertion vacuous.
        for (var p = 0; p < siblingBefore.Length; p++)
        {
            Assert.That(sibling.GetCurrentCheckpointForPartition(p), Is.EqualTo(siblingBefore[p]),
                $"a stale hint must not move the sibling's partition {p} checkpoint");
        }
    }

    [Test]
    public void SetCheckpointOffsetHints_ignores_a_hint_at_or_below_the_current_checkpoint()
    {
        long[] checkpoints = { 8485L, 8485L };
        var (leaf, _) = CreateLeafAtCheckpoints("stale-hint-callee", checkpoints);

        var before = new long[checkpoints.Length];
        for (var p = 0; p < before.Length; p++)
        {
            before[p] = leaf.GetCurrentCheckpointForPartition(p);
        }

        // Below on partition 0, exactly at the current checkpoint on
        // partition 1 - both are non-advancing and both must be dropped.
        long[] hints = { 19L, before[1] };

        Assert.DoesNotThrowAsync(
            () => ((IBPlusLeafGrain)leaf).SetCheckpointOffsetHintsAsync(hints),
            "a non-advancing hint must be dropped by the callee, not passed to the "
            + "projection seam, which rejects a backward move by throwing");

        for (var p = 0; p < before.Length; p++)
        {
            Assert.That(leaf.GetCurrentCheckpointForPartition(p), Is.EqualTo(before[p]),
                $"a non-advancing hint must leave partition {p} exactly where it was");
        }
    }

    [Test]
    public async Task SetCheckpointOffsetHints_advances_a_fresh_leaf_to_the_hinted_head()
    {
        // The guard must drop only non-advancing hints. A leaf at birth is
        // behind every head, so every hint here must land - otherwise the
        // three tests above would all pass against a seam that did nothing.
        long[] heads = { 19L, 27L };
        var (leaf, _) = CreateLeafAtCheckpoints("fresh-hint-callee", checkpoints: null);

        for (var p = 0; p < heads.Length; p++)
        {
            Assert.That(leaf.GetCurrentCheckpointForPartition(p), Is.LessThan(heads[p]),
                $"a fresh leaf must start behind the head for partition {p}");
        }

        await ((IBPlusLeafGrain)leaf).SetCheckpointOffsetHintsAsync(heads);

        for (var p = 0; p < heads.Length; p++)
        {
            Assert.That(leaf.GetCurrentCheckpointForPartition(p), Is.EqualTo(heads[p]),
                $"an advancing hint must move partition {p} to the hinted head");
        }
    }

    // -----------------------------------------------------------------
    // Call site B - the donor, hinted in-process on `this`.
    // -----------------------------------------------------------------

    [Test]
    public async Task CompleteSplit_retry_with_stale_heads_does_not_move_the_donor_checkpoint_backward()
    {
        long[] headsAtSplit = { 19L, 19L };
        long[] donorCheckpoints = { 8485L, 8485L };

        var siblingStub = CreateSiblingStub();
        siblingStub.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);

        var (donor, donorState) = CreateLeafAtCheckpoints(
            "stale-hint-donor-ahead", donorCheckpoints, siblingStub: siblingStub);

        var donorBefore = new long[headsAtSplit.Length];
        for (var p = 0; p < donorBefore.Length; p++)
        {
            donorBefore[p] = donor.GetCurrentCheckpointForPartition(p);
            Assert.That(donorBefore[p], Is.GreaterThan(headsAtSplit[p]),
                $"the donor must start AHEAD of the stale head on partition {p}, "
                + "or this test cannot express the defect");
        }

        var siblingId = StageSplit(donor, donorState);

        var result = await InvokeCompleteSplitAsync(donor, headsAtSplit);

        Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
        for (var p = 0; p < donorBefore.Length; p++)
        {
            Assert.That(donor.GetCurrentCheckpointForPartition(p), Is.EqualTo(donorBefore[p]),
                $"a stale head must not move the donor's partition {p} checkpoint");
        }
    }
}
