using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the "persisted / in-memory divergence on
/// <c>WriteStateAsync</c> failure" anti-pattern (bug-hunter Class B) against
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeMergeGrain"/>. Every
/// mutation+persist pair on this workflow grain mutates <c>state.State</c>
/// in memory before awaiting <c>state.WriteStateAsync()</c>. If the persist
/// call throws, the activation is left serving in-memory state that was
/// never durably committed; the grain's idempotency / progress guards then
/// short-circuit retries on the dirty in-memory value, turning a transient
/// storage failure into a permanent divergence until the activation is
/// recycled.
/// </summary>
public partial class TreeMergeGrainTests
{
    [Test]
    public void InitiateMergeState_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: fresh grain - the pre-mutation baseline is the default
        // TreeMergeState (InProgress=false, SourceTreeId=null, etc.).
        var (grain, state, _, grainFactory, _) = CreateGrain();

        var inProgressBefore = state.State.InProgress;
        var sourceTreeIdBefore = state.State.SourceTreeId;
        var sourceShardCountBefore = state.State.SourceShardCount;
        var nextShardIndexBefore = state.State.NextShardIndex;
        var shardRetriesBefore = state.State.ShardRetries;
        var completeBefore = state.State.Complete;
        var sourcePhysicalTreeIdBefore = state.State.SourcePhysicalTreeId;
        var targetPhysicalTreeIdBefore = state.State.TargetPhysicalTreeId;
        var sourcePhysicalShardsBefore = state.State.SourcePhysicalShards;

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        // Act: InitiateMergeStateAsync mutates nine fields in memory before
        // awaiting WriteStateAsync. The thrown exception must leave the
        // activation in a state that matches what a peer or reactivation
        // would read from durable storage - i.e. the pre-mutation defaults.
        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.InitiateMergeStateAsync(SourceTreeId, ShardCount));

        // Assert: every mutated field must be the pre-mutation value. If
        // InProgress stays at true in memory, a follow-up MergeAsync call
        // with a DIFFERENT source tree throws the wrong
        // InvalidOperationException ("already in progress for 'source-tree'")
        // even though the first attempt never persisted - and a retry of
        // MergeAsync with the SAME source returns successfully without
        // re-attempting the storage write, leaving the merge silently
        // un-started on disk.
        Assert.That(state.State.InProgress, Is.EqualTo(inProgressBefore),
            "InProgress mutated in memory survived a failing WriteStateAsync; "
            + "subsequent MergeAsync calls short-circuit on the dirty in-memory "
            + "guard while disk says no merge is running.");
        Assert.That(state.State.SourceTreeId, Is.EqualTo(sourceTreeIdBefore),
            "SourceTreeId mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.SourceShardCount, Is.EqualTo(sourceShardCountBefore),
            "SourceShardCount mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.NextShardIndex, Is.EqualTo(nextShardIndexBefore),
            "NextShardIndex mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.ShardRetries, Is.EqualTo(shardRetriesBefore),
            "ShardRetries mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.Complete, Is.EqualTo(completeBefore),
            "Complete mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.SourcePhysicalTreeId, Is.EqualTo(sourcePhysicalTreeIdBefore),
            "SourcePhysicalTreeId mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.TargetPhysicalTreeId, Is.EqualTo(targetPhysicalTreeIdBefore),
            "TargetPhysicalTreeId mutated in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.SourcePhysicalShards, Is.SameAs(sourcePhysicalShardsBefore),
            "SourcePhysicalShards reference mutated in memory survived a failing WriteStateAsync.");

        GC.KeepAlive(grainFactory);
    }

    [Test]
    public void CompleteMerge_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: a grain that has finished draining every source shard
        // and is about to flip itself to Complete. We seed the state
        // directly (bypassing WriteStateAsync) so the pre-mutation
        // baseline matches what disk would observe.
        var (grain, state, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 5;
        state.State.ShardRetries = 3;
        state.State.SourcePhysicalShards = [0, 1];
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;

        var inProgressBefore = state.State.InProgress;
        var completeBefore = state.State.Complete;
        var nextShardIndexBefore = state.State.NextShardIndex;
        var shardRetriesBefore = state.State.ShardRetries;

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on WriteStateAsync");

        // Act: CompleteMergeAsync flips InProgress -> false and Complete ->
        // true in memory before awaiting WriteStateAsync. The thrown
        // exception must revert the in-memory state.
        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.CompleteMergeAsync());

        // Assert: every mutated field must hold its pre-mutation value.
        // If InProgress stays at false in memory, IsCompleteAsync returns
        // true (lying to the caller); RunMergePassAsync short-circuits at
        // its top-of-method !InProgress guard (no further drain attempts);
        // meanwhile disk still says InProgress=true, so a reactivation
        // would resume the merge under stale assumptions.
        Assert.That(state.State.InProgress, Is.EqualTo(inProgressBefore),
            "InProgress flipped to false in memory survived a failing "
            + "WriteStateAsync; IsCompleteAsync now lies to callers while "
            + "disk says the merge is still running.");
        Assert.That(state.State.Complete, Is.EqualTo(completeBefore),
            "Complete flipped to true in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.NextShardIndex, Is.EqualTo(nextShardIndexBefore),
            "NextShardIndex reset in memory survived a failing WriteStateAsync.");
        Assert.That(state.State.ShardRetries, Is.EqualTo(shardRetriesBefore),
            "ShardRetries reset in memory survived a failing WriteStateAsync.");
    }

    [Test]
    public void ProcessCurrentShard_reverts_retry_counter_when_pre_merge_WriteStateAsync_throws()
    {
        // Arrange: a grain in mid-merge state with one source shard left to
        // drain. ProcessCurrentShardAsync increments ShardRetries and persists
        // BEFORE attempting the merge - this is the documented safety
        // invariant ("a non-throwing crash still burns budget on
        // reactivation"). If that persist throws and the in-memory
        // increment leaks, future ticks on the same activation see an
        // already-incremented counter, while disk still has the old value.
        // On reactivation, ShardRetries reverts to the disk value, losing
        // a unit of retry budget against a deterministic-crash shard.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = 1;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.SourcePhysicalShards = [0];
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;

        // No shard mocks set up - WriteStateAsync throws on the FIRST write
        // inside ProcessCurrentShardAsync (the pre-merge counter increment
        // at L235-236), which fires before MergeShardAsync ever runs.

        var shardRetriesBefore = state.State.ShardRetries;
        var nextShardIndexBefore = state.State.NextShardIndex;

        state.ThrowOnWrite = new InvalidOperationException(
            "simulated storage failure on pre-merge counter increment");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.RunMergePassAsync());

        // Assert: ShardRetries must be the pre-mutation value (0). If the
        // in-memory increment to 1 survives, the documented safety invariant
        // is silently weakened on the dirty window AND a subsequent
        // deactivation drops the increment - net effect: a deterministic-
        // crash shard gets at least one extra retry attempt across the
        // lifecycle.
        Assert.That(state.State.ShardRetries, Is.EqualTo(shardRetriesBefore),
            "ShardRetries incremented in memory survived a failing pre-merge "
            + "WriteStateAsync; the documented safety invariant ('a non-throwing "
            + "crash still burns budget on reactivation') is violated because "
            + "in-memory and disk disagree.");
        Assert.That(state.State.NextShardIndex, Is.EqualTo(nextShardIndexBefore),
            "NextShardIndex must not have moved; the pre-merge write fires "
            + "before the shard drain executes.");

        GC.KeepAlive(grainFactory);
    }
}
