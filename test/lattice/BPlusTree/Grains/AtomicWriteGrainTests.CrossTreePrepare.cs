using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Cross-tree prepare-and-pause coverage for <see cref="AtomicWriteGrain"/>:
/// the <c>PrepareForCoordinatorAsync</c> path that stages writes, registers the
/// per-tree registry delegation, and parks in <see cref="AtomicWritePhase.Prepared"/>
/// awaiting the coordinator's finalize.
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task PrepareForCoordinatorAsync_happy_path_parks_prepared_and_registers_delegation()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        var (grain, state, _, _, _) = CreateGrain(
            configureFactory: f => f.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry));

        var vote = await grain.PrepareForCoordinatorAsync(
            TreeId, MakeEntries(("k1", [1]), ("k2", [2])), predicate: null, coordinatorKey: "xcoord-1");

        Assert.That(vote, Is.EqualTo(CrossTreePrepareVote.Prepared));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Prepared),
            "a fully-staged cross-tree sub-saga must park in Prepared awaiting finalize");
        await registry.Received(1).RegisterExternalDecisionAuthorityAsync(
            Arg.Any<Guid>(), "xcoord-1");
    }

    [Test]
    public void PrepareForCoordinatorAsync_park_registry_failure_propagates_not_voted_failed()
    {
        // A transient failure registering the registry delegation is RETRYABLE:
        // every prepared write is still staged. The grain must surface it as a
        // thrown exception (so the coordinator keeps the transaction Preparing
        // and retries), NOT a Failed vote - which would spuriously abort the
        // whole cross-tree saga and strand this sub-saga parked forever.
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.RegisterExternalDecisionAuthorityAsync(Arg.Any<Guid>(), Arg.Any<string>())
            .Throws(new TimeoutException("registry blip"));
        var (grain, state, _, _, _) = CreateGrain(
            configureFactory: f => f.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry));

        Assert.CatchAsync(() => grain.PrepareForCoordinatorAsync(
            TreeId, MakeEntries(("k1", [1])), predicate: null, coordinatorKey: "xcoord-1"));

        Assert.That(state.State.Phase, Is.Not.EqualTo(AtomicWritePhase.Prepared),
            "a failed park must leave the sub-saga in a retryable (non-parked) state");
        Assert.That(state.State.Phase, Is.Not.EqualTo(AtomicWritePhase.Completed),
            "a failed park must not terminate the sub-saga");
    }

    [Test]
    public async Task PrepareForCoordinatorAsync_park_retry_after_registry_failure_votes_prepared()
    {
        // First park attempt fails on the registry delegation; the second
        // succeeds (coordinator retry). The sub-saga must re-park cleanly and
        // vote Prepared - proving the failure was retryable, not terminal.
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.RegisterExternalDecisionAuthorityAsync(Arg.Any<Guid>(), Arg.Any<string>())
            .Returns(_ => throw new TimeoutException("registry blip"), _ => Task.CompletedTask);
        var (grain, state, _, _, _) = CreateGrain(
            configureFactory: f => f.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry));
        var entries = MakeEntries(("k1", [1]));

        Assert.CatchAsync(() => grain.PrepareForCoordinatorAsync(
            TreeId, entries, predicate: null, coordinatorKey: "xcoord-1"));

        var vote = await grain.PrepareForCoordinatorAsync(
            TreeId, entries, predicate: null, coordinatorKey: "xcoord-1");

        Assert.That(vote, Is.EqualTo(CrossTreePrepareVote.Prepared));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Prepared));
    }
}
