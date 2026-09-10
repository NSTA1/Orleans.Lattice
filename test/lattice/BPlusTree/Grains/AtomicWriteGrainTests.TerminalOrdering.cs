using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detector for the TLA+ property <c>LinearizedTerminals</c> (issue #2551): the
/// saga's tree-wide decision write must precede its per-leaf terminal
/// broadcast, so no leaf can surface a committed value before a decision exists
/// for a reader to resolve against.
/// <para>
/// WHAT THESE PIN, AND WHY THEY ARE THREE TESTS. Production orders the pair -
/// <c>RecordTerminalDecisionAsync</c> then <c>BroadcastTerminalsAsync</c> - at
/// three independent call sites in <c>AtomicWriteGrain</c>: the post-execute
/// commit tail of <c>RunSagaAsync</c>, its <c>Compensate</c> (abort) tail, and
/// the cross-tree <c>FinalizeAsync</c> tail. Each is its own pair of adjacent
/// <c>await</c>s, so a reversal at one site is invisible to a test that drives
/// only another; one test per site is the only shape under which each reversal
/// is independently falsifiable. A single parameterised test would not do,
/// because the three sites are reached through three different entry points
/// rather than through three arguments.
/// </para>
/// <para>
/// WHY <c>Received.InOrder</c> RATHER THAN AN OBSERVED RACE. The ordering is a
/// two-call sequence within one grain turn, so the sequence is the observable.
/// An integration test that reads through the saga converges to the same
/// settled state under either order and cannot see the mid-window exposure at
/// all - which is exactly how this gap survived until the #2527 census. The
/// idiom is already established here for structurally identical invariants:
/// the saturation gate in <c>AtomicWriteGrainTests.ShutdownRefused</c> and the
/// split swap in <c>TreeShardSplitGrainTests.SwapReorder</c>.
/// </para>
/// <para>
/// WHY THE COYOTE TIER DOES NOT SUBSUME THESE.
/// <c>AtomicCommitInvariantCoyoteTests</c> drives <c>AtomicCommitInvariantModel</c>,
/// a hand-written saga lifecycle whose ordering is sequenced inside the model.
/// It never invokes <c>RunSagaAsync</c>, <c>RecordTerminalDecisionAsync</c> or
/// <c>BroadcastTerminalsAsync</c>, so the shipped ordering that the model
/// abstracts is unasserted there.
/// </para>
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// Builds the grain with an explicit <see cref="ITxRegistryGrain"/>
    /// substitute so one ordered call sequence spans both the registry (the
    /// decision write) and the shard root (the terminal broadcast).
    /// </summary>
    private static (AtomicWriteGrain Grain,
                    FakePersistentState<AtomicWriteState> State,
                    ILattice Lattice,
                    IShardRootGrain Shard,
                    ITxRegistryGrain Registry) CreateGrainForTerminalOrdering()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        // The post-fan-out late-pickup loop re-fetches participants; a stable
        // empty set keeps the terminal fan-out to exactly one RPC, which is
        // what makes the ordered sequence below unambiguous.
        registry.GetParticipantsAsync(Arg.Any<Guid>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(new List<int>()));

        var (grain, state, _, lattice, shard) = CreateGrain(
            configureFactory: f => f.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry));
        shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));

        return (grain, state, lattice, shard, registry);
    }

    /// <summary>
    /// The saga's terminal broadcast RPC, matched loosely on every argument
    /// except the commit verdict - these tests pin the ordering, not the
    /// payload. The verdict goes through <c>Arg.Is</c> rather than a bare
    /// literal because the signature carries a second <see cref="bool"/>
    /// (<c>inlineWalAppend</c>): mixing a literal and a matcher across two
    /// arguments of one type is ambiguous to NSubstitute and throws.
    /// </summary>
    private static void ExpectTerminalBroadcast(IShardRootGrain shard, bool committed) =>
        shard.AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            Arg.Is(committed),
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(),
            Arg.Any<bool>());

    /// <summary>
    /// Asserts the terminal broadcast fired exactly once with the given
    /// verdict. Guards the ordering assertions below against vacuity: an
    /// ordered sequence over calls that never happened is not a passing
    /// ordering check, and NSubstitute's own message for that case does not
    /// say which half went missing.
    /// </summary>
    private static Task AssertTerminalBroadcastReceivedOnce(IShardRootGrain shard, bool committed) =>
        shard.Received(1).AppendTxTerminalAsync(
            Arg.Any<Guid>(),
            Arg.Is(committed),
            Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
            Arg.Any<CancellationToken>(),
            Arg.Any<bool>());

    [Test]
    public async Task RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals()
    {
        // Ordering site 1 of 3: the post-execute commit tail of RunSagaAsync.
        var (grain, state, _, shard, registry) = CreateGrainForTerminalOrdering();

        await grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await AssertTerminalBroadcastReceivedOnce(shard, committed: true);

        // The linearization point. A leaf that flips before the registry has
        // recorded the decision can surface a committed value to a reader who
        // then dials the registry and is told the saga is still in flight -
        // the partial-commit view that AllOrNothing exists to exclude.
        Received.InOrder(() =>
        {
            registry.MarkCommittedAsync(Arg.Any<Guid>());
            ExpectTerminalBroadcast(shard, committed: true);
        });
    }

    [Test]
    public async Task RunSagaAsync_abort_records_the_decision_before_broadcasting_terminals()
    {
        // Ordering site 2 of 3: the Compensate tail of RunSagaAsync. A
        // separate call site from the commit tail, and the one that runs when
        // something has already gone wrong, so a fix applied only to the
        // commit path would leave this half of the linearization point
        // unprotected.
        var (grain, state, lattice, shard, registry) = CreateGrainForTerminalOrdering();
        // A plain storage failure, deliberately NOT the shutdown-refused
        // sentinel: that regime skips the broadcast entirely, which would make
        // the ordering unobservable rather than wrong.
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Throws(new InvalidOperationException("simulated staging failure"));

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k", [1]))));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed),
            "The compensating saga still drives to Completed; only its recorded outcome differs.");
        await registry.Received(1).MarkAbortedAsync(Arg.Any<Guid>());
        await AssertTerminalBroadcastReceivedOnce(shard, committed: false);

        Received.InOrder(() =>
        {
            registry.MarkAbortedAsync(Arg.Any<Guid>());
            ExpectTerminalBroadcast(shard, committed: false);
        });
    }

    [Test]
    public async Task FinalizeAsync_records_the_decision_before_broadcasting_terminals()
    {
        // Ordering site 3 of 3: the cross-tree FinalizeAsync tail. Reached
        // only through the prepare-and-pause path, so neither RunSagaAsync
        // test covers it.
        var (grain, state, _, shard, registry) = CreateGrainForTerminalOrdering();

        var vote = await grain.PrepareForCoordinatorAsync(
            TreeId, MakeEntries(("k", [1])), predicate: null, coordinatorKey: "xcoord-1",
            participants: new[] { TreeId });
        Assert.That(vote, Is.EqualTo(CrossTreePrepareVote.Prepared),
            "Anti-vacuity: FinalizeAsync short-circuits unless the sub-saga actually parked in Prepared.");

        await grain.FinalizeAsync(commit: true);

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await AssertTerminalBroadcastReceivedOnce(shard, committed: true);

        Received.InOrder(() =>
        {
            registry.MarkCommittedAsync(Arg.Any<Guid>());
            ExpectTerminalBroadcast(shard, committed: true);
        });
    }
}
