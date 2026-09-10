using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detector for the <c>PrepareTx(t)</c> row of <c>spec/Refinement.md</c>
/// (gap #2553). The row claims <c>AtomicWriteGrain.PrepareAsync</c> +
/// <c>ExecutePhaseAsync</c> "stage every write into per-leaf pending buckets
/// (hidden)". Before these tests only half of that claim was falsifiable:
/// <c>BPlusLeafGrainTests.GetAsync_with_in_flight_pending_uses_pre_saga_visibility</c>
/// proves a staged bucket stays hidden at the leaf, but it installs that
/// bucket directly, so nothing asserted that the saga itself routes its
/// writes into one.
/// <para>
/// WHAT THE SAGA'S SHARE OF THAT CLAIM ACTUALLY IS, and therefore what these
/// tests assert. The execute phase does not touch a leaf. It dispatches one
/// <see cref="ILattice.SetManyAsync"/> per batch, and the routing decision it
/// owns is carried entirely by two ambients on the Orleans
/// <see cref="RequestContext"/>: <c>LatticePreparedContext</c> selects the
/// prepared branch (<c>BPlusLeafGrain.CommitSetManyAsync</c> reads
/// <c>LatticePreparedContext.Current</c> into its <c>isPrepared</c> flag and
/// calls <c>AddPreparedMutation</c> instead of writing the visible
/// projection) and <c>LatticeTransactionContext</c> names WHICH pending
/// bucket (<c>AddPreparedMutation</c> keys <c>_pendingTx</c> by it, and
/// throws rather than accept <see cref="Guid.Empty"/>). Both must hold at the
/// instant the batch is dispatched; either one absent and the same call
/// becomes an ordinary immediately-visible write. Asserting them at that
/// instant is therefore asserting the routing itself at the seam where the
/// saga makes it, not a proxy for it.
/// </para>
/// <para>
/// WHY THE UNIT TIER RATHER THAN INTEGRATION. A pending bucket exists only
/// between the prepare fan-out and the terminal broadcast, and a saga run
/// through the public API closes that window itself, so an integration test
/// cannot observe the bucket without pausing the saga mid-flight. That is
/// exactly what the chaos-tier <c>AtomicVisibilityChaosTests</c> witnesses do,
/// probabilistically and CI-only - which is the coverage #2553 records as
/// insufficient. A deterministic per-PR guard has to observe the decision
/// where it is made, and the substituted <c>ILattice</c> the saga harness
/// already carries puts the test on exactly that boundary.
/// </para>
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// Captures the ambient prepared flag, transaction id and dispatched keys
    /// observed at the moment the saga calls
    /// <see cref="ILattice.SetManyAsync"/>. Both ambients are read inside the
    /// substitute's callback, which NSubstitute invokes synchronously on the
    /// dispatching call, so the observation is of the live scope rather than
    /// of anything reconstructed afterwards.
    /// </summary>
    private static (Func<bool?> Prepared, Func<Guid> TransactionId, Func<List<string>?> Keys)
        ObserveDispatchContext(ILattice lattice)
    {
        bool? prepared = null;
        var transactionId = Guid.Empty;
        List<string>? keys = null;

        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(callInfo =>
            {
                prepared = LatticePreparedContext.Current;
                transactionId = LatticeTransactionContext.Current;
                keys = ((List<KeyValuePair<string, byte[]>>)callInfo[0])
                    .Select(kv => kv.Key)
                    .ToList();
                return Task.CompletedTask;
            });

        return (() => prepared, () => transactionId, () => keys);
    }

    [Test]
    public async Task ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path()
    {
        var (grain, state, _, lattice, _) = CreateGrain();
        var observed = ObserveDispatchContext(lattice);

        // Anti-vacuity: nothing outside the saga is in a prepared scope, so a
        // true reading below can only have come from the execute phase.
        Assert.That(LatticePreparedContext.Current, Is.False);
        Assert.That(LatticeTransactionContext.Current, Is.EqualTo(Guid.Empty));

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Prepared(),
                Is.True,
                "The execute phase dispatched its batch outside a LatticePreparedContext scope. "
                + "The leaf's commit path reads that ambient to choose AddPreparedMutation over a "
                + "visible write, so these entries would land immediately visible instead of in a "
                + "hidden per-leaf pending bucket - the saga would no longer refine PrepareTx(t).");

            Assert.That(
                observed.TransactionId(),
                Is.Not.EqualTo(Guid.Empty),
                "The execute phase dispatched its batch with no ambient transaction id. A prepared "
                + "mutation is keyed by that id, and AddPreparedMutation rejects Guid.Empty rather "
                + "than leak the write into a bucket no terminal can ever find.");

            Assert.That(
                observed.TransactionId(),
                Is.EqualTo(state.State.TransactionId),
                "The batch was staged under a different transaction id from the one the saga "
                + "persisted and will later broadcast its terminal for, so the terminal would not "
                + "match the bucket the write created.");

            // Every entry travels the prepared path, not merely the first: a
            // regression that staged one batch and then fell through to a
            // direct write for the remainder would still satisfy the
            // assertions above on their own.
            Assert.That(observed.Keys(), Is.EqualTo(new[] { "a", "b", "c" }));
        });

        // The scope is bounded by the phase; it must not leak past it.
        Assert.That(LatticePreparedContext.Current, Is.False);
    }

    [Test]
    public async Task ReceiveReminder_resumes_execute_through_the_prepared_path()
    {
        // Crash-replay half of the same claim. The reminder-driven resume
        // re-enters ExecutePhaseAsync without re-running the caller's
        // ExecuteAsync entry, so a routing scope established on the entry path
        // alone would leave every resumed write immediately visible - the
        // exact split-visibility shape AllOrNothing exists to exclude.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]), ("b", [2]), ("c", [3]));
        state.State.PreValues =
        [
            new AtomicPreValue { Key = "a", Value = null, Existed = false },
            new AtomicPreValue { Key = "b", Value = null, Existed = false },
            new AtomicPreValue { Key = "c", Value = null, Existed = false },
        ];
        state.State.NextIndex = 1;
        state.State.AtomicBatchSize = 3;
        state.State.TransactionId = Guid.NewGuid();

        var (grain, _, _, lattice, _) = CreateGrain(state);
        var observed = ObserveDispatchContext(lattice);

        Assert.That(LatticePreparedContext.Current, Is.False);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(
                observed.Prepared(),
                Is.True,
                "A resumed execute phase dispatched its remaining entries outside a "
                + "LatticePreparedContext scope, so they would land immediately visible while the "
                + "pre-crash entries stayed staged.");
            Assert.That(observed.TransactionId(), Is.EqualTo(state.State.TransactionId));
            Assert.That(observed.Keys(), Is.EqualTo(new[] { "b", "c" }));
        });
    }
}
