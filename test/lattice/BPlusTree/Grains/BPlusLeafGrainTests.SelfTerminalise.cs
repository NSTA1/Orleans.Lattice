using System.Text;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2190: a leaf self-terminalises a saga prepare
/// that is still resident after activation-time replay once the per-tree
/// <see cref="ITxRegistryGrain"/> reports the saga terminally decided.
/// <para>
/// The scenario these tests reconstruct hermetically. A saga prepare lands in a
/// leaf's pending-tx bucket at some WAL offset. The saga is then decided in the
/// registry, but the matching terminal never reaches this leaf. With the
/// durable unresolved-replay ledger (issue #2165) at capacity - modelled here
/// by <c>maxDurableUnresolvedReplayWork: 0</c>, the no-record path - the
/// resident prepare clamps the incremental flush ceiling one below its own
/// offset. The projection checkpoint cannot advance past it, the checkpoint
/// keeps the coverage-gated WAL GC from trimming the prefix, and every
/// subsequent activation re-reads the identical window and banks no forward
/// progress. Nothing time-, count- or registry-driven removes a resident
/// prepare, so the clamp cannot age out on its own.
/// </para>
/// <para>
/// The fix resolves each resident prepare against the registry during replay
/// and, on a terminal decision, applies it locally through the same
/// <c>ApplyTxCommit</c> / <c>ApplyTxAbort</c> path a real terminal would drive.
/// The clamp then lifts as a consequence of the effect landing - the committed
/// write drains into the cache, or the aborted write is discarded - so the
/// final reconciliation banks the freed prefix. A prepare the registry has NOT
/// decided is left resident and still clamps, which is the isolation-preserving
/// safety property the fix must not break.
/// </para>
/// <para>
/// Two-arm control. Every test below is pre-classified. The two
/// <c>Registry_..._prepare_self_terminalises_...</c> tests are DISCRIMINATORS:
/// they fail without the fix (the checkpoint stays clamped at 1 and the prepare
/// stays resident) and pass with it. The
/// <c>Undecided_resident_prepare_still_clamps_ceiling</c> test and the
/// <c>Registry_failure_during_resolution_...</c> containment test are GUARDS:
/// they assert safety properties (an undecided prepare still clamps; a registry
/// resolution failure degrades to the pre-heal behaviour without failing
/// activation) and pass on both arms. The progress signals asserted are the
/// projection checkpoint (must move 1 -> 4), the pending-tx count (must fall
/// 1 -> 0) and the key's read visibility (the landed effect), never a log line.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    // Set at 1; a saga prepare for "p2" at offset 2; Sets at 3,4. This is the
    // exact offset shape of Incremental_flush_clamps_below_an_unresolved_prepare,
    // so both arms of the control replay an identical window and differ only in
    // the registry's recorded outcome for the prepare's saga.
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) BuildSelfTerminaliseLeaf(
        Guid txId,
        TxStatus registryOutcome,
        out ILeafReplayCoordinatorGrain coordinator,
        long persistedCheckpoint = 0)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(txId).Returns(registryOutcome);
        return BuildSelfTerminaliseLeafCore(txId, registry, out coordinator, persistedCheckpoint);
    }

    // As BuildSelfTerminaliseLeaf, but the registry resolution RPC FAULTS. A
    // TimeoutException models the registry-path timeouts observed on the
    // deployed box under exactly the load that produces this pin. Used by the
    // containment guard: a resolution failure must degrade to the pre-heal
    // behaviour (prepare resident, clamp held) and must never fail the leaf's
    // activation.
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) BuildSelfTerminaliseLeafWithFaultingRegistry(
        Guid txId,
        out ILeafReplayCoordinatorGrain coordinator,
        long persistedCheckpoint = 0)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusAsync(txId)
            .ThrowsAsync(new TimeoutException("registry unavailable during activation replay"));
        return BuildSelfTerminaliseLeafCore(txId, registry, out coordinator, persistedCheckpoint);
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) BuildSelfTerminaliseLeafCore(
        Guid txId,
        ITxRegistryGrain registry,
        out ILeafReplayCoordinatorGrain coordinator,
        long persistedCheckpoint)
    {
        var entries = new[]
        {
            Set(1, "p1"),
            new CommitLogSliceEntry(2, BuildPreparedSet(txId, "p2", Encoding.UTF8.GetBytes("v2"), treeId: ResumableTreeId)),
            Set(3, "p3"),
            Set(4, "p4"),
        };
        coordinator = BuildChunkingCoordinator(head: 4, sliceSize: 2, tail: 0, entries);

        var store = new InMemorySnapshotStore();
        var state = NewResumableState(persistedCheckpoint);

        var (grain, _) = BuildResumableLeaf(
            state, coordinator, store.Stub, reclassifyEveryN: 0,
            // No-record path (issue #2165): the prepare is not durably recorded,
            // so the pre-fix clamp genuinely bites and the resident prepare pins
            // the ceiling. This is the configuration under which the defect
            // reproduces.
            maxDurableUnresolvedReplayWork: 0,
            registry: registry);

        return (grain, state);
    }

    [Test]
    public async Task Registry_committed_resident_prepare_self_terminalises_and_ceiling_advances()
    {
        // DISCRIMINATOR (must FAIL without the fix, PASS with it).
        // The registry has committed the saga, but no terminal reached this
        // leaf, so the prepare for "p2" is resident after replay. Without the
        // fix the ceiling clamps at 1, "p2" stays a hidden pending value and one
        // transaction stays resident forever. With the fix the leaf applies the
        // commit locally: the prepare drains, the ceiling advances to the full
        // frontier (4), and the committed value becomes visible.
        var txId = Guid.NewGuid();
        var (grain, state) = BuildSelfTerminaliseLeaf(txId, TxStatus.Committed, out _);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Progress counter that must MOVE: the checkpoint advances past the
        // resolved prepare to the applied frontier rather than pinning at 1.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L),
            "A registry-committed resident prepare must self-terminalise so the ceiling advances to the applied frontier.");
        // The pending bucket drained: no prepare is left to re-clamp on the next
        // activation, so the window is not re-replayed forever.
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
            "Self-terminalising a committed saga must drain its resident prepare.");
        // The EFFECT landed - this is what separates a legitimate fix from
        // relaxing the clamp. A clamp-relaxation would advance the checkpoint
        // while leaving "p2" a lost pending write; here the committed value is
        // durably applied and readable.
        var value = await grain.GetAsync("p2");
        Assert.That(value, Is.Not.Null,
            "The committed prepare's write must be applied (the effect lands), not merely resolved.");
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Registry_aborted_resident_prepare_self_terminalises_and_ceiling_advances()
    {
        // DISCRIMINATOR (must FAIL without the fix, PASS with it).
        // The registry has aborted the saga. Without the fix the resident
        // prepare still clamps the ceiling at 1 (the leaf cannot tell an
        // undecided prepare from a decided-but-orphaned one). With the fix the
        // leaf applies the abort locally: the prepare is discarded, the ceiling
        // advances to 4, and the prepared value is never surfaced.
        var txId = Guid.NewGuid();
        var (grain, state) = BuildSelfTerminaliseLeaf(txId, TxStatus.Aborted, out _);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L),
            "A registry-aborted resident prepare must self-terminalise so the ceiling advances to the applied frontier.");
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
            "Self-terminalising an aborted saga must discard its resident prepare.");
        // The aborted write must not become visible: abort discards the prepared
        // value, and there was no pre-saga value for "p2".
        Assert.That(await grain.GetAsync("p2"), Is.Null,
            "An aborted prepare's value must be discarded, never surfaced.");
    }

    [Test]
    public async Task Undecided_resident_prepare_still_clamps_ceiling()
    {
        // GUARD (must PASS on BOTH arms).
        // The registry has NOT decided the saga (InFlight - the same view an
        // aged-out decision reads back as). Self-terminalising must NOT fire: a
        // resumed replay still has to re-read the open prepare to rebuild the
        // pending-tx bucket, so the ceiling must stay clamped one below the
        // prepare offset and the transaction must stay resident. This is the
        // isolation-preserving safety property; it is invariant across the fix.
        var txId = Guid.NewGuid();
        var (grain, state) = BuildSelfTerminaliseLeaf(txId, TxStatus.InFlight, out _);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
            "An undecided prepare must keep the checkpoint clamped one below its offset.");
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
            "An undecided prepare must remain resident so a resumed replay can re-read it.");
        // The still-open prepare's value must stay hidden under strict isolation.
        Assert.That(await grain.GetAsync("p2"), Is.Null,
            "An undecided prepare's value must stay hidden until the saga is decided.");
    }

    [Test]
    public async Task Registry_failure_during_resolution_leaves_prepare_resident_and_activation_survives()
    {
        // GUARD (must PASS on BOTH arms).
        // The registry resolution RPC FAULTS (a TimeoutException models the
        // registry-path timeouts observed on the deployed box, under exactly the
        // load that produces this pin). The heal must contain the failure per
        // txid and degrade to the pre-heal behaviour: the leaf still activates,
        // the prepare stays resident, and the ceiling stays clamped one below the
        // prepare offset. It must NOT let the fault escape and fail activation -
        // the host would retry straight back into the same timeout, so the
        // self-heal could never run under the load it exists to clear and a leaf
        // that previously activated-but-pinned would go offline entirely. The
        // heal is simply deferred to a later activation with a reachable
        // registry.
        var txId = Guid.NewGuid();
        var (grain, state) = BuildSelfTerminaliseLeafWithFaultingRegistry(txId, out _);

        // Activation must COMPLETE rather than throw: this await failing is the
        // exact regression the containment prevents.
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Identical end state to the undecided-prepare guard: the resolution
        // could not be obtained, so the prepare is treated as unresolved.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
            "A registry failure during resolution must leave the ceiling clamped exactly as the pre-heal path did.");
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
            "A registry failure must leave the prepare resident so the heal retries on a later activation.");
        // A read of "p2" is deliberately NOT asserted here. The scan-path read
        // (GetWithPendingAsync) independently resolves the resident prepare
        // against the registry, so under this faulting stub the READ path throws
        // too - that is pre-existing read-path behaviour, unchanged by and
        // outside the scope of this activation-time containment. The clamped
        // checkpoint and the resident count already establish that the heal left
        // the prepare and its clamp exactly as the pre-heal path would.
    }
}
