using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2082: the per-partition WAL prefix-trim
/// probe that gates snapshot rehydrate must confine a coordinator fault to
/// the partition that raised it.
/// <para>
/// <c>AnyPartitionWalPrefixTrimmedAsync</c> asks each WAL partition whether
/// its oldest still-readable offset has advanced past zero. A non-zero tail
/// on ANY partition is positive evidence that the WAL GC trimmed a prefix,
/// which makes a snapshot at or behind the persisted checkpoint the sole
/// durable copy of that prefix and so worth rehydrating. The <c>try</c>
/// originally wrapped the whole partition loop, so the FIRST faulting
/// coordinator aborted the probe for every later partition and the method
/// reported "not trimmed" for the whole tree. On an 8-partition tree one
/// slow coordinator masked the other seven.
/// </para>
/// <para>
/// The consequence is expensive and self-reinforcing: declining the
/// rehydrate forces a full replay from the oldest readable offset, which
/// keeps the tree saturated, which makes the next probe (itself a grain
/// call INTO that tree) more likely to fault, which declines again. The
/// failure mode fires most readily exactly when its cost is highest.
/// </para>
/// <para>
/// These tests pin the three behaviours that matter: (1) a fault on an
/// early partition must not hide a trimmed prefix on a later one;
/// (2) a positive is returned on the first non-zero tail and is NOT gated
/// on a fault-free sweep; and (3) when nothing could be proved the decline
/// is unchanged, because a probe that could not prove a prefix was trimmed
/// must never be read as proof that it was.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Wraps a coordinator so <see cref="ILeafReplayCoordinatorGrain.GetTailOffsetAsync"/>
    /// faults the way a saturated partition's coordinator does - an
    /// asynchronous failure from the grain call, not a synchronous throw.
    /// </summary>
    private static ILeafReplayCoordinatorGrain WithFaultingTailProbe(
        ILeafReplayCoordinatorGrain coordinator,
        string reason = "coordinator timed out")
    {
        coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<long>(new TimeoutException(reason)));
        return coordinator;
    }

    /// <summary>
    /// Drives a first activation that is torn down after its first
    /// incremental flush, leaving checkpoint 4 and a covering snapshot at
    /// offset 4 durable on <paramref name="state"/> / <paramref name="store"/>.
    /// </summary>
    private static void BankACoveringSnapshotAtOffsetFour(
        FakePersistentState<LeafNodeState> state,
        InMemorySnapshotStore store)
    {
        var fullEntries = Enumerable.Range(1, 12)
            .Select(i => FlushSet(i, $"k{i:D2}"))
            .ToArray();

        using var cts = new CancellationTokenSource();
        var persists = 0;
        state.OnWriteState = _ =>
        {
            if (++persists == 1)
                cts.Cancel();
        };

        var coldP0 = BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: null, fullEntries);
        var coldP1 = BuildObservableCoordinator(head: 0, sliceSize: 4, tail: 0, onRead: null);
        var grain = BuildFlushCeilingLeaf(state, [coldP0, coldP1], store.Stub, reclassifyEveryN: 1);

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(cts.Token));

        state.OnWriteState = null;

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L),
            "precondition: the interrupted cold replay banked its first slice");
        Assert.That(store.Latest, Is.Not.Null,
            "precondition: the interrupted cold replay captured a covering snapshot");
    }

    [Test]
    public async Task A_faulting_partition_probe_does_not_mask_a_trimmed_prefix_on_a_later_partition()
    {
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        BankACoveringSnapshotAtOffsetFour(state, store);

        // Second activation. Partition 0's coordinator faults on the tail
        // probe (the saturated-coordinator case), while partition 1 reports a
        // trimmed tail - so a prefix demonstrably HAS been trimmed and the
        // snapshot at offset 4 is the only durable copy of offsets [1, 4].
        // Partition 0 now exposes only the surviving suffix (4, 12], exactly
        // as a trimmed WAL would: k01..k04 can ONLY come from the snapshot.
        var suffixEntries = Enumerable.Range(5, 8)
            .Select(i => FlushSet(i, $"k{i:D2}"))
            .ToArray();

        var p0 = WithFaultingTailProbe(
            BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: null, suffixEntries));
        var p1 = BuildObservableCoordinator(head: 0, sliceSize: 4, tail: 5, onRead: null);

        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub, reclassifyEveryN: 1);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // The rehydrate must have happened: the replay resumed strictly past
        // the durable offset instead of restarting from the oldest readable
        // one, and the trimmed prefix survived. The DidNotReceive is the
        // discriminating assertion - a cold replay slices THROUGH offset 4,
        // so only the absence of the from-oldest read proves the resume.
        await p0.Received().ReadSliceAsync(4L, 12L, Arg.Any<int>(), Arg.Any<CancellationToken>());
        await p0.DidNotReceive().ReadSliceAsync(-1L, Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
        for (var i = 1; i <= 12; i++)
        {
            Assert.That(await grain.GetAsync($"k{i:D2}"), Is.Not.Null,
                $"key k{i:D2} missing: k01..k04 prove the snapshot was rehydrated despite partition 0's "
                + "probe faulting, k05..k12 prove the surviving suffix still replayed");
        }
    }

    [Test]
    public async Task A_trimmed_prefix_is_positive_evidence_even_when_another_partition_could_not_be_probed()
    {
        // The same masking shape, asserted as the rule rather than through the
        // replay outcome: the positive must NOT be gated on a fault-free
        // sweep. Gating it would reintroduce the masking one level up, since
        // "some partition faulted" would once again suppress a genuine
        // trimmed-prefix finding.
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        BankACoveringSnapshotAtOffsetFour(state, store);

        var suffixEntries = Enumerable.Range(5, 8)
            .Select(i => FlushSet(i, $"k{i:D2}"))
            .ToArray();

        // Three partitions, two of which fault, and the trimmed one probed
        // LAST - so only a probe that survives every fault can reach it.
        var p0 = WithFaultingTailProbe(
            BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: null, suffixEntries),
            "partition 0 coordinator timed out");
        var p1 = WithFaultingTailProbe(
            BuildObservableCoordinator(head: 0, sliceSize: 4, tail: 0, onRead: null),
            "partition 1 coordinator timed out");
        var p2 = BuildObservableCoordinator(head: 0, sliceSize: 4, tail: 9, onRead: null);

        var grain = BuildFlushCeilingLeaf(state, [p0, p1, p2], store.Stub, reclassifyEveryN: 1);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            // Every partition was asked, rather than the sweep stopping at the
            // first fault.
            await p1.Received().GetTailOffsetAsync(Arg.Any<CancellationToken>());
            await p2.Received().GetTailOffsetAsync(Arg.Any<CancellationToken>());
        });

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
        await p0.DidNotReceive().ReadSliceAsync(-1L, Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(await grain.GetAsync("k01"), Is.Not.Null,
            "the trimmed prefix must be rehydrated from the snapshot even though two earlier "
            + "partitions could not be probed");
    }

    [Test]
    public async Task An_untrimmed_wal_still_declines_the_rehydrate()
    {
        // Durability semantics are deliberately unchanged. A snapshot at or
        // behind the persisted checkpoint is only worth rehydrating when a
        // prefix was actually trimmed; against an intact WAL it is redundant
        // and the leaf must replay instead. Confining faults to their own
        // partition must not weaken that, so every partition here is probed
        // successfully and honestly reports a tail of zero.
        //
        // The "could not probe" branch declines through the SAME return, so
        // it is safe by construction: the fix only distinguishes the two in
        // the log, never in the return value. It is not exercised end to end
        // here because a declined rehydrate then reaches the separate #945
        // fall-off guard, which probes the same coordinator and deliberately
        // does NOT tolerate a fault - a different call site with its own
        // fail-closed contract.
        var state = NewFlushCeilingState();
        var store = new InMemorySnapshotStore();
        BankACoveringSnapshotAtOffsetFour(state, store);

        var fullEntries = Enumerable.Range(1, 12)
            .Select(i => FlushSet(i, $"k{i:D2}"))
            .ToArray();

        var p0 = BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: null, fullEntries);
        var p1 = BuildObservableCoordinator(head: 0, sliceSize: 4, tail: 0, onRead: null);

        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub, reclassifyEveryN: 1);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Replayed the intact WAL from its oldest readable offset rather than
        // rehydrating, and still converged. -1 is the from-oldest sentinel,
        // and it is exactly the read the rehydrating tests must NOT make.
        await p0.Received().ReadSliceAsync(-1L, 12L, Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
        Assert.That(await grain.GetAsync("k01"), Is.Not.Null,
            "the intact WAL rebuilds the prefix when no partition reports a trimmed one");
    }
}
