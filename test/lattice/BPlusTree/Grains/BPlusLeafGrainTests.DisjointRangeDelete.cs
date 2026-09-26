using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards issue #3601: replay pass 1 must consume a <see cref="MutationKind.DeleteRange"/>
/// whose range cannot overlap the leaf's key range, instead of deferring it into
/// a durable unresolved-work ledger slot (and, once the ledger is full, arming the
/// in-memory flush clamp). Every leaf reads every range delete, so deferring the
/// disjoint ones made each leaf pay ledger capacity for every other leaf's work.
/// <para>
/// The leaf here owns <c>[d, g)</c>. Partition 0 carries the range deletes and is
/// given the SMALLER backlog, so it is not the partition absorbed last and its
/// terminals defer rather than draining inline - which is what puts them in front
/// of the ledger at all. Partition 1 is a longer run of in-range sets.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string DisjointLeafLow = "d";
    private const string DisjointLeafHigh = "g";

    /// <summary>
    /// A coordinator for a BOUNDED leaf. A leaf with key bounds pushes its
    /// ownership down through the filtered <c>ReadSliceAsync</c> overload
    /// (issue #3565), which <see cref="BuildObservableCoordinator"/> does not
    /// serve, so both overloads are answered here by <see cref="ReplaySliceStub"/>
    /// and <paramref name="onRead"/> observes each read with its 1-based ordinal.
    /// Range deletes are never excluded by the filter, so every one of them
    /// reaches the replay loop, exactly as it does against real storage.
    /// </summary>
    private static ILeafReplayCoordinatorGrain BuildBoundedLeafCoordinator(
        Action<int>? onRead,
        params CommitLogSliceEntry[] entries)
    {
        var head = entries.Length + 1L;
        ReachableWalFixture.EnsureReachable(head, entries);
        var reads = 0;
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                onRead?.Invoke(++reads);
                return Task.FromResult(ReplaySliceStub.Unfiltered(
                    entries, call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2)));
            });
        coord.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                onRead?.Invoke(++reads);
                return Task.FromResult(ReplaySliceStub.Filtered(
                    entries, call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), call.ArgAt<WalKeyFilter>(3)));
            });
        return coord;
    }

    [Test]
    public async Task Disjoint_range_deletes_take_no_ledger_slot_and_do_not_clamp_the_checkpoint()
    {
        // Four range deletes over [m0, m9), wholly above this leaf's [d, g), against
        // a one-slot ledger. Before the fix the first took the slot and the other
        // three were refused into the clamp, so the checkpoint could not pass them
        // during pass 1 and each refusal counted as a drop.
        const int cap = 1;
        var state = NewFlushCeilingState();
        state.State.LowKeyInclusive = DisjointLeafLow;
        state.State.HighKeyExclusive = DisjointLeafHigh;

        var p0Entries = new CommitLogSliceEntry[8];
        for (var i = 1; i <= 8; i++)
            p0Entries[i - 1] = i <= 4 ? FlushDeleteRange(i) : FlushSet(i, $"e{i:D2}");

        var p1Entries = new CommitLogSliceEntry[12];
        for (var i = 1; i <= 12; i++)
            p1Entries[i - 1] = FlushSet(i, $"f{i:D2}");

        int? ledgerCountAtPartitionOne = null;
        long? checkpointAtPartitionOne = null;
        var p0 = BuildBoundedLeafCoordinator(onRead: null, p0Entries);
        var p1 = BuildBoundedLeafCoordinator(onRead: read =>
            {
                if (read != 1)
                    return;

                // Pass 1 has swept partition 0 and not yet begun pass 2.
                ledgerCountAtPartitionOne = state.State.UnresolvedReplayWork?.Count ?? 0;
                checkpointAtPartitionOne = state.State.ProjectionCheckpointOffset;
            }, p1Entries);
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub, maxDurableUnresolvedReplayWork: cap);

        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafDeferredTerminalsDroppedAtCap,
            () => LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(ledgerCountAtPartitionOne, Is.EqualTo(0),
                "A range delete disjoint from the leaf's range must not occupy a durable "
                + "unresolved-work ledger slot: the leaf can never own a key it covers.");
            Assert.That(checkpointAtPartitionOne, Is.EqualTo(p0Entries.Length),
                "With nothing deferred, pass 1 must carry partition 0's checkpoint past the "
                + "disjoint range deletes rather than clamping below them.");
            Assert.That(measurements.Sum(m => m.Value), Is.Zero,
                "No disjoint range delete may be refused at the cap, because none may be recorded.");
            Assert.That(state.State.UnresolvedReplayWork ?? [], Is.Empty);
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(p0Entries.Length));
        });
        Assert.That(await grain.GetAsync("e05"), Is.Not.Null, "Partition 0's own sets were applied.");
        Assert.That(await grain.GetAsync("f01"), Is.Not.Null, "Partition 1 was applied.");
    }

    [TestCase("e0", "e9", "e05", "d20", TestName = "Overlapping_range_delete_inside_the_leaf_is_still_deferred_and_applied")]
    [TestCase("a", "e5", "e01", "f20", TestName = "Overlapping_range_delete_straddling_low_is_still_deferred_and_applied")]
    [TestCase("f", "z", "f05", "d20", TestName = "Overlapping_range_delete_straddling_high_is_still_deferred_and_applied")]
    public async Task Overlapping_range_delete_is_still_deferred_and_applied(
        string start, string end, string victim, string survivor)
    {
        // The unchanged half of issue #3601. A range delete that shares even one
        // key with the leaf must defer exactly as before: it takes a ledger slot
        // in pass 1, and pass 2 applies it to a key the later partition set.
        var state = NewFlushCeilingState();
        state.State.LowKeyInclusive = DisjointLeafLow;
        state.State.HighKeyExclusive = DisjointLeafHigh;

        CommitLogSliceEntry[] p0Entries =
        [
            new(1, BuildDeleteRange(start, end, hlcPhysical: 500, treeId: FlushCeilingTreeId)),
            FlushSet(2, "d10"),
            FlushSet(3, "d11"),
        ];

        var p1Entries = new CommitLogSliceEntry[8];
        p1Entries[0] = FlushSet(1, victim);
        for (var i = 1; i < p1Entries.Length; i++)
            p1Entries[i] = FlushSet(i + 1, survivor);

        var deferredIntoLedger = false;
        var p0 = BuildBoundedLeafCoordinator(onRead: null, p0Entries);
        var p1 = BuildBoundedLeafCoordinator(onRead: read =>
            {
                if (read == 1)
                {
                    deferredIntoLedger = state.State.UnresolvedReplayWork?.Any(
                        e => e.Partition == 0 && e.Offset == 1) ?? false;
                }
            }, p1Entries);
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(deferredIntoLedger, Is.True,
            $"[{start}, {end}) overlaps [{DisjointLeafLow}, {DisjointLeafHigh}) and must still be "
            + "recorded as unresolved replay work in pass 1.");
        Assert.That(await grain.GetAsync(victim), Is.Null,
            "The deferred range delete must still tombstone the in-range key set by the later partition.");
        Assert.That(await grain.GetAsync(survivor), Is.Not.Null, "A key outside the deleted range survives.");
    }
}
