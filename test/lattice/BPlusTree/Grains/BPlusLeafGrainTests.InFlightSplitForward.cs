using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- (#3583) A span forward never targets a split sibling that is not yet initialised ---
    //
    // A split persists its intent before it seeds the new sibling: from that
    // point until the donor's high bound narrows, NextSibling names the new
    // sibling while HighKeyExclusive is still the pre-split bound and
    // OldNextSibling is the real successor. The sibling has no declared span
    // yet, so it accepts and acknowledges any key forwarded to it, and
    // InitializeSiblingAsync then gives it [splitKey, preSplitHigh) without
    // moving anything. A key at or above the pre-split bound forwarded there
    // sits outside the span every read is routed by: the write was
    // acknowledged and no read ever finds it.
    //
    // The commit-time span re-check reaches that state directly. A commit is
    // admitted, a split interleaves with its WAL append that narrows the leaf
    // and a second split then starts, and the re-check relocates the key.
    //
    // Perturbation: revert TryResolveSpanForwardTarget to prefer NextSibling
    // while a split is in flight and every relocation test below fails,
    // because the key goes to the uninitialised sibling.

    private static readonly GrainId InFlightSibling = GrainId.Create("leaf", "in-flight-sibling");
    private static readonly GrainId RealSuccessor = GrainId.Create("leaf", "real-successor");

    /// <summary>
    /// A leaf whose first armed WAL append narrows its span to <c>[.., "m")</c>
    /// and then starts a second split at <c>"f"</c>, leaving the division in
    /// flight: <c>NextSibling</c> names the uninitialised new sibling and
    /// <c>OldNextSibling</c> the real successor, exactly as
    /// <c>SplitAsync</c> leaves them before <c>CompleteSplitAsync</c> runs.
    /// </summary>
    private static (BPlusLeafGrain Grain, IBPlusLeafGrain NewSibling, IBPlusLeafGrain Successor, Action Arm)
        CreateSplitStartingDuringAppendGrain()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var armed = false;
        var fired = false;
        void StartSplit()
        {
            if (!armed || fired) return;
            fired = true;
            state.State.HighKeyExclusive = "m";
            state.State.SplitKey = "f";
            state.State.SplitSiblingId = InFlightSibling;
            state.State.OldNextSibling = RealSuccessor;
            state.State.NextSibling = InFlightSibling;
            state.State.SplitInFlight = true;
        }

        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                StartSplit();
                return Task.FromResult(0L);
            });
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                StartSplit();
                var records = (IReadOnlyList<WalRecord>)callInfo[0];
                return Task.FromResult<IReadOnlyList<long>>(new long[records.Count]);
            });

        var newSibling = Substitute.For<IBPlusLeafGrain>();
        var successor = Substitute.For<IBPlusLeafGrain>();
        var grain = CreateGrain(
            state,
            commitLog: writer,
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain>
            {
                [InFlightSibling] = newSibling,
                [RealSuccessor] = successor,
            });
        return (grain, newSibling, successor, () => armed = true);
    }

    [Test]
    public async Task SetAsync_relocates_to_the_real_successor_when_a_split_starts_during_the_wal_append()
    {
        var (grain, newSibling, successor, arm) = CreateSplitStartingDuringAppendGrain();
        arm();

        await grain.SetAsync("p", Utf8("v"));

        Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.False);
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
        await successor.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && !d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task SetManyAsync_relocates_to_the_real_successor_when_a_split_starts_during_the_wal_append()
    {
        var (grain, newSibling, successor, arm) = CreateSplitStartingDuringAppendGrain();
        arm();

        await grain.SetManyAsync(Batch("a", "h", "p", "z"));

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "h" }),
            "Keys the in-flight split's donor still declares stay here for its transfer to carry across.");
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
        await successor.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Keys.OrderBy(k => k, StringComparer.Ordinal).SequenceEqual(new[] { "p", "z" })),
            false);
    }

    [Test]
    public async Task MergeManyAsync_relocates_to_the_real_successor_when_a_split_starts_during_the_wal_append()
    {
        var (grain, newSibling, successor, arm) = CreateSplitStartingDuringAppendGrain();
        arm();
        var stamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Utf8("a"), stamp),
            ["p"] = LwwValue<byte[]>.Create(Utf8("p"), stamp),
        });

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a" }));
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
        await successor.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && d["p"].Timestamp == stamp),
            false);
    }

    [Test]
    public async Task DeleteTrackedAsync_relocates_tombstone_to_the_real_successor_when_a_split_starts_during_the_wal_append()
    {
        var (grain, newSibling, successor, arm) = CreateSplitStartingDuringAppendGrain();
        await grain.SetAsync("p", Utf8("old"));
        arm();

        var result = await grain.DeleteTrackedAsync("p");

        Assert.That(result.Deleted, Is.True);
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
        await successor.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task SetAsync_keeps_a_key_the_in_flight_donor_still_declares_local()
    {
        // "h" is above the in-flight split key but below the donor's pre-split
        // high bound, so the donor still declares it and the split's own
        // transfer carries it to the sibling once that sibling is initialised.
        var (grain, newSibling, successor, arm) = CreateSplitStartingDuringAppendGrain();
        arm();

        var split = await grain.SetAsync("h", Utf8("v"));

        Assert.Multiple(() =>
        {
            Assert.That(split, Is.Null);
            Assert.That(grain.EntriesForTest.ContainsKey("h"), Is.True);
        });
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
        await successor.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    // --- (#3583) Split recovery routes the caller's write by declared span ---
    //
    // The recovery branches in SetCoreAsync and MergeManyAsync used to route the
    // caller's write by SplitKey / SplitSiblingId after awaiting the recovery.
    // Those fields describe the most recent division, and a new one can start
    // between the recovery completing and the caller resuming. The caller then
    // forwarded to that new, uninitialised sibling. Each test starts a second
    // division on the persist that completes the recovered one.
    //
    // Perturbation: restore the SplitKey / SplitSiblingId routing in either
    // recovery branch and the matching test below fails.

    private static readonly GrainId RecoveredSibling = GrainId.Create("leaf", "recovered-sibling");
    private static readonly GrainId RecoveryDownstream = GrainId.Create("leaf", "recovery-downstream");

    /// <summary>
    /// A leaf holding an interrupted division at <c>"m"</c> whose recovery, on
    /// the persist that completes it, sees a second division start at
    /// <c>"f"</c> toward the uninitialised <see cref="InFlightSibling"/>.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, IBPlusLeafGrain RecoveredSiblingStub, IBPlusLeafGrain NewSibling)>
        CreateRecoveryRacedBySecondSplitGrainAsync()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var recoveredSibling = Substitute.For<IBPlusLeafGrain>();
        var newSibling = Substitute.For<IBPlusLeafGrain>();
        var downstream = Substitute.For<IBPlusLeafGrain>();
        var grain = CreateGrain(
            state,
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain>
            {
                [RecoveredSibling] = recoveredSibling,
                [InFlightSibling] = newSibling,
                [RecoveryDownstream] = downstream,
            });

        await grain.SetAsync("b", Utf8("b"));
        await grain.SetAsync("n", Utf8("n"));

        state.State.TreeId = "test-tree";
        state.State.HighKeyExclusive = "z";
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = RecoveredSibling;
        state.State.OldNextSibling = RecoveryDownstream;
        state.State.NextSibling = RecoveredSibling;
        state.State.SplitInFlight = true;

        var fired = false;
        state.OnWriteState = s =>
        {
            if (fired || s.SplitInFlight || s.HighKeyExclusive != "m") return;
            fired = true;
            s.SplitKey = "f";
            s.SplitSiblingId = InFlightSibling;
            s.OldNextSibling = s.NextSibling;
            s.NextSibling = InFlightSibling;
            s.SplitInFlight = true;
        };

        return (grain, recoveredSibling, newSibling);
    }

    [Test]
    public async Task SetAsync_after_recovery_keeps_a_declared_key_local_when_a_second_split_starts()
    {
        var (grain, _, newSibling) = await CreateRecoveryRacedBySecondSplitGrainAsync();

        await grain.SetAsync("h", Utf8("v"));

        Assert.That(grain.EntriesForTest.ContainsKey("h"), Is.True,
            "The donor still declares the key, so the in-flight division's transfer carries it.");
        await newSibling.DidNotReceiveWithAnyArgs().SetAsync(default!, default!, default);
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task SetAsync_after_recovery_forwards_beyond_span_to_the_initialised_sibling_when_a_second_split_starts()
    {
        var (grain, recoveredSibling, newSibling) = await CreateRecoveryRacedBySecondSplitGrainAsync();

        await grain.SetAsync("p", Utf8("v"));

        Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.False);
        await newSibling.DidNotReceiveWithAnyArgs().SetAsync(default!, default!, default);
        await recoveredSibling.Received(1).SetAsync("p", Arg.Any<byte[]>(), Arg.Any<long>());
    }

    [Test]
    public async Task MergeManyAsync_after_recovery_routes_by_declared_span_when_a_second_split_starts()
    {
        var (grain, recoveredSibling, newSibling) = await CreateRecoveryRacedBySecondSplitGrainAsync();
        var stamp = new HybridLogicalClock { WallClockTicks = long.MaxValue / 2, Counter = 0 };

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["h"] = LwwValue<byte[]>.Create(Utf8("h"), stamp),
            ["p"] = LwwValue<byte[]>.Create(Utf8("p"), stamp),
        });

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.ContainsKey("h"), Is.True);
            Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.False);
        });
        await newSibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
        await recoveredSibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p")),
            false);
    }
}
