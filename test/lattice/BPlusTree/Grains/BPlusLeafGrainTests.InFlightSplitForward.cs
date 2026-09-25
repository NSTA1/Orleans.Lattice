using System.Text;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
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
    // Forwarding to OldNextSibling instead is not enough on its own. Once the
    // new sibling is initialised, a reclaim can fold an empty OldNextSibling
    // into it and retire it, so a forward still aimed there fails, or is
    // lost if the retired leaf is later reactivated with no declared span.
    // The commit-time relocation therefore completes the interrupted split
    // first and routes by the narrowed span, which names the sibling only
    // once it is initialised.
    //
    // The commit-time span re-check reaches that state directly. A commit is
    // admitted, a split interleaves with its WAL append that narrows the leaf
    // and a second split then starts, and the re-check relocates the key. The
    // old successor below is retired, as the reclaim fold leaves it.
    //
    // Perturbation: revert TryResolveSpanForwardTarget to prefer NextSibling
    // while a split is in flight, or drop the recovery from
    // RelocateStrandedAsync, and every relocation test below fails.

    private static readonly GrainId InFlightSibling = GrainId.Create("leaf", "in-flight-sibling");
    private static readonly GrainId RealSuccessor = GrainId.Create("leaf", "real-successor");

    /// <summary>
    /// A leaf whose first armed WAL append narrows its span to <c>[.., "m")</c>
    /// and then starts a second split at <c>"f"</c>, leaving the division in
    /// flight: <c>NextSibling</c> names the new sibling and
    /// <c>OldNextSibling</c> the old successor, exactly as <c>SplitAsync</c>
    /// leaves them before <c>CompleteSplitAsync</c> runs. The old successor
    /// is retired and refuses every write. <c>SiblingCalls</c> records the new
    /// sibling's initialisation and every merge forwarded to it, in order.
    /// </summary>
    private static (BPlusLeafGrain Grain, IBPlusLeafGrain NewSibling, IBPlusLeafGrain Successor, List<string> SiblingCalls, Action Arm)
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

        var siblingCalls = new List<string>();
        var newSibling = Substitute.For<IBPlusLeafGrain>();
        newSibling.When(s => s.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()))
            .Do(_ => siblingCalls.Add("init"));
        newSibling.When(s => s.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>()))
            .Do(c => siblingCalls.Add(
                "merge:" + string.Join(",", ((Dictionary<string, LwwValue<byte[]>>)c[0]).Keys.OrderBy(k => k, StringComparer.Ordinal))));

        var successor = Substitute.For<IBPlusLeafGrain>();
        successor.MergeManyAsync(default!, default)
            .ThrowsAsyncForAnyArgs(new LeafRetiredException(RealSuccessor.ToString()));

        var grain = CreateGrain(
            state,
            commitLog: writer,
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain>
            {
                [InFlightSibling] = newSibling,
                [RealSuccessor] = successor,
            });
        return (grain, newSibling, successor, siblingCalls, () => armed = true);
    }

    [Test]
    public async Task SetAsync_relocates_only_to_an_initialised_sibling_when_a_split_starts_during_the_wal_append()
    {
        var (grain, _, successor, siblingCalls, arm) = CreateSplitStartingDuringAppendGrain();
        arm();

        await grain.SetAsync("p", Utf8("v"));

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.False);
            Assert.That(siblingCalls, Is.EqualTo(new[] { "init", "merge:p" }));
        });
        await successor.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task SetManyAsync_relocates_only_to_an_initialised_sibling_when_a_split_starts_during_the_wal_append()
    {
        var (grain, _, successor, siblingCalls, arm) = CreateSplitStartingDuringAppendGrain();
        arm();

        await grain.SetManyAsync(Batch("a", "h", "p", "z"));

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a" }),
                "The completed split carries the in-span key \"h\" to the sibling with the rest of its right half.");
            Assert.That(siblingCalls, Is.EqualTo(new[] { "init", "merge:p,z" }));
        });
        await successor.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task MergeManyAsync_relocates_only_to_an_initialised_sibling_when_a_split_starts_during_the_wal_append()
    {
        var (grain, newSibling, successor, siblingCalls, arm) = CreateSplitStartingDuringAppendGrain();
        arm();
        var stamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Utf8("a"), stamp),
            ["p"] = LwwValue<byte[]>.Create(Utf8("p"), stamp),
        });

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a" }));
            Assert.That(siblingCalls, Is.EqualTo(new[] { "init", "merge:p" }));
        });
        await newSibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d["p"].Timestamp == stamp),
            false);
        await successor.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task DeleteTrackedAsync_relocates_tombstone_only_to_an_initialised_sibling_when_a_split_starts_during_the_wal_append()
    {
        var (grain, newSibling, successor, siblingCalls, arm) = CreateSplitStartingDuringAppendGrain();
        await grain.SetAsync("p", Utf8("old"));
        arm();

        var result = await grain.DeleteTrackedAsync("p");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.True);
            Assert.That(siblingCalls, Is.EqualTo(new[] { "init", "merge:p" }));
        });
        await newSibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d["p"].IsTombstone),
            false);
        await successor.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task SetAsync_keeps_a_key_the_in_flight_donor_still_declares_local()
    {
        // "h" is above the in-flight split key but below the donor's pre-split
        // high bound, so the donor still declares it and the split's own
        // transfer carries it to the sibling once that sibling is initialised.
        var (grain, newSibling, successor, _, arm) = CreateSplitStartingDuringAppendGrain();
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

    // --- (#3583) Delete and conditional bulk write complete an interrupted split before forwarding ---
    //
    // A donor whose split was interrupted after its sibling was initialised
    // (a crash between the sibling's initialisation and the donor's narrow)
    // still holds its pre-split high bound, so a key at or above it is out of
    // span. OldNextSibling names the old successor, and a reclaim may have
    // folded that successor into the initialised sibling and retired it. The
    // delete and conditional bulk write paths used to forward there without
    // completing the split, as SetAsync and SetManyAsync already did.
    //
    // Perturbation: remove the HasInterruptedSplit recovery from either
    // DeleteCoreAsync or SetManyWherePredicateAsync and the matching test
    // below fails, because the key goes to the retired successor. Run the
    // recovery on the untracked DeleteAsync shape as well and the two
    // DeleteAsync tests fail, because that shape drops the recovered split.

    /// <summary>
    /// A leaf holding an interrupted division at <c>"m"</c> whose new sibling
    /// (<see cref="RecoveredSibling"/>) is initialised and whose old successor
    /// (<see cref="RecoveryDownstream"/>) has been retired by a reclaim fold.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, IBPlusLeafGrain Sibling, IBPlusLeafGrain RetiredSuccessor)>
        CreateInterruptedSplitWithRetiredSuccessorGrainAsync()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.DeleteTrackedAsync(default!)
            .ReturnsForAnyArgs(new LeafDeleteResult { Deleted = true });
        sibling.SetManyWherePredicateAsync(default!, default!)
            .ReturnsForAnyArgs(c => new ConditionalSetManyResult
            {
                WrittenKeys = ((List<KeyValuePair<string, byte[]>>)c[0]).Select(e => e.Key).ToList(),
            });

        var retired = Substitute.For<IBPlusLeafGrain>();
        var retiredFault = new LeafRetiredException(RecoveryDownstream.ToString());
        retired.DeleteTrackedAsync(default!).ThrowsAsyncForAnyArgs(retiredFault);
        retired.DeleteAsync(default!).ThrowsAsyncForAnyArgs(retiredFault);
        retired.SetManyWherePredicateAsync(default!, default!).ThrowsAsyncForAnyArgs(retiredFault);

        var grain = CreateGrain(
            state,
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain>
            {
                [RecoveredSibling] = sibling,
                [RecoveryDownstream] = retired,
            });

        await grain.SetAsync("b", ScoredJson(1));
        await grain.SetAsync("n", ScoredJson(1));

        state.State.TreeId = "test-tree";
        state.State.HighKeyExclusive = "z";
        state.State.SplitState = SplitState.SplitComplete;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = RecoveredSibling;
        state.State.OldNextSibling = RecoveryDownstream;
        state.State.NextSibling = RecoveredSibling;
        state.State.SplitInFlight = true;

        return (grain, sibling, retired);
    }

    [Test]
    public async Task DeleteTrackedAsync_completes_an_interrupted_split_before_forwarding_past_the_pre_split_bound()
    {
        var (grain, sibling, retired) = await CreateInterruptedSplitWithRetiredSuccessorGrainAsync();

        var result = await grain.DeleteTrackedAsync("zz");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.True);
            Assert.That(result.Split, Is.Not.Null, "The recovered split is reported for the shard root to link.");
            Assert.That(grain.EntriesForTest.ContainsKey("n"), Is.False, "The recovery moved the right half.");
        });
        await sibling.Received(1).DeleteTrackedAsync("zz");
        await retired.DidNotReceiveWithAnyArgs().DeleteTrackedAsync(default!);
    }

    [Test]
    public async Task DeleteAsync_leaves_an_interrupted_split_for_a_tracked_write_to_report()
    {
        // The untracked shape cannot report a split. Completing the recovery
        // there would drop the only SplitResult that links the new sibling, and
        // no later write would see the split as interrupted, so the sibling
        // would stay chained but unreachable by descent.
        var (grain, _, _) = await CreateInterruptedSplitWithRetiredSuccessorGrainAsync();

        var deleted = await grain.DeleteAsync("b");
        var result = await grain.DeleteTrackedAsync("c");

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(result.Split, Is.Not.Null,
                "The split the untracked delete left interrupted is completed and reported by the next tracked write.");
        });
    }

    [Test]
    public async Task DeleteAsync_relocation_leaves_an_interrupted_split_for_a_tracked_write_to_report()
    {
        var (grain, _, successor, siblingCalls, arm) = CreateSplitStartingDuringAppendGrain();
        successor.MergeManyAsync(default!, default).ReturnsForAnyArgs((SplitResult?)null);
        await grain.SetAsync("p", Utf8("old"));
        arm();

        var deleted = await grain.DeleteAsync("p");

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(siblingCalls, Is.Empty, "The untracked delete must not complete a split it cannot report.");
        });
        await successor.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task DeleteTrackedAsync_reports_the_recovered_split_when_the_key_is_absent()
    {
        var (grain, _, _) = await CreateInterruptedSplitWithRetiredSuccessorGrainAsync();

        var result = await grain.DeleteTrackedAsync("c");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.False);
            Assert.That(result.Split, Is.Not.Null);
        });
    }

    [Test]
    public async Task SetManyWherePredicateAsync_completes_an_interrupted_split_before_forwarding_past_the_pre_split_bound()
    {
        var (grain, sibling, retired) = await CreateInterruptedSplitWithRetiredSuccessorGrainAsync();

        var result = await grain.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>> { Kv("b", 9), Kv("zz", 9) },
            ScoreAtLeast(0));

        Assert.Multiple(() =>
        {
            Assert.That(result.WrittenKeys, Is.EquivalentTo(new[] { "b", "zz" }));
            Assert.That(result.Split, Is.Not.Null, "The recovered split is reported for the shard root to link.");
        });
        await sibling.Received(1).SetManyWherePredicateAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(l => l.Count == 1 && l[0].Key == "zz"),
            Arg.Any<LatticePredicateNode>());
        await retired.DidNotReceiveWithAnyArgs().SetManyWherePredicateAsync(default!, default!);
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
