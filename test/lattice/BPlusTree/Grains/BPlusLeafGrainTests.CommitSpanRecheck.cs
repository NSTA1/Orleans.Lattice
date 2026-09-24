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
    // --- (#3523) A commit re-checks the declared span after its WAL append ---
    //
    // The foreground write paths are [AlwaysInterleave], so a split can divide
    // the leaf while a commit awaits its WAL append. The split moves every row at
    // or above its split key to the new sibling and narrows this leaf's high
    // bound. A value applied locally after that sits outside the range every
    // reader is routed by: an acknowledged write is lost, or a deleted value
    // resurfaces. Each test narrows the span from inside the WAL append, which
    // is exactly that window, and pins that the value goes to the leaf that now
    // declares the key rather than being stored here.
    //
    // Perturbation: make StoreAdmittedEntry store unconditionally and every
    // relocation test below fails, because the value stays on this leaf.

    private static readonly GrainId NarrowedSuccessor = GrainId.Create("leaf", "narrowed-successor");

    /// <summary>
    /// A commit-log writer that, once <paramref name="arm"/> has been invoked,
    /// narrows <paramref name="state"/>'s declared span to <c>[.., "m")</c> on
    /// its next append, as an interleaved split would. Appends before arming
    /// pass through, so a test can seed rows through the real write path first.
    /// </summary>
    private static ICommitLogWriter CreateSpanNarrowingWriter(
        FakePersistentState<LeafNodeState> state, out Action arm)
    {
        var armed = false;
        var narrowed = false;
        arm = () => armed = true;
        void Narrow()
        {
            if (!armed || narrowed) return;
            narrowed = true;
            state.State.HighKeyExclusive = "m";
            state.State.NextSibling = NarrowedSuccessor;
        }

        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Narrow();
                return Task.FromResult(0L);
            });
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                Narrow();
                var records = (IReadOnlyList<WalRecord>)callInfo[0];
                return Task.FromResult<IReadOnlyList<long>>(new long[records.Count]);
            });
        return writer;
    }

    /// <summary>
    /// A leaf whose span narrows during the first append made after the returned
    /// arm action runs. Rows seeded before arming commit normally.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, Action Arm) CreateSpanNarrowingGrain(
        IBPlusLeafGrain sibling)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var writer = CreateSpanNarrowingWriter(state, out var arm);
        var grain = CreateGrain(state, siblingStub: sibling, commitLog: writer);
        return (grain, state, arm);
    }

    private static SplitResult SiblingSplit(string promotedKey) => new()
    {
        PromotedKey = promotedKey,
        NewSiblingId = GrainId.Create("leaf", "sibling-split-" + promotedKey),
        ChildIsLeaf = true,
    };

    private static IBPlusLeafGrain CreateSplittingSibling(string promotedKey)
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<bool>())
            .Returns(Task.FromResult<SplitResult?>(SiblingSplit(promotedKey)));
        return sibling;
    }

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task SetAsync_relocates_value_when_span_narrows_during_wal_append()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        arm();

        await grain.SetAsync("p", Utf8("v"));

        Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.False,
            "A value the span no longer covers must not be stored where no read looks.");
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && !d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task SetAsync_returns_the_relocation_split_marked_forwarded()
    {
        var sibling = CreateSplittingSibling("q");
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        arm();

        var split = await grain.SetAsync("p", Utf8("v"));

        Assert.That(split, Is.Not.Null, "The relocation's split must reach the shard root to be linked.");
        Assert.Multiple(() =>
        {
            Assert.That(split!.PromotedKey, Is.EqualTo("q"));
            Assert.That(split.Forwarded, Is.True,
                "A sibling's split must be linked by re-descent, not against this leaf's path.");
        });
    }

    [Test]
    public async Task SetAsync_keeps_value_local_when_span_still_covers_key_after_wal_append()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        arm();

        var split = await grain.SetAsync("b", Utf8("v"));

        Assert.Multiple(() =>
        {
            Assert.That(split, Is.Null);
            Assert.That(grain.EntriesForTest.ContainsKey("b"), Is.True);
        });
        await sibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task SetAsync_keeps_value_local_when_narrowed_span_has_no_forward_target()
    {
        // A split always sets the successor pointer, so this is the torn-chain
        // case. There is nowhere better to put the value, so it stays here
        // under the fail-open rule rather than being dropped.
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var state = new FakePersistentState<LeafNodeState>();
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                state.State.HighKeyExclusive = "m";
                return Task.FromResult(0L);
            });
        var grain = CreateGrain(state, siblingStub: sibling, commitLog: writer);

        await grain.SetAsync("p", Utf8("v"));

        Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.True);
        await sibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task SetManyAsync_relocates_only_entries_the_narrowed_span_excludes()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        arm();

        await grain.SetManyAsync(Batch("a", "b", "p", "z"));

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }));
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Keys.OrderBy(k => k, StringComparer.Ordinal).SequenceEqual(new[] { "p", "z" })),
            false);
    }

    [Test]
    public async Task SetManyAsync_returns_the_relocation_split_marked_forwarded()
    {
        var sibling = CreateSplittingSibling("q");
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        arm();

        var split = await grain.SetManyAsync(Batch("a", "p"));

        Assert.Multiple(() =>
        {
            Assert.That(split?.PromotedKey, Is.EqualTo("q"));
            Assert.That(split?.Forwarded, Is.True);
        });
    }

    [Test]
    public async Task MergeManyAsync_relocates_entries_the_narrowed_span_excludes_with_their_stamps()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        arm();
        var stamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Utf8("a"), stamp),
            ["p"] = LwwValue<byte[]>.Create(Utf8("p"), stamp),
        };

        await grain.MergeManyAsync(entries);

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a" }));
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && d["p"].Timestamp == stamp),
            false);
    }

    [Test]
    public async Task DeleteTrackedAsync_relocates_tombstone_when_span_narrows_during_wal_append()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        await grain.SetAsync("p", Utf8("old"));
        arm();

        var result = await grain.DeleteTrackedAsync("p");

        Assert.That(result.Deleted, Is.True);
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task DeleteTrackedAsync_returns_the_relocation_split_marked_forwarded()
    {
        var sibling = CreateSplittingSibling("q");
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        await grain.SetAsync("p", Utf8("old"));
        arm();

        var result = await grain.DeleteTrackedAsync("p");

        Assert.That(result.Split, Is.Not.Null,
            "A split the relocated tombstone caused must reach the shard root to be linked.");
        Assert.Multiple(() =>
        {
            Assert.That(result.Split!.PromotedKey, Is.EqualTo("q"));
            Assert.That(result.Split.Forwarded, Is.True);
        });
    }

    [Test]
    public async Task DeleteTrackedAsync_stores_tombstone_locally_when_span_still_covers_key()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        await grain.SetAsync("b", Utf8("old"));
        arm();

        var result = await grain.DeleteTrackedAsync("b");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.True);
            Assert.That(result.Split, Is.Null);
            Assert.That(grain.EntriesForTest["b"].IsTombstone, Is.True);
        });
        await sibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public async Task DeleteTrackedAsync_returns_not_deleted_for_absent_key()
    {
        var grain = CreateGrain();

        var result = await grain.DeleteTrackedAsync("missing");

        Assert.That(result, Is.EqualTo(default(LeafDeleteResult)));
    }

    [Test]
    public async Task DeleteTrackedAsync_out_of_span_forwards_tracked_and_marks_split_forwarded()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.DeleteTrackedAsync("p")
            .Returns(Task.FromResult(new LeafDeleteResult { Deleted = true, Split = SiblingSplit("q") }));
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        var result = await grain.DeleteTrackedAsync("p");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.True);
            Assert.That(result.Split?.PromotedKey, Is.EqualTo("q"));
            Assert.That(result.Split?.Forwarded, Is.True,
                "The declaring leaf's split must be linked by re-descent.");
        });
        await sibling.DidNotReceive().DeleteAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DeleteAsync_out_of_span_forwards_through_untracked_method()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.DeleteAsync("p").Returns(Task.FromResult(true));
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        var deleted = await grain.DeleteAsync("p");

        Assert.That(deleted, Is.True);
        await sibling.DidNotReceive().DeleteTrackedAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DeleteAsync_relocates_tombstone_when_span_narrows_during_wal_append()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        await grain.SetAsync("p", Utf8("old"));
        arm();

        var deleted = await grain.DeleteAsync("p");

        Assert.That(deleted, Is.True);
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey("p") && d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task DeleteRangeAsync_relocates_tombstones_the_narrowed_span_excludes_and_returns_split()
    {
        var sibling = CreateSplittingSibling("q");
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        await grain.SetAsync("b", Utf8("b"));
        await grain.SetAsync("p", Utf8("p"));
        arm();

        var result = await grain.DeleteRangeAsync("a", "z");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.EqualTo(2),
                "Every matched key was tombstoned, here or on the leaf that now declares it.");
            Assert.That(grain.EntriesForTest["b"].IsTombstone, Is.True);
            Assert.That(grain.EntriesForTest["p"].IsTombstone, Is.False,
                "The tombstone for a key this leaf no longer declares belongs on its custodian.");
            Assert.That(result.Split?.PromotedKey, Is.EqualTo("q"));
            Assert.That(result.Split?.Forwarded, Is.True);
        });
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p") && d["p"].IsTombstone),
            false);
    }

    [Test]
    public async Task DeleteRangeAsync_without_span_change_reports_no_split()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, arm) = CreateSpanNarrowingGrain(sibling);
        await grain.SetAsync("b", Utf8("b"));
        arm();

        var result = await grain.DeleteRangeAsync("a", "c");

        Assert.Multiple(() =>
        {
            Assert.That(result.Deleted, Is.EqualTo(1));
            Assert.That(result.Split, Is.Null);
        });
        await sibling.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }
}
