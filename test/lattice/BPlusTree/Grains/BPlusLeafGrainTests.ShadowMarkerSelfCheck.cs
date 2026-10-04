using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4545: a destination-side shadow marker must never gate a key after its
/// saga's terminal can no longer clear it.
/// <para>
/// Three rules close it:
/// </para>
/// <list type="number">
/// <item><description>A marker installed after the leaf applied the saga's terminal is not installed.</description></item>
/// <item><description>A leaf split never carries a marker for a saga whose terminal the donor applied.</description></item>
/// <item><description>A marker that carries the saga's marked original prepare stamp P is released once the row it guards is stamped at or above P. This is the rule that holds when the first two cannot: a reactivation forgets the terminal, and a marker installed afterwards is indistinguishable from a live one.</description></item>
/// </list>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly HybridLogicalClock MarkedPrepare = new() { WallClockTicks = 5_000_000, Counter = 4 };

    /// <summary>Seeds a migrated row for <paramref name="key"/> stamped <paramref name="stamp"/>.</summary>
    private static void SeedMigratedRowAt(BPlusLeafGrain grain, string key, byte[] value, HybridLogicalClock stamp) =>
        grain.EntriesForTest[key] = LwwValue<byte[]>.Create(value, stamp) with { IsMigrated = true };

    private static async Task MarkWithStampAsync(BPlusLeafGrain grain, Guid txid, string key, HybridLogicalClock? stamp)
    {
        using (LatticeOriginalPrepareStampContext.With(
            stamp is { } p ? new Dictionary<string, HybridLogicalClock> { [key] = p } : null))
        {
            await grain.MarkSagaShadowAsync(txid, [key]);
        }
    }

    private static Task<byte[]?> ReadUnderAsync(BPlusLeafGrain grain, string key, Guid txid, TxStatus status)
    {
        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [txid] = status }))
        {
            return grain.GetAsync(key);
        }
    }

    // ---- Rule 3: the self-verifying marker

    /// <summary>
    /// The reactivation counter-trace the shard-ownership retention model found
    /// (b0c753b8, depth 14): the leaf applied the saga's terminal, a reactivation
    /// forgot it, and the delayed marker install arrives. The leaf cannot tell it
    /// from a live marker, and no terminal will come again. The row already holds
    /// the saga's value at its prepare stamp, so the read must be served.
    /// </summary>
    [Test]
    public async Task A_marker_carrying_its_marked_prepare_stamp_is_released_once_the_row_is_at_that_stamp(
        [Values] bool committed)
    {
        var status = committed ? TxStatus.Committed : TxStatus.Indeterminate;
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [7], MarkedPrepare);

        await MarkWithStampAsync(grain, txid, "k", MarkedPrepare);

        Assert.That(await ReadUnderAsync(grain, "k", txid, status), Is.EqualTo(new byte[] { 7 }),
            "the row is the saga's own value; a terminal this leaf never sees must not hide it");
    }

    [Test]
    public async Task A_marker_carrying_its_marked_prepare_stamp_releases_a_later_write()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [8], new HybridLogicalClock { WallClockTicks = 6_000_000, Counter = 0 });

        await MarkWithStampAsync(grain, txid, "k", MarkedPrepare);

        Assert.That(await ReadUnderAsync(grain, "k", txid, TxStatus.Committed), Is.EqualTo(new byte[] { 8 }));
    }

    /// <summary>
    /// The direction the self-check must never relax: a migrated row stamped below
    /// the saga's prepare is the pre-saga value. Serving it under a committed
    /// saga would tear the batch and lose the saga's write for this key.
    /// </summary>
    [Test]
    public async Task A_marker_carrying_its_marked_prepare_stamp_still_gates_a_row_below_that_stamp()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [1], new HybridLogicalClock { WallClockTicks = 4_000_000, Counter = 0 });

        await MarkWithStampAsync(grain, txid, "k", MarkedPrepare);

        Assert.That(async () => await ReadUnderAsync(grain, "k", txid, TxStatus.Committed),
            Throws.InstanceOf<StaleShardRoutingException>());
    }

    /// <summary>
    /// A marker installed without a marked stamp - by an older silo, from an
    /// unmarked prepare, or from a resize copy whose writes P does not order -
    /// keeps the original gate, whatever the row's stamp.
    /// </summary>
    [Test]
    public async Task A_marker_without_a_marked_prepare_stamp_keeps_the_original_gate()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        SeedMigratedRowAt(grain, "k", [1], new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 });

        await MarkWithStampAsync(grain, txid, "k", stamp: null);

        Assert.That(async () => await ReadUnderAsync(grain, "k", txid, TxStatus.Committed),
            Throws.InstanceOf<StaleShardRoutingException>());
    }

    [Test]
    public async Task Installing_a_marker_with_a_marked_prepare_stamp_moves_the_leaf_clock_past_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 0 };

        await MarkWithStampAsync(grain, Guid.NewGuid(), "k", MarkedPrepare);

        Assert.That(state.State.Clock.CompareTo(MarkedPrepare), Is.GreaterThanOrEqualTo(0),
            "property H: every write this leaf acknowledges afterwards is stamped above the saga's prepare");
    }

    [Test]
    public async Task A_terminal_clears_the_marker_stamp_with_the_marker()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        var txid = Guid.NewGuid();
        await MarkWithStampAsync(grain, txid, "z", MarkedPrepare);

        await grain.ApplyTxTerminalAsync(txid, committed: true);
        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Is.Empty, "the terminal cleared the marker, so a split has nothing to carry");
    }

    // ---- Rule 1: no marker installed after its terminal

    [Test]
    public async Task A_marker_installed_after_its_terminal_is_not_installed_and_a_split_does_not_carry_it()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        var txid = Guid.NewGuid();

        await grain.ApplyTxTerminalAsync(txid, committed: true);
        await grain.MarkSagaShadowAsync(txid, ["z"]);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Is.Empty,
            "a marker the leaf received after the saga's terminal guards nothing; a split must not carry it "
            + "onto a sibling the terminal will never reach");
    }

    // ---- Rule 2 and the transfer carrying P

    /// <summary>
    /// A leaf split carries each marker's marked prepare stamp, so the sibling's
    /// gate can release the marker it inherits even though the terminal for it
    /// was, or will be, delivered elsewhere.
    /// </summary>
    [Test]
    public async Task A_leaf_split_carries_each_markers_marked_prepare_stamp_to_the_sibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        HybridLogicalClock? carried = null;
        sibling
            .When(s => s.MarkSagaShadowAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<string>>()))
            .Do(_ => carried = LatticeOriginalPrepareStampContext.TryGetStamp("z", out var p) ? p : null);
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        var txid = Guid.NewGuid();
        await MarkWithStampAsync(grain, txid, "z", MarkedPrepare);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling).Single().Txid, Is.EqualTo(txid));
        Assert.That(carried, Is.EqualTo(MarkedPrepare));
        Assert.That(LatticeOriginalPrepareStampContext.HasStamps, Is.False,
            "the carried stamps are scoped to the transfer and must not leak into the donor's own context");
    }

    [Test]
    public async Task A_leaf_split_carries_a_marker_without_a_stamp_without_one()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var sibling = CreateSplitSiblingStub();
        var sawStamp = false;
        sibling
            .When(s => s.MarkSagaShadowAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<string>>()))
            .Do(_ => sawStamp = LatticeOriginalPrepareStampContext.HasStamps);
        var grain = CreateGrain(state, siblingStub: sibling);
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("2"));
        await MarkWithStampAsync(grain, Guid.NewGuid(), "z", stamp: null);

        ArmInterruptedSplit(state, sibling, splitKey: "m");
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(ShadowMarksOn(sibling), Has.Count.EqualTo(1));
        Assert.That(sawStamp, Is.False, "an unmarked marker stays on the original gate on the sibling too");
    }
}
