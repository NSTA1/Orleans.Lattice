using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Span admission on the saga terminal (issue #4335). A leaf split that
/// narrows a donor's span while a saga's prepared bucket is still on the
/// donor leaves prepared keys the donor no longer declares. The terminal
/// must not drain those keys into the donor - that stores a second row for
/// the key in the shard's chain, over-counting and breaking the ordered scan
/// - but re-deliver them as a backstop to the leaf that declares them. A
/// committed-values backstop routed to the donor by a stale descent is
/// re-routed the same way.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId StrandedSuccessorId = GrainId.Create("leaf", "stranded-successor");

    [TearDown]
    public void ClearStrandedPreparedAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusLeafGrain Successor) CreateNarrowableDonor()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var successor = Substitute.For<IBPlusLeafGrain>();
        var grain = CreateGrain(
            state,
            leafStubs: new Dictionary<GrainId, IBPlusLeafGrain> { [StrandedSuccessorId] = successor });
        return (grain, state, successor);
    }

    private static void NarrowTo(FakePersistentState<LeafNodeState> state, string highKeyExclusive)
    {
        state.State.HighKeyExclusive = highKeyExclusive;
        state.State.NextSibling = StrandedSuccessorId;
    }

    [Test]
    public async Task ApplyTxTerminalAsync_commit_forwards_stranded_prepared_key_instead_of_draining_it()
    {
        var (grain, state, successor) = CreateNarrowableDonor();
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "a", [1]);
        await PreparedSetAsync(grain, txid, "x", [2]);
        NarrowTo(state, "m");

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(grain.EntriesForTest["a"].Value, Is.EqualTo(new byte[] { 1 }),
            "An in-span prepared key still drains locally.");
        Assert.That(grain.EntriesForTest.ContainsKey("x"), Is.False,
            "A prepared key the donor no longer declares must not be drained into it.");
        await successor.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d =>
                d != null && d.Count == 1 && d["x"].SequenceEqual(new byte[] { 2 })));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_commit_forwards_stranded_key_with_committed_value()
    {
        var (grain, state, successor) = CreateNarrowableDonor();
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "x", [2]);
        NarrowTo(state, "m");

        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["x"] = [7] };
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(grain.EntriesForTest.ContainsKey("x"), Is.False);
        await successor.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d =>
                d != null && d.Count == 1 && d["x"].SequenceEqual(new byte[] { 7 })));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_forwards_out_of_span_committed_value()
    {
        var (grain, state, successor) = CreateNarrowableDonor();
        NarrowTo(state, "m");
        var txid = Guid.NewGuid();

        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["a"] = [1],
            ["x"] = [2],
        };
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(grain.EntriesForTest["a"].Value, Is.EqualTo(new byte[] { 1 }));
        Assert.That(grain.EntriesForTest.ContainsKey("x"), Is.False,
            "A backstop key routed here by a stale descent must not be stored outside the span.");
        await successor.Received(1).ApplyTxTerminalAsync(
            txid,
            true,
            Arg.Is<IReadOnlyDictionary<string, byte[]>?>(d => d != null && d.Count == 1 && d.ContainsKey("x")));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_abort_discards_stranded_prepared_key_without_forwarding()
    {
        var (grain, state, successor) = CreateNarrowableDonor();
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "x", [2]);
        NarrowTo(state, "m");

        await grain.ApplyTxTerminalAsync(txid, committed: false, committedValues: null);

        Assert.That(grain.EntriesForTest.ContainsKey("x"), Is.False);
        await successor.DidNotReceiveWithAnyArgs().ApplyTxTerminalAsync(default, default, default);
    }

    [Test]
    public async Task ApplyTxTerminalAsync_stranded_key_with_no_successor_fails_open_locally()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "x", [2]);
        state.State.HighKeyExclusive = "m";

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(grain.EntriesForTest["x"].Value, Is.EqualTo(new byte[] { 2 }),
            "With no neighbour to forward to, the committed value is kept rather than lost.");
    }
}
