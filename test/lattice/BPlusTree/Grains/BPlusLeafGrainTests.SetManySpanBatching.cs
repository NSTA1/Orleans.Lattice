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
    // --- (#3348) SetMany keeps its batched shape through a split or a span straddle ---
    //
    // The per-key fallback paid one serial WAL admission and commit per key, so
    // under WAL saturation one mid-split or straddling leaf call ran for
    // minutes and blew the all-or-nothing fan-out budget. These pin the batched
    // shape: one append for the local entries and one forwarded SetMany per
    // sibling. Placement (which leaf ends up holding which key) is pinned end to
    // end by LeafSpanAdmissionIntegrationTests.

    private static (ICommitLogWriter Writer, List<string[]> Batches) CreateBatchRecordingWriter()
    {
        var batches = new List<string[]>();
        var writer = Substitute.For<ICommitLogWriter>();
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var records = (IReadOnlyList<WalRecord>)callInfo[0];
                batches.Add(records.Select(r => r.Key).ToArray());
                return Task.FromResult<IReadOnlyList<long>>(new long[records.Count]);
            });
        return (writer, batches);
    }

    private static List<KeyValuePair<string, byte[]>> Batch(params string[] keys) =>
        keys.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes(k))).ToList();

    private static void SeedSealedSpan(FakePersistentState<LeafNodeState> state, GrainId next)
    {
        state.State.LowKeyInclusive = null;
        state.State.HighKeyExclusive = "m";
        state.State.NextSibling = next;
    }

    [Test]
    public async Task SetMany_straddling_the_declared_span_forwards_one_batch_and_appends_the_rest_once()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (writer, batches) = CreateBatchRecordingWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling, commitLog: writer);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        await grain.SetManyAsync(Batch("a", "n", "b", "z"));

        Assert.Multiple(() =>
        {
            Assert.That(batches, Has.Count.EqualTo(1),
                "The in-span entries must commit through one batched append, not one per key.");
            Assert.That(batches.SingleOrDefault(), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "b" }),
                "Out-of-span entries must not be admitted on a leaf whose span excludes them.");
        });
        await writer.DidNotReceive().AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await sibling.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(b => b.Select(e => e.Key).SequenceEqual(new[] { "n", "z" })));
        await sibling.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        await sibling.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>());
    }

    [Test]
    public async Task SetMany_wholly_out_of_span_forwards_without_a_local_append()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (writer, batches) = CreateBatchRecordingWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling, commitLog: writer);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        await grain.SetManyAsync(Batch("n", "z"));

        Assert.That(batches, Is.Empty);
        Assert.That(grain.EntriesForTest, Is.Empty);
        await sibling.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task SetMany_on_a_mid_split_leaf_completes_the_split_once_then_batches_both_sides()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (writer, batches) = CreateBatchRecordingWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling, maxLeafKeys: 4, commitLog: writer);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        batches.Clear();
        writer.ClearReceivedCalls();
        sibling.ClearReceivedCalls();

        var result = await grain.SetManyAsync(Batch("b", "n", "c", "z"));

        Assert.Multiple(() =>
        {
            Assert.That(result?.NewSiblingId, Is.EqualTo(siblingId),
                "Completing the interrupted split must still report it to the shard root.");
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("m"));
            Assert.That(batches, Is.EqualTo(new[] { new[] { "b", "c" } }),
                "The entries staying on the donor must commit through one batched append.");
            Assert.That(grain.EntriesForTest.Keys, Does.Contain("b").And.Contain("c").And.No.Contain("n").And.No.Contain("z"));
        });
        await writer.DidNotReceive().AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await sibling.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(b => b.Select(e => e.Key).SequenceEqual(new[] { "n", "z" })));
        await sibling.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>());
    }

    [Test]
    public async Task SetMany_straddling_the_declared_span_inside_an_atomic_batch_keeps_the_per_key_path()
    {
        // Saga legs keep the per-key loop: their per-key prepared semantics are
        // what the atomic suites prove, and they are not on the bulk path.
        RequestContext.Clear();
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (writer, batches) = CreateBatchRecordingWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling, commitLog: writer);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        try
        {
            using (LatticeAtomicBatchContext.With((3, 0)))
            {
                await grain.SetManyAsync(Batch("a", "n", "z"));
            }
        }
        finally
        {
            RequestContext.Clear();
        }

        Assert.That(batches, Is.Empty);
        await writer.Received(1).AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await sibling.Received(2).SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>());
        await sibling.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }
}
