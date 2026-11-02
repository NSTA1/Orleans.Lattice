using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5664 (a) wiring detectors: a leaf write reaches its fenced append carrying the
/// leaf's own admission mark, captured before its span admission. A stamped write
/// replaces any mark its caller sent; the cross-migration backstop, which awaits its
/// forwards between its span check and its local appends, captures one too.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private sealed class OpenGate : IWalAppendFenceGate
    {
        public bool IsOpen => true;
    }

    private static (BPlusLeafGrain Grain, List<WalRoutingStamp> Routings) CreateFencedGrain()
    {
        var routings = new List<WalRoutingStamp>();
        var writer = Substitute.For<IFencedCommitLogWriter>();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<ILeafAppendFence>(), Arg.Any<WalRoutingStamp>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                lock (routings)
                {
                    routings.Add(call.ArgAt<WalRoutingStamp>(2));
                }

                return Task.FromResult(1L);
            });
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<ILeafAppendFence>(), Arg.Any<WalRoutingStamp>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                lock (routings)
                {
                    routings.Add(call.ArgAt<WalRoutingStamp>(2));
                }

                return Task.FromResult<IReadOnlyList<long>>(call.ArgAt<IReadOnlyList<WalRecord>>(0).Select(_ => 1L).ToArray());
            });
        var grain = CreateGrain(
            new FakePersistentState<LeafNodeState>(),
            replicaId: Guid.NewGuid().ToString("N"),
            commitLog: writer,
            fenceGate: new OpenGate());
        return (grain, routings);
    }

    [Test]
    public async Task A_stamped_write_appends_with_the_leafs_own_admission_mark_not_its_callers()
    {
        var (grain, routings) = CreateFencedGrain();
        var fromCaller = WalRoutingStamp.ForShardRoot(0, 3) with { AdmissionMark = 99 };

        await grain.SetStampedAsync("k", [1], 0, fromCaller);

        Assert.That(grain.AppendFenceForTesting, Is.Not.Null, "precondition: the leaf has an append fence");
        Assert.That(routings, Has.Count.EqualTo(1));
        Assert.That(routings[0].AdmissionMark, Is.EqualTo(grain.AppendFenceForTesting!.HandOffCloses + 1),
            "the mark is this activation's close count at admission, never the caller's");
        Assert.That(routings[0].Generation, Is.EqualTo(3), "the shard root's generation travels unchanged");
    }

    [Test]
    public async Task A_backstop_append_carries_an_admission_mark_captured_before_its_span_check()
    {
        var (grain, routings) = CreateFencedGrain();

        await grain.ApplyTxTerminalAsync(Guid.NewGuid(), committed: true, new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["k"] = [1] });

        Assert.That(routings, Has.Count.EqualTo(1), "the backstop is appended through the routed, fenced path");
        Assert.That(routings[0].AdmissionMark, Is.EqualTo(grain.AppendFenceForTesting!.HandOffCloses + 1));
    }
}
