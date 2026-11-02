using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the coordinator side of the coordinator-terminal stamp class (epic
/// #5605, member 2): a saga coordinator stamps its terminals with the class while the
/// fence gate is open, appends them unstamped otherwise, and falls back to unstamped
/// for a backoff after a partition on an older-build silo answers "method unknown".
/// </summary>
[TestFixture]
public class CoordinatorTerminalAppendFenceTests
{
    private sealed class SwitchableGate : IWalAppendFenceGate
    {
        public bool IsOpen { get; set; } = true;
    }

    private static WalRecord Terminal() => new()
    {
        TreeId = "tree",
        Op = MutationKind.TxCommit,
        Key = "0",
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        TransactionId = Guid.NewGuid(),
    };

    [Test]
    public async Task Stamps_the_terminal_class_while_the_gate_is_open_and_nothing_when_closed()
    {
        var gate = new SwitchableGate();
        var fence = new CoordinatorTerminalAppendFence(gate);
        var shard = Substitute.For<IWalShardGrain>();

        var open = await fence.StampForAsync(shard, 0, CancellationToken.None);
        gate.IsOpen = false;
        var closed = await fence.StampForAsync(shard, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(open, Is.EqualTo(WalAppendStamp.CoordinatorTerminal));
            Assert.That(open!.Value.IsCoordinatorTerminal, Is.True);
            Assert.That(closed, Is.Null, "a closed gate appends through the existing, unstamped method");
        });
    }

    [Test]
    public async Task A_method_unknown_failure_closes_the_fence_view_for_the_backoff()
    {
        var fence = new CoordinatorTerminalAppendFence(new SwitchableGate());

        fence.OnStampedMethodUnknown();
        var stamp = await fence.StampForAsync(Substitute.For<IWalShardGrain>(), 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(fence.StampsForTesting, Is.False);
            Assert.That(stamp, Is.Null);
        });
    }

    [Test]
    public async Task Terminals_go_through_the_fenced_writer_when_a_fence_is_resolved()
    {
        var fenced = Substitute.For<IFencedCommitLogWriter>();
        var fence = new CoordinatorTerminalAppendFence(new SwitchableGate());
        var terminals = new[] { Terminal() };

        await CoordinatorTerminalAppendFence.AppendAsync(fenced, terminals[0], fence, CancellationToken.None);
        await CoordinatorTerminalAppendFence.AppendManyAsync(fenced, terminals, fence, CancellationToken.None);

        await fenced.Received(1).AppendAsync(terminals[0], fence, Arg.Any<CancellationToken>());
        await fenced.Received(1).AppendManyAsync(terminals, fence, Arg.Any<CancellationToken>());
        await fenced.DidNotReceive().AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await fenced.DidNotReceive().AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Terminals_go_unstamped_without_a_fence_or_a_fencing_writer()
    {
        var fenced = Substitute.For<IFencedCommitLogWriter>();
        var plain = Substitute.For<ICommitLogWriter>();
        var fence = new CoordinatorTerminalAppendFence(new SwitchableGate());
        var terminals = new[] { Terminal() };

        await CoordinatorTerminalAppendFence.AppendManyAsync(fenced, terminals, fence: null, CancellationToken.None);
        await CoordinatorTerminalAppendFence.AppendAsync(plain, terminals[0], fence, CancellationToken.None);

        await fenced.Received(1).AppendManyAsync(terminals, Arg.Any<CancellationToken>());
        await plain.Received(1).AppendAsync(terminals[0], Arg.Any<CancellationToken>());
    }

    [Test]
    public void Resolve_returns_null_without_a_fence_gate()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CoordinatorTerminalAppendFence.Resolve(null), Is.Null);
            Assert.That(CoordinatorTerminalAppendFence.Resolve(
                new Microsoft.Extensions.DependencyInjection.ServiceCollection().BuildServiceProvider()), Is.Null);
        });
    }
}
