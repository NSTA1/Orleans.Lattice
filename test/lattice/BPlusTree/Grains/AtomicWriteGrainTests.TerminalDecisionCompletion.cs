using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Commit_terminal_waits_for_the_durable_decision_response(bool crossTree)
    {
        var (grain, _, _, shard, registry) = CreateGrainForTerminalOrdering();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durable = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.MarkCommittedAsync(Arg.Any<Guid>()).Returns(_ =>
        {
            entered.TrySetResult();
            return durable.Task;
        });

        if (crossTree)
        {
            var vote = await grain.PrepareForCoordinatorAsync(
                TreeId, MakeEntries(("k", [1])), predicate: null,
                coordinatorKey: "decision-completion", participants: [TreeId]);
            Assert.That(vote, Is.EqualTo(CrossTreePrepareVote.Prepared));
        }

        var execution = crossTree
            ? grain.FinalizeAsync(commit: true)
            : grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(durable.Task.IsCompleted, Is.False);
            Assert.That(execution.IsCompleted, Is.False,
                "Invoking the decision RPC is not its durable completion.");
            await shard.DidNotReceive().AppendTxTerminalAsync(
                Arg.Any<Guid>(), Arg.Is(true),
                Arg.Any<IReadOnlyDictionary<string, byte[]>?>(),
                Arg.Any<CancellationToken>(), Arg.Any<bool>());
        }
        finally
        {
            durable.TrySetResult();
            await execution.WaitAsync(TimeSpan.FromSeconds(5));
        }

        await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await AssertTerminalBroadcastReceivedOnce(shard, committed: true);
    }
}
