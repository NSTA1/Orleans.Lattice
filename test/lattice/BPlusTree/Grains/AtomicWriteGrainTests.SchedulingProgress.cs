using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task ExecuteAsync_decision_is_not_caller_completion_until_terminals_and_checkpoint_finish()
    {
        var (grain, state, _, shard, registry) = CreateGrainForTerminalOrdering();
        var terminal = new TaskCompletionSource<WalRecord?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(),
                Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns(_ =>
            {
                entered.TrySetResult();
                return terminal.Task;
            });

        var caller = grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
            Assert.Multiple(() =>
            {
                Assert.That(caller.IsCompleted, Is.False, "a durable decision is not terminal delivery");
                Assert.That(state.State.Phase, Is.Not.EqualTo(AtomicWritePhase.Completed));
            });
        }
        finally
        {
            terminal.TrySetResult(null);
        }
        await caller.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await registry.Received(1).ForgetAsync(Arg.Any<Guid>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ExecuteAsync_original_caller_waits_for_completion_checkpoint_durability(bool failWrite)
    {
        var (grain, state, _, shard, registry) = CreateGrainForTerminalOrdering();
        var checkpoint = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durableCompletion = false;
        state.BeforeWrite = () =>
        {
            if (state.State.Phase != AtomicWritePhase.Completed)
                return Task.CompletedTask;
            entered.TrySetResult();
            return checkpoint.Task;
        };
        state.OnWriteState = s => durableCompletion |= s.Phase == AtomicWritePhase.Completed;
        var caller = grain.ExecuteAsync(TreeId, MakeEntries(("k", [1])));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var fault = new InvalidOperationException("completion storage unavailable");
        try
        {
            await registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
            await AssertTerminalBroadcastReceivedOnce(shard, committed: true);
            Assert.Multiple(() =>
            {
                Assert.That(durableCompletion, Is.False, "the in-memory Completed phase is not durable success");
                Assert.That(caller.IsCompleted, Is.False,
                    "[CallerOnlyAfterTerminals] the original caller must also await completion durability");
            });
            await registry.DidNotReceive().ForgetAsync(Arg.Any<Guid>());
        }
        finally
        {
            if (failWrite) checkpoint.TrySetException(fault);
            else checkpoint.TrySetResult();
        }

        if (failWrite)
        {
            var caught = Assert.ThrowsAsync<InvalidOperationException>(
                () => caller.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.That(caught, Is.SameAs(fault));
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Execute));
            Assert.That(durableCompletion, Is.False);
            await registry.DidNotReceive().ForgetAsync(Arg.Any<Guid>());
        }
        else
        {
            await caller.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(durableCompletion, Is.True);
            await registry.Received(1).ForgetAsync(Arg.Any<Guid>());
        }
    }

}
