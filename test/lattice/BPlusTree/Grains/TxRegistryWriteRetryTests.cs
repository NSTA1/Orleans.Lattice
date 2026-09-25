using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TxRegistryWriteRetry"/>, the bounded retry callers
/// wrap saga decision registry mutators in (issue #3501). Only a
/// <see cref="TxRegistryWriteFailedException"/> is retried; everything else, and
/// the final attempt's failure, propagates.
/// </summary>
[TestFixture]
public class TxRegistryWriteRetryTests
{
    private static TxRegistryWriteFailedException WriteFailed() =>
        new("tree-x", new IOException("disk"), conflict: false);

    [Test]
    public void RunAsync_returns_the_first_task_when_it_completed_synchronously()
    {
        var calls = 0;

        var task = TxRegistryWriteRetry.RunAsync(0, _ => { calls++; return Task.CompletedTask; });

        Assert.Multiple(() =>
        {
            Assert.That(task.IsCompletedSuccessfully, Is.True);
            Assert.That(calls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RunAsync_retries_a_registry_write_failure_until_an_attempt_succeeds()
    {
        var calls = 0;

        await TxRegistryWriteRetry.RunAsync(0, _ => ++calls < 3 ? Task.FromException(WriteFailed()) : Task.CompletedTask);

        Assert.That(calls, Is.EqualTo(3));
    }

    [Test]
    public void RunAsync_gives_up_after_the_attempt_bound_and_surfaces_the_last_failure()
    {
        var calls = 0;

        Assert.ThrowsAsync<TxRegistryWriteFailedException>(
            () => TxRegistryWriteRetry.RunAsync(0, _ => { calls++; return Task.FromException(WriteFailed()); }));

        Assert.That(calls, Is.EqualTo(TxRegistryWriteRetry.MaxAttempts));
    }

    [Test]
    public void RunAsync_does_not_retry_any_other_exception()
    {
        var calls = 0;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => TxRegistryWriteRetry.RunAsync(0, _ => { calls++; return Task.FromException(new InvalidOperationException("conflict")); }));

        Assert.That(calls, Is.EqualTo(1), "A decision conflict is a verdict, not a transient write failure.");
    }

    [Test]
    public void RunAsync_passes_the_state_to_every_attempt()
    {
        var seen = new List<string>();

        Assert.ThrowsAsync<TxRegistryWriteFailedException>(
            () => TxRegistryWriteRetry.RunAsync("arg", s => { seen.Add(s); return Task.FromException(WriteFailed()); }));

        Assert.That(seen, Has.Count.EqualTo(TxRegistryWriteRetry.MaxAttempts).And.All.EqualTo("arg"));
    }

    [Test]
    public async Task MarkDecisionAsync_routes_a_commit_and_an_abort_to_the_matching_mutator()
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.MarkCommittedAsync(Arg.Any<Guid>()).Returns(Task.FromException(WriteFailed()), Task.CompletedTask);
        registry.MarkAbortedAsync(Arg.Any<Guid>()).Returns(Task.CompletedTask);
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();

        await TxRegistryWriteRetry.MarkDecisionAsync(registry, committed, committed: true);
        await TxRegistryWriteRetry.MarkDecisionAsync(registry, aborted, committed: false);

        await registry.Received(2).MarkCommittedAsync(committed);
        await registry.Received(1).MarkAbortedAsync(aborted);
        await registry.DidNotReceive().MarkAbortedAsync(committed);
    }
}
