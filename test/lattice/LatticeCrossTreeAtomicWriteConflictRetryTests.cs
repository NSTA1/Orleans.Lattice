using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the bounded conflict re-attach in
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>
/// (issue #3572): a saga state-write conflict re-attaches by operation id on a
/// fresh activation; any other fault propagates at once.
/// </summary>
[TestFixture]
public sealed class LatticeCrossTreeAtomicWriteConflictRetryTests
{
    private static LatticeStateWriteFailedException Conflict() =>
        new("cross-tree-tx", "op-1", new InconsistentStateException("etag"), conflict: true);

    [Test]
    public async Task CommitReattachingOnConflictAsync_reattaches_after_a_conflict()
    {
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.CommitAsync(Arg.Any<List<LatticeTreeBatch>>())
            .Returns(
                _ => Task.FromException<CrossTreeAtomicWriteOutcome>(Conflict()),
                _ => Task.FromResult(CrossTreeAtomicWriteOutcome.Committed));

        var outcome = await LatticeCrossTreeAtomicWriteExtensions.CommitReattachingOnConflictAsync(
            coordinator, [], CancellationToken.None, static _ => TimeSpan.Zero);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await coordinator.Received(2).CommitAsync(Arg.Any<List<LatticeTreeBatch>>());
    }

    [Test]
    public async Task CommitReattachingOnConflictAsync_gives_up_after_the_attempt_budget()
    {
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.CommitAsync(Arg.Any<List<LatticeTreeBatch>>()).ThrowsAsync(Conflict());

        var ex = Assert.CatchAsync(() => LatticeCrossTreeAtomicWriteExtensions.CommitReattachingOnConflictAsync(
            coordinator, [], CancellationToken.None, static _ => TimeSpan.Zero));

        Assert.That(ex, Is.TypeOf<LatticeStateWriteFailedException>());
        await coordinator.Received(LatticeCrossTreeAtomicWriteExtensions.MaxConflictAttempts)
            .CommitAsync(Arg.Any<List<LatticeTreeBatch>>());
    }

    [Test]
    public async Task CommitReattachingOnConflictAsync_does_not_retry_other_faults()
    {
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.CommitAsync(Arg.Any<List<LatticeTreeBatch>>())
            .ThrowsAsync(new InvalidOperationException("rolled back"));

        Assert.ThrowsAsync<InvalidOperationException>(() => LatticeCrossTreeAtomicWriteExtensions.CommitReattachingOnConflictAsync(
            coordinator, [], CancellationToken.None, static _ => TimeSpan.Zero));

        await coordinator.Received(1).CommitAsync(Arg.Any<List<LatticeTreeBatch>>());
    }

    [Test]
    public async Task SetManyAtomicAsync_reattaches_to_the_same_coordinator_after_a_conflict()
    {
        var factory = Substitute.For<IGrainFactory>();
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        factory.GetGrain<ILatticeCrossTreeTxGrain>("op-1").Returns(coordinator);
        coordinator.CommitAsync(Arg.Any<List<LatticeTreeBatch>>())
            .Returns(
                _ => Task.FromException<CrossTreeAtomicWriteOutcome>(Conflict()),
                _ => Task.FromResult(CrossTreeAtomicWriteOutcome.Committed));

        var outcome = await factory.SetManyAtomicAsync([], "op-1");

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await coordinator.Received(2).CommitAsync(Arg.Any<List<LatticeTreeBatch>>());
    }
}
