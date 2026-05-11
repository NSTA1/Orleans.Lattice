using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for <c>DeadLetterTrackingReplicationApplier.ApplyBatchAsync</c>'s three
/// execution paths: empty-batch short-circuit, fast-path delegation when no entry
/// has prior retry history, and per-entry slow-path fallback when retry history
/// exists or when the inner applier's batch path throws.
/// </summary>
public partial class DeadLetterTrackingReplicationApplierTests
{
    [Test]
    public async Task ApplyBatchAsync_empty_batch_returns_not_applied_without_calling_inner()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);

        var result = await decorator.ApplyBatchAsync(Array.Empty<WalRecord>(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await inner.DidNotReceiveWithAnyArgs().ApplyBatchAsync(default!, default);
        await inner.DidNotReceiveWithAnyArgs().ApplyAsync(default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_fast_path_delegates_entire_batch_to_inner_when_no_retry_history()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var hwm = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var expected = new ApplyResult { Applied = true, HighWaterMark = hwm };
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var entries = new[] { MakeEntry("a"), MakeEntry("b") };
        var result = await decorator.ApplyBatchAsync(entries, CancellationToken.None);

        Assert.That(result, Is.EqualTo(expected));
        await inner.Received(1).ApplyBatchAsync(
            Arg.Is<IReadOnlyList<WalRecord>>(l => l.Count == 2),
            Arg.Any<CancellationToken>());
        await inner.DidNotReceiveWithAnyArgs().ApplyAsync(default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_single_entry_fast_paths_through_per_entry_decorator()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var expected = new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Tick(HybridLogicalClock.Zero) };
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await decorator.ApplyBatchAsync(new[] { MakeEntry("a") }, CancellationToken.None);

        Assert.That(result, Is.EqualTo(expected));
        await inner.Received(1).ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await inner.DidNotReceiveWithAnyArgs().ApplyBatchAsync(default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_slow_path_routes_per_entry_when_an_entry_has_retry_history()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var poisoned = MakeEntry("a");

        // Pre-poison: induce a single failure on entry "a" so its retry counter is non-zero.
        inner.ApplyAsync(poisoned, Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("boom"));
        Assert.That(
            async () => await decorator.ApplyAsync(poisoned, CancellationToken.None),
            Throws.InvalidOperationException);

        // Reset inner: succeed for both entries on the next pass.
        inner.ClearReceivedCalls();
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new ApplyResult
            {
                Applied = true,
                HighWaterMark = callInfo.Arg<WalRecord>().Timestamp,
            }));

        var entries = new[] { poisoned, MakeEntry("b") };
        var result = await decorator.ApplyBatchAsync(entries, CancellationToken.None);

        Assert.That(result.Applied, Is.True);
        await inner.Received(2).ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await inner.DidNotReceiveWithAnyArgs().ApplyBatchAsync(default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_falls_back_to_per_entry_when_inner_batch_throws()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch-boom"));
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new ApplyResult
            {
                Applied = true,
                HighWaterMark = callInfo.Arg<WalRecord>().Timestamp,
            }));

        var entries = new[] { MakeEntry("a"), MakeEntry("b") };
        var result = await decorator.ApplyBatchAsync(entries, CancellationToken.None);

        Assert.That(result.Applied, Is.True);
        await inner.Received(1).ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        await inner.Received(2).ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void ApplyBatchAsync_propagates_inner_batch_cancellation_without_falling_through()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new OperationCanceledException());

        var entries = new[] { MakeEntry("a"), MakeEntry("b") };

        Assert.That(
            async () => await decorator.ApplyBatchAsync(entries, CancellationToken.None),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ApplyBatchAsync_throws_on_pre_cancelled_token()
    {
        var (decorator, _, _, _, _) = Build(maxRetries: 3);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await decorator.ApplyBatchAsync(new[] { MakeEntry("a") }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ApplyBatchAsync_throws_on_null_entries()
    {
        var (decorator, _, _, _, _) = Build(maxRetries: 3);

        Assert.That(
            async () => await decorator.ApplyBatchAsync(null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
