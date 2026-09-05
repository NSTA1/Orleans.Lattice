using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins how <c>DeadLetterTrackingReplicationApplier.ApplyBatchAsync</c> folds the
/// per-entry results it collects on its slow path into a single
/// <see cref="ApplyResult"/>.
/// <para>
/// The fold is not a formality. <see cref="ApplyResult.Deferred"/> is how the
/// causal-apply buffer tells the caller that at least one entry was parked
/// awaiting its dependencies, and it must survive the batch aggregation: a batch
/// that silently drops the flag reports itself fully applied while entries are
/// still parked, so the caller advances past a gap that was never closed.
/// Likewise <see cref="ApplyResult.HighWaterMark"/> must fold to the
/// <em>maximum</em> observed clock, not the last one, because the slow path
/// visits entries in batch order rather than clock order.
/// </para>
/// <para>
/// These arms are only reachable through the per-entry slow path, which the
/// fast-path fixtures deliberately avoid, so they are exercised here by forcing
/// the inner batch call to throw.
/// </para>
/// </summary>
public partial class DeadLetterTrackingReplicationApplierTests
{
    private static WalRecord EntryAt(string key, long ticks) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        OriginClusterId = "site-b",
    };

    [Test]
    public async Task ApplyBatchAsync_slow_path_propagates_a_deferred_entry_to_the_batch_result()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var parked = EntryAt("a", 10);
        var applied = EntryAt("b", 20);

        // Force the per-entry slow path: the inner batch call throws, so the
        // decorator re-establishes accounting entry by entry.
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch boom"));

        inner.ApplyAsync(parked, Arg.Any<CancellationToken>())
            .Returns(new ApplyResult { Applied = false, Deferred = true, HighWaterMark = HybridLogicalClock.Zero });
        inner.ApplyAsync(applied, Arg.Any<CancellationToken>())
            .Returns(new ApplyResult { Applied = true, HighWaterMark = applied.Timestamp });

        var result = await decorator.ApplyBatchAsync([parked, applied], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Deferred, Is.True,
                "dropping the deferred flag would let the caller advance past an entry still parked on its dependencies");
            Assert.That(result.Applied, Is.True, "the sibling entry did apply, so the batch reports progress");
            Assert.That(result.HighWaterMark, Is.EqualTo(applied.Timestamp));
        });
    }

    [Test]
    public async Task ApplyBatchAsync_slow_path_reports_not_deferred_when_every_entry_applied()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var first = EntryAt("a", 10);
        var second = EntryAt("b", 20);

        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch boom"));
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new ApplyResult
            {
                Applied = true,
                HighWaterMark = callInfo.Arg<WalRecord>().Timestamp,
            }));

        var result = await decorator.ApplyBatchAsync([first, second], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Deferred, Is.False);
            Assert.That(result.Applied, Is.True);
        });
    }

    [Test]
    public async Task ApplyBatchAsync_slow_path_folds_the_high_water_mark_to_the_maximum_not_the_last()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var highest = EntryAt("a", 900);
        var lower = EntryAt("b", 100);

        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch boom"));
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(new ApplyResult
            {
                Applied = true,
                HighWaterMark = callInfo.Arg<WalRecord>().Timestamp,
            }));

        // Batch order deliberately puts the highest clock first, so a fold that
        // simply keeps the last result would report the lower clock.
        var result = await decorator.ApplyBatchAsync([highest, lower], CancellationToken.None);

        Assert.That(result.HighWaterMark, Is.EqualTo(highest.Timestamp),
            "regressing the high-water-mark would re-admit already-applied entries on the next pass");
    }

    [Test]
    public async Task ApplyBatchAsync_slow_path_reports_not_applied_when_every_entry_is_deferred()
    {
        var (decorator, inner, _, _, _) = Build(maxRetries: 3);
        var first = EntryAt("a", 10);
        var second = EntryAt("b", 20);

        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch boom"));
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(new ApplyResult { Applied = false, Deferred = true, HighWaterMark = HybridLogicalClock.Zero });

        var result = await decorator.ApplyBatchAsync([first, second], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.Deferred, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
                "an all-parked batch must not advance the cursor past entries that were never applied");
        });
    }
}
