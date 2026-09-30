using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// A <see cref="DurableVectorIndexOptions.IngestSliceBudget"/> longer than a timer
/// can wait must still bound a slice rather than fault it.
/// <para>
/// The slice deadline is armed with <see cref="TimeProvider.CreateTimer"/> when the
/// slice first has to wait for its source, and the system timer refuses a due time
/// above <c>0xFFFFFFFE</c> milliseconds (about 49.7 days) with
/// <see cref="ArgumentOutOfRangeException"/>. The budget itself has no upper bound -
/// the repository-context host reads it from an environment variable that accepts any
/// value <see cref="TimeSpan"/> can hold - so a budget meant as "effectively
/// unbounded" used to fault every slice that waited, and the build never advanced.
/// The source here answers asynchronously, so every slice takes the waiting path and
/// arms the deadline; the work count, not the clock, ends each slice.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexSliceBudgetTimerCeilingTests
{
    private const int Corpus = 40;
    private const int BatchSize = 16;

    private static readonly TimeSpan TimerCeiling = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private static IEnumerable<TimeSpan> BudgetsAboveTheTimerCeiling()
    {
        yield return TimerCeiling + TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromDays(60);
        yield return TimeSpan.MaxValue;
    }

    private static DeferredVectorSource DeferredSource()
    {
        var corpus = VectorCorpus.Clustered(Corpus, DurableIndexHarness.Dimensions, 4, seed: 11);
        var source = new DeferredVectorSource(DurableIndexHarness.Dimensions);
        for (var i = 0; i < Corpus; i++)
        {
            source.Set(DurableIndexHarness.Id(i), corpus[i]);
        }

        return source;
    }

    [TestCaseSource(nameof(BudgetsAboveTheTimerCeiling))]
    public async Task A_slice_budget_above_the_timer_ceiling_still_ingests(TimeSpan budget)
    {
        Assert.That(
            () => TimeProvider.System.CreateTimer(static _ => { }, null, budget, Timeout.InfiniteTimeSpan).Dispose(),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "anti-vacuity: the budget is one the system timer itself refuses");

        var options = DurableIndexHarness.Options(ingestBatchSize: BatchSize);
        options.IngestSliceBudget = budget;
        options.TimeProvider = TimeProvider.System;
        var index = await DurableVectorIndex.OpenAsync(
            new InMemoryVectorIndexStore(), DeferredSource(), options, VectorIndexLoadMode.Full);

        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));

        Assert.That(
            async () => await index.BuildStepAsync(),
            Throws.Nothing,
            "a slice that has to wait arms its deadline, and a budget the timer cannot hold must be "
            + "clamped to the longest wait it can rather than faulting the slice");

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(BatchSize),
                "the work count ends the slice, because the clamped deadline is weeks away");
            Assert.That(index.Progress.SlicesDeadlined, Is.Zero);
        });
    }
}
