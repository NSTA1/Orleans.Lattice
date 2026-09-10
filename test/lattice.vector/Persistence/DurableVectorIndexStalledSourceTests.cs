using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The paired negative for the slice bound: a source that yields NOTHING (#2536).
/// <para>
/// <see cref="DurableVectorIndexSliceBudgetTests"/> proves the budget stops a
/// slice that is reading, and <see cref="DurableVectorIndexSliceFaultTests"/>
/// proves a slice that throws banks what it had. Neither can see the case that
/// actually wedged the deployment rig, because both are driven by a source that
/// ANSWERS. A budget sampled only after an item is consumed is not a bound at all
/// when no item is ever consumed, and a checkpoint conditioned on
/// <c>consumed &gt; 0</c> banks nothing for the same reason and from the same
/// cause. The build then re-reads an identical range on every step forever: the
/// indexed count never leaves zero, no cursor advances, and nothing faults in a
/// way that says so.
/// </para>
/// <para>
/// A test whose source yields is structurally incapable of distinguishing a real
/// bound from a bound that is never evaluated, which is why every assertion here
/// is a COUNT - of vectors banked, and of slices deadlined with and without
/// progress - rather than the presence of an exception.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexStalledSourceTests
{
    private const int Corpus = 20;

    /// <summary>
    /// Long enough that a page of in-memory vectors is never mistaken for a slow
    /// source, short enough that a wedged fixture fails quickly.
    /// </summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The bound the fixture itself relies on. A pre-fix build never returns from
    /// a stalled slice at all, so without this the regression presents as a hung
    /// run rather than as a failing assertion.
    /// </summary>
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    private static DurableVectorIndexOptions Options(int maxItemsPerChunk = 64)
    {
        // The real clock, because the deadline this fixture exercises is a TIMER
        // and not a sampled reading: a fake clock that only moves when it is read
        // would leave the deadline permanently unarmed, and the fixture would pass
        // against the very code it exists to fail.
        var options = DurableIndexHarness.Options(
            ingestBatchSize: 4_096, maxItemsPerChunk: maxItemsPerChunk);
        options.IngestSliceBudget = Budget;
        options.TimeProvider = TimeProvider.System;
        return options;
    }

    private static StallingVectorSource Stalling(int pageSize, int pagesBeforeStall)
    {
        var corpus = VectorCorpus.Clustered(Corpus, DurableIndexHarness.Dimensions, 4, seed: 11);
        var source = new StallingVectorSource(DurableIndexHarness.Dimensions, pageSize, pagesBeforeStall);
        for (var i = 0; i < Corpus; i++)
        {
            source.Set(DurableIndexHarness.Id(i), corpus[i]);
        }

        return source;
    }

    private static async Task<T> WithinGuardAsync<T>(Task<T> work, string because)
    {
        var finished = await Task.WhenAny(work, Task.Delay(Guard));
        Assert.That(finished, Is.SameAs(work), because);
        return await work;
    }

    private static async Task WithinGuardAsync(Task work, string because)
    {
        var finished = await Task.WhenAny(work, Task.Delay(Guard));
        Assert.That(finished, Is.SameAs(work), because);
        await work;
    }

    /// <summary>
    /// Advances the build past the NotStarted transition, which consumes nothing,
    /// so the step under test is an ingest slice.
    /// </summary>
    private static async Task<DurableVectorIndex> IngestingAsync(
        InMemoryVectorIndexStore store, StallingVectorSource source, DurableVectorIndexOptions options)
    {
        var index = await DurableVectorIndex.OpenAsync(store, source, options, VectorIndexLoadMode.Full);
        await WithinGuardAsync(index.BuildStepAsync(), "starting a build must not touch the stalled page");
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        return index;
    }

    [Test]
    public async Task A_source_that_never_yields_a_first_item_still_ends_its_slice()
    {
        var store = new InMemoryVectorIndexStore();
        var source = Stalling(pageSize: 4, pagesBeforeStall: 0);
        var index = await IngestingAsync(store, source, Options());

        await WithinGuardAsync(
            index.BuildStepAsync(),
            "a slice whose source never answers must be ended by its own deadline; before #2536 was "
            + "fixed it ran until the caller's timeout, which for the ANN build grain meant holding a "
            + "non-reentrant turn far past the thirty second call timeout");

        var progress = index.Progress;

        Assert.Multiple(() =>
        {
            Assert.That(source.Stalls, Is.EqualTo(1),
                "the fixture proves nothing unless the slice really did reach the stalled fetch");
            Assert.That(progress.VectorsIndexed, Is.Zero,
                "nothing was yielded, so nothing can have been indexed - this pins the arithmetic the "
                + "rest of the fixture rests on");
            Assert.That(progress.SlicesDeadlined, Is.EqualTo(1),
                "the deadline is the only thing that can have ended this slice");
            Assert.That(progress.SlicesDeadlinedWithoutProgress, Is.EqualTo(1),
                "and it ended it having banked nothing, which is the distinction the discriminator exists "
                + "to report");
            Assert.That(progress.IsStarvedBySource, Is.True,
                "every deadlined slice banked nothing, so raising the budget cannot help and the source "
                + "read is what needs fixing");
            Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting),
                "a slice that read nothing is not evidence the corpus ran out");
        });
    }

    [Test]
    public async Task A_slice_stopped_by_its_deadline_banks_every_vector_it_had_already_read()
    {
        var store = new InMemoryVectorIndexStore();

        // Two pages of five arrive, then the fetch that never returns. Ten is the
        // number that separates a fix from a non-fix: the pre-#2536 slice reached
        // the same stall having consumed the same ten, and durably discarded them.
        var source = Stalling(pageSize: 5, pagesBeforeStall: 2);
        var index = await IngestingAsync(store, source, Options());

        await WithinGuardAsync(index.BuildStepAsync(), "the deadline must end the stalled slice");
        var progress = index.Progress;

        Assert.Multiple(() =>
        {
            Assert.That(source.Yielded, Is.EqualTo(10), "the source handed over exactly two pages");
            Assert.That(progress.VectorsIndexed, Is.EqualTo(10),
                "every vector read before the deadline is banked, so the slice composes progress rather "
                + "than discarding it");
            Assert.That(progress.SlicesDeadlined, Is.EqualTo(1));
            Assert.That(progress.SlicesDeadlinedWithoutProgress, Is.Zero,
                "this slice banked, so it is the tune-the-budget case and not the starved case");
            Assert.That(progress.IsStarvedBySource, Is.False,
                "a build that is advancing must never be reported as starved, or the discriminator would "
                + "send the next investigation to the wrong place");
        });
    }

    [Test]
    public async Task The_indexed_count_climbs_across_steps_against_a_permanently_stalling_source()
    {
        // This is the epic's headline number. The source stalls on the second page
        // of EVERY enumeration, so no step can ever finish the corpus and a build
        // that banks nothing is indistinguishable from one that banks everything -
        // unless the count climbs.
        var store = new InMemoryVectorIndexStore();
        var source = Stalling(pageSize: 4, pagesBeforeStall: 1);
        var index = await IngestingAsync(store, source, Options());

        var counts = new List<int>();
        for (var step = 0; step < 3; step++)
        {
            await WithinGuardAsync(index.BuildStepAsync(), "every slice must end on its own deadline");
            counts.Add(index.Progress.VectorsIndexed);
        }

        Assert.Multiple(() =>
        {
            Assert.That(counts, Is.EqualTo(new[] { 4, 8, 12 }),
                "the count must climb by a page a step; staying at zero is the defect #2536 reports and "
                + "climbing once then stopping would mean the cursor was not banked");
            Assert.That(counts, Is.Ordered.Ascending.And.Unique);
            Assert.That(index.Progress.SlicesDeadlined, Is.EqualTo(3));
            Assert.That(index.Progress.SlicesDeadlinedWithoutProgress, Is.Zero);
        });
    }

    [Test]
    public async Task Banked_progress_outlives_the_index_that_banked_it()
    {
        // Climbing in memory would be worth nothing if a restart re-read from the
        // beginning: the deployment fault interrupts builds constantly, so "banked"
        // has to mean durable rather than merely counted.
        //
        // The pages are chunk-aligned deliberately. A checkpoint persists whole
        // chunks, so a fixture whose pages straddle a chunk boundary would measure
        // that pre-existing granularity rather than this change, and would read as
        // a failure of banking when nothing had failed at all.
        var store = new InMemoryVectorIndexStore();
        var options = Options(maxItemsPerChunk: 4);
        var source = Stalling(pageSize: 4, pagesBeforeStall: 1);
        var index = await IngestingAsync(store, source, options);

        await WithinGuardAsync(index.BuildStepAsync(), "the first slice must end on its deadline");
        await WithinGuardAsync(index.BuildStepAsync(), "the second slice must end on its deadline");
        var banked = index.Progress.VectorsIndexed;

        var reopened = await DurableVectorIndex.OpenAsync(
            store, Stalling(pageSize: 4, pagesBeforeStall: 1), options, VectorIndexLoadMode.Full);

        Assert.Multiple(() =>
        {
            Assert.That(banked, Is.EqualTo(8), "two slices of a page each");
            Assert.That(reopened.Progress.VectorsIndexed, Is.EqualTo(banked),
                "a build resumes from banked progress rather than restarting, which is what turns a "
                + "repeatedly interrupted build from wedged into merely slow");
            Assert.That(reopened.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        });

        await WithinGuardAsync(reopened.BuildStepAsync(), "the resumed slice must end on its deadline");
        Assert.That(reopened.Progress.VectorsIndexed, Is.EqualTo(12),
            "and it resumes from the banked cursor rather than re-reading what was already indexed");
    }

    [Test]
    public async Task A_deadlined_slice_is_a_bounded_slice_and_not_a_failed_one()
    {
        // The distinction the ANN build grain depends on: a phase tick that throws
        // is logged as a failure and retried from scratch, where a bounded slice
        // simply returns with less done. Reporting the deadline as a fault would
        // have swapped one wedge for another.
        var store = new InMemoryVectorIndexStore();
        var source = Stalling(pageSize: 4, pagesBeforeStall: 1);
        var index = await IngestingAsync(store, source, Options());

        Assert.That(
            async () => await index.BuildStepAsync(),
            Throws.Nothing,
            "a spent deadline is the budget working, not a fault");

        Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(4),
            "and it still banks, so 'did not throw' is not being bought with 'did not progress'");
    }

    [Test]
    public async Task A_build_whose_source_answers_is_never_reported_as_starved()
    {
        // The discriminator's false-positive guard. It is read by an operator to
        // decide whether to tune a budget or to go and fix a source read, so a
        // healthy build that reported starvation would be worse than no signal.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options(ingestBatchSize: 8);
        options.IngestSliceBudget = Budget;
        options.TimeProvider = TimeProvider.System;

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(8));
            Assert.That(index.Progress.SlicesDeadlined, Is.Zero,
                "a source that answers ends its slice on the work count, never on the deadline");
            Assert.That(index.Progress.IsStarvedBySource, Is.False);
        });
    }
}
