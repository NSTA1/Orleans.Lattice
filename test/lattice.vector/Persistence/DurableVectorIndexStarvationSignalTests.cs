using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The starvation verdict as a claim about NOW: that it is raised when the source
/// stops answering, and - the half that was missing - that it is RETRACTED when
/// the source starts answering again.
/// <para>
/// <see cref="DurableVectorIndexStalledSourceTests"/> covers both VALUES of
/// <see cref="VectorIndexBuildProgress.IsStarvedBySource"/> and still could not
/// see this defect, which is the point worth keeping. Every fixture there drives
/// a source whose regime is fixed for the life of the test, so each one observes
/// the predicate in a single steady state. A verdict that is correct in both
/// steady states and cannot move between them passes all of them.
/// </para>
/// <para>
/// The predicate used to be assembled out of two LIFETIME counters that are never
/// reset:
/// <c>SlicesDeadlined &gt; 0 &amp;&amp; SlicesDeadlinedWithoutProgress == SlicesDeadlined</c>.
/// A slice that completes inside its budget increments neither, so an advancing
/// build could not move that equality at all - the sole event that could was a
/// future slice that was both deadlined AND productive. Both directions were
/// therefore stuck, and each fixture below pins one of them.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexStarvationSignalTests
{
    private const int Corpus = 20;

    /// <summary>How many pages the fixture hands over before the fetch that never returns.</summary>
    private const int PageSize = 4;

    /// <summary>
    /// A <see cref="StallingVectorSource.PagesBeforeStall"/> the corpus cannot
    /// reach, which is how the fixture turns the stall OFF without swapping the
    /// source for a different instance and losing the index's cursor with it.
    /// </summary>
    private const int NeverStalls = 1_000;

    /// <summary>
    /// Long enough that a page of in-memory vectors is never mistaken for a slow
    /// source, short enough that a wedged fixture fails quickly.
    /// </summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The bound the fixture itself relies on, so a regression presents as a
    /// failing assertion rather than as a hung run.
    /// </summary>
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    private static DurableVectorIndexOptions Options()
    {
        // The real clock, because the deadline exercised here is a TIMER and not a
        // sampled reading: a fake clock that only moves when it is read would leave
        // the deadline permanently unarmed.
        var options = DurableIndexHarness.Options(ingestBatchSize: 4_096, maxItemsPerChunk: 64);
        options.IngestSliceBudget = Budget;
        options.TimeProvider = TimeProvider.System;
        return options;
    }

    private static StallingVectorSource Stalling(int pagesBeforeStall)
    {
        var corpus = VectorCorpus.Clustered(Corpus, DurableIndexHarness.Dimensions, 4, seed: 11);
        var source = new StallingVectorSource(DurableIndexHarness.Dimensions, PageSize, pagesBeforeStall);
        for (var i = 0; i < Corpus; i++)
        {
            source.Set(DurableIndexHarness.Id(i), corpus[i]);
        }

        return source;
    }

    private static async Task WithinGuardAsync(Task work, string because)
    {
        var finished = await Task.WhenAny(work, Task.Delay(Guard));
        Assert.That(finished, Is.SameAs(work), because);
        await work;
    }

    /// <summary>
    /// Advances the build past the NotStarted transition, which consumes nothing,
    /// so that the next step is an ingest slice.
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
    public async Task A_starvation_verdict_is_retracted_once_the_source_answers_again()
    {
        // The measured shape. A repository-context plane took a run of empty
        // deadlines while its embedder was cold, then ingested tens of thousands of
        // vectors without another deadline of any kind - and went on logging "the
        // build is not advancing" and "raising the budget will not help" on every
        // tick, beside a corpus figure in the same sentence climbing a hundred
        // vectors a tick.
        var store = new InMemoryVectorIndexStore();
        var source = Stalling(pagesBeforeStall: 0);
        var index = await IngestingAsync(store, source, Options());

        await WithinGuardAsync(index.BuildStepAsync(), "the deadline must end the slice that read nothing");

        var starved = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(starved.VectorsIndexed, Is.Zero, "the source yielded nothing to bank");
            Assert.That(starved.IsStarvedBySource, Is.True,
                "a build whose only slice was deadlined empty-handed IS starved - this is the positive "
                + "control, and without it the retraction below could be bought by a predicate that is "
                + "simply never true");
            Assert.That(starved.EmptyDeadlinesSinceLastAdvance, Is.EqualTo(1));
        });

        // The source starts answering. Nothing else changes: same index, same
        // cursor, same store - only the regime.
        source.PagesBeforeStall = NeverStalls;
        await WithinGuardAsync(index.BuildStepAsync(), "a source that answers must not be deadlined");

        var advanced = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(advanced.VectorsIndexed, Is.EqualTo(Corpus),
                "the fixture proves nothing unless the build really did advance");
            Assert.That(advanced.EmptyDeadlinesSinceLastAdvance, Is.Zero,
                "banking clears the count the verdict rests on");
            Assert.That(advanced.IsStarvedBySource, Is.False,
                "and so the verdict is retracted; before this fix a slice that completed inside its budget "
                + "incremented neither lifetime counter, so an advancing build could not move the old "
                + "equality and went on reporting starvation for the rest of its life");

            // The lifetime pair is deliberately NOT reset, and this is what keeps
            // the fix from being bought by quietly discarding a figure somebody
            // else reads. They remain the post-mortem totals they always were; the
            // verdict simply no longer consults them.
            Assert.That(advanced.SlicesDeadlined, Is.EqualTo(1),
                "the lifetime total still records the deadline that fired");
            Assert.That(advanced.SlicesDeadlinedWithoutProgress, Is.EqualTo(1),
                "and still records that it banked nothing");
        });
    }

    [Test]
    public async Task A_build_that_wedges_after_a_productive_deadline_is_still_reported_as_starved()
    {
        // The mirror-image failure, and the reason the old predicate could not
        // simply be given an extra conjunct. One deadlined-but-PRODUCTIVE slice
        // broke the equality permanently, so a build that wedged solid afterwards
        // reported starvation never - silently, and in the direction that costs an
        // operator the most.
        var store = new InMemoryVectorIndexStore();
        var source = Stalling(pagesBeforeStall: 1);
        var index = await IngestingAsync(store, source, Options());

        await WithinGuardAsync(index.BuildStepAsync(), "the first slice must end on its deadline");

        var productive = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(productive.VectorsIndexed, Is.EqualTo(PageSize),
                "one page arrived before the stall, so this deadline was productive");
            Assert.That(productive.SlicesDeadlined, Is.EqualTo(1));
            Assert.That(productive.SlicesDeadlinedWithoutProgress, Is.Zero);
            Assert.That(productive.IsStarvedBySource, Is.False, "a build that is advancing is not starved");
        });

        // Now the source stops delivering at all, which is the state a contended
        // shard root puts it in.
        source.PagesBeforeStall = 0;
        await WithinGuardAsync(index.BuildStepAsync(), "the wedged slice must end on its deadline");

        var wedged = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(wedged.VectorsIndexed, Is.EqualTo(PageSize),
                "nothing was banked by the wedged slice, which is what makes it a wedge");
            Assert.That(wedged.EmptyDeadlinesSinceLastAdvance, Is.EqualTo(1));
            Assert.That(wedged.IsStarvedBySource, Is.True,
                "the build is now blocked on a read that does not complete and must say so; the old "
                + "predicate compared 2 deadlines against 1 empty one and reported nothing at all");
            Assert.That(wedged.SlicesDeadlined, Is.EqualTo(2),
                "both deadlines are still counted for the lifetime record");
            Assert.That(wedged.SlicesDeadlinedWithoutProgress, Is.EqualTo(1),
                "of which one banked nothing - the very inequality that used to suppress the verdict");
        });
    }

    [Test]
    public async Task Empty_deadlines_accumulate_while_the_source_stays_silent()
    {
        // The counterweight for the retraction. A verdict that clears too eagerly
        // would be worse than one that sticks, because the wedge it exists to
        // report would flicker instead of standing still - so pin that consecutive
        // empty deadlines accumulate rather than resetting on anything other than
        // banked work.
        var store = new InMemoryVectorIndexStore();
        var source = Stalling(pagesBeforeStall: 0);
        var index = await IngestingAsync(store, source, Options());

        for (var step = 0; step < 3; step++)
        {
            await WithinGuardAsync(index.BuildStepAsync(), "every silent slice must end on its own deadline");
        }

        var progress = index.Progress;
        Assert.Multiple(() =>
        {
            Assert.That(progress.VectorsIndexed, Is.Zero, "the source never yielded");
            Assert.That(progress.EmptyDeadlinesSinceLastAdvance, Is.EqualTo(3),
                "nothing was banked, so nothing cleared the run");
            Assert.That(progress.IsStarvedBySource, Is.True);
        });
    }
}
