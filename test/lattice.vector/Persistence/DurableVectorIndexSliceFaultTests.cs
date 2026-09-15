using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// What an ingest slice keeps when the source faults part way through it.
/// <para>
/// <see cref="DurableVectorIndexSliceBudgetTests"/> pins the bounds a slice
/// chooses to stop at. These pin the exit it does not choose. The two are easily
/// conflated, and conflating them is what produced issue #2536:
/// <see cref="DurableVectorIndexOptions.IngestSliceBudget"/> documents that a
/// step "always makes progress" because the budget is checked only after an item
/// has been consumed, which is true of the BUDGET and says nothing about a slice
/// that faults. A fault propagated out of the enumeration before the checkpoint,
/// so every item the slice had already consumed was durably discarded.
/// </para>
/// <para>
/// That is not a rare window. A source streaming over a remote store of record
/// reaches it through a grain call, and a grain call against a contended
/// non-reentrant shard root can exceed the cluster's response timeout while
/// merely queued. The same range then fails on the next step for the same
/// reason, so the build re-read a range it could not get past and banked nothing
/// on every attempt. The measured deployment took over four hours to reach Ready
/// and answered none of thirteen searches from the approximate plane in the
/// meantime.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexSliceFaultTests
{
    private const int Corpus = 300;
    private const int PageSize = 64;
    private const int PagesBeforeFault = 2;
    private const int ConsumedBeforeFault = PageSize * PagesBeforeFault;

    /// <summary>
    /// A work budget far larger than the corpus, so the batch count cannot end a
    /// slice before the fault does and the fixture measures only the fault.
    /// </summary>
    private static DurableVectorIndexOptions Options() =>
        DurableIndexHarness.Options(ingestBatchSize: 4_096);

    private static PageFaultingVectorSource Source(int maxFaults)
    {
        var corpus = VectorCorpus.Clustered(Corpus, DurableIndexHarness.Dimensions, clusters: 8, seed: 11);
        var source = new PageFaultingVectorSource(
            DurableIndexHarness.Dimensions, PageSize, PagesBeforeFault, maxFaults);
        for (var i = 0; i < Corpus; i++)
        {
            source.Set(DurableIndexHarness.Id(i), corpus[i]);
        }

        return source;
    }

    private static Task<DurableVectorIndex> OpenAsync(
        InMemoryVectorIndexStore store, PageFaultingVectorSource source, DurableVectorIndexOptions options) =>
        DurableVectorIndex.OpenAsync(store, source, options, VectorIndexLoadMode.Full);

    private static async Task<DurableVectorIndex> FaultedIngestAsync(
        InMemoryVectorIndexStore store, PageFaultingVectorSource source, DurableVectorIndexOptions options)
    {
        var index = await OpenAsync(store, source, options);

        // The first step is the NotStarted transition and consumes nothing; the
        // second is the ingest slice that meets the failing page fetch.
        await index.BuildStepAsync();
        Assert.That(async () => await index.BuildStepAsync(), Throws.TypeOf<TimeoutException>(),
            "The fault must still reach the caller. Banking the slice's work makes a repeated failure "
            + "cheap; it must not make it silent, or the coordinator stops counting a build that is "
            + "not progressing.");

        return index;
    }

    [Test]
    public async Task A_slice_whose_source_faults_part_way_banks_the_items_it_already_consumed()
    {
        var store = new InMemoryVectorIndexStore();
        var source = Source(maxFaults: 1);
        var options = Options();

        var index = await FaultedIngestAsync(store, source, options);

        // Reading the store through a second index is the whole assertion. The
        // first index still holds the consumed vectors in memory whether or not
        // they were banked, so an in-memory count cannot tell the two apart - and
        // in the deployment the in-memory copy is exactly what a process restart
        // loses.
        var resumed = await OpenAsync(store, Source(maxFaults: 0), options);

        Assert.Multiple(() =>
        {
            Assert.That(source.FaultsRaised, Is.EqualTo(1),
                "The fixture proves nothing unless the page fetch it asked to fail actually failed.");
            Assert.That(index.Count, Is.EqualTo(ConsumedBeforeFault),
                "The faulted slice consumed two whole pages before the third fetch failed.");
            Assert.That(resumed.Count, Is.EqualTo(ConsumedBeforeFault),
                "A slice that faults must leave its consumed items durable. Discarding them is what "
                + "let a build re-read the same contended range forever without banking a byte.");
        });
    }

    [Test]
    public async Task A_build_whose_source_faults_once_still_converges_without_re_reading_banked_work()
    {
        var store = new InMemoryVectorIndexStore();
        var options = Options();

        await FaultedIngestAsync(store, Source(maxFaults: 1), options);

        // A healthy source, so the resumed build is bounded only by what the
        // faulted one left behind.
        var healthy = Source(maxFaults: 0);
        var resumed = await OpenAsync(store, healthy, options);
        await resumed.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(resumed.Count, Is.EqualTo(Corpus),
                "Resuming across a fault must lose nothing and duplicate nothing.");
            Assert.That(healthy.Yielded, Is.EqualTo(Corpus - ConsumedBeforeFault),
                "The resumed build must read only the remainder. Re-reading the banked prefix would "
                + "mean the cursor was not banked with it, which is the same defect wearing a "
                + "correct-looking count.");
        });

        var hits = DurableIndexHarness.SearchIds(resumed, healthy[DurableIndexHarness.Id(7)], 1);
        Assert.That(hits, Does.Contain(DurableIndexHarness.Id(7)),
            "An index built across a faulted slice answers exactly as one built without it.");
    }

    [Test]
    public async Task A_faulted_slice_never_reports_the_corpus_exhausted()
    {
        var store = new InMemoryVectorIndexStore();
        var options = Options();

        var index = await FaultedIngestAsync(store, Source(maxFaults: 1), options);

        // A fault is the one exit that carries no evidence the corpus ended.
        // Claiming exhaustion here would train on a truncated corpus, persist it,
        // and report Ready with no error anywhere - which is silent, unlike a
        // build that merely keeps failing.
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "Banking a faulted slice must record it as incomplete, or the build advances to Training "
            + "on a fraction of the corpus.");

        var resumed = await OpenAsync(store, Source(maxFaults: 0), options);
        Assert.That(resumed.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "The incompleteness has to survive in the store too, not only in the index that observed it.");
    }
}
