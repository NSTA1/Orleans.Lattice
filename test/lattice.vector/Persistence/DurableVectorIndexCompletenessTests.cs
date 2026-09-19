using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Pins the rule that a durable build may only report a finished corpus on the
/// strength of the source having signalled exhaustion.
/// <para>
/// These are invariant guards rather than reproductions of a live defect. The
/// build has exactly two ways out of an ingest slice today - the source ends, or
/// the per-slice work budget is reached - so the older formulation that inferred
/// completion from "consumed fewer than allowed" happens to agree with the
/// source's own signal in every case reachable now. It stops agreeing the moment
/// a third way out is added, and the failure is silent: a partial corpus is
/// trained, persisted, and reported Ready. The fixture exists so that change
/// cannot land unnoticed.
/// </para>
/// </summary>
[TestFixture]
internal sealed class DurableVectorIndexCompletenessTests
{
    private static async Task<(DurableVectorIndex Index, ObservingVectorSource Source)> OpenAsync(
        InMemoryVectorIndexStore store, int corpus, int ingestBatchSize)
    {
        var source = new ObservingVectorSource(DurableIndexHarness.Source(corpus));
        var options = DurableIndexHarness.Options(ingestBatchSize: ingestBatchSize);
        var index = await DurableVectorIndex.OpenAsync(store, source, options);
        return (index, source);
    }

    [Test]
    public async Task A_build_reports_ready_only_after_the_source_signals_exhaustion()
    {
        var store = new InMemoryVectorIndexStore();
        var (index, source) = await OpenAsync(store, corpus: 200, ingestBatchSize: 64);

        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));

            // The claim under test: Ready was not reached on an inference, it was
            // reached after the source was actually read to its end.
            Assert.That(source.Drained, Is.EqualTo(1),
                "Ready must follow exactly one enumeration that ran to the end.");
            Assert.That(source.Yielded, Is.EqualTo(200));
        });
    }

    [Test]
    public async Task A_slice_stopped_by_the_work_budget_does_not_finish_the_build()
    {
        var store = new InMemoryVectorIndexStore();
        var (index, source) = await OpenAsync(store, corpus: 200, ingestBatchSize: 64);

        // StartBuild, then a single ingest slice. The slice fills its budget, so
        // the source was never drained and the build has no licence to complete.
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
            Assert.That(source.Yielded, Is.EqualTo(64));
            Assert.That(source.Drained, Is.Zero,
                "A budget-bounded slice must not be recorded as an exhausted source.");
        });
    }

    [Test]
    public async Task A_corpus_that_is_an_exact_multiple_of_the_work_budget_still_completes()
    {
        // The boundary case the older inference was most fragile around: every
        // slice fills its budget exactly, so completion can only ever come from a
        // final slice that yields nothing and ends.
        var store = new InMemoryVectorIndexStore();
        var (index, source) = await OpenAsync(store, corpus: 128, ingestBatchSize: 64);

        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(source.Yielded, Is.EqualTo(128));
            Assert.That(source.Drained, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_empty_corpus_completes_on_the_sources_signal_rather_than_on_a_count()
    {
        var store = new InMemoryVectorIndexStore();
        var (index, source) = await OpenAsync(store, corpus: 0, ingestBatchSize: 64);

        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(source.Yielded, Is.Zero);
            Assert.That(source.Drained, Is.EqualTo(1),
                "An empty corpus is still a corpus the source has to declare finished.");
        });
    }

    [Test]
    public async Task A_resumed_build_completes_on_the_signal_from_its_final_slice()
    {
        var store = new InMemoryVectorIndexStore();
        var (first, _) = await OpenAsync(store, corpus: 200, ingestBatchSize: 64);

        await first.BuildStepAsync();
        await first.BuildStepAsync();
        Assert.That(first.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));

        // A second index over the same store resumes from the durable cursor. Its
        // own completion has to come from its own observation of the source, not
        // from anything the first one persisted about how far it got.
        var (second, resumed) = await OpenAsync(store, corpus: 200, ingestBatchSize: 64);
        await second.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(second.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(resumed.Drained, Is.EqualTo(1));
            Assert.That(resumed.Yielded, Is.EqualTo(136),
                "The resumed build must read the remainder of the corpus, not all of it.");
        });
    }
}
