using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Guards the write volume an ingesting index produces.
/// <para>
/// While the index is ingesting it holds one untrained cell, and the checkpoint
/// that banks a build slice can either append the chunks that arrived since the
/// last one or rewrite the cell whole. Which of the two it picks is decided by
/// whether anything disturbed the committed prefix. Charging a rewrite to a
/// mutation that disturbed nothing is not a small overcharge: the writer hands a
/// batch over once per slice, so the build pays a rewrite of the whole index per
/// slice and its write-ahead volume becomes quadratic in corpus size. That is
/// issue #2691, where one tree reached 25,789 MB of write-ahead log in fourteen
/// hours while its largest sibling reached 185 MB.
/// </para>
/// <para>
/// The volume assertions are therefore written against the SHAPE of the growth
/// and not against a byte count: doubling the corpus must roughly double the
/// bytes written, because that is the difference between linear and quadratic,
/// and it is the property that actually protects the deployment.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexWriteAmplificationTests
{
    private const int SmallCorpus = 1_000;
    private const int LargeCorpus = 2_000;

    /// <summary>
    /// Linear growth doubles when the corpus doubles; quadratic growth
    /// quadruples. The bar sits between the two and close to linear: the
    /// measured ratio with the checkpoint behaving is a little over 2, and
    /// charging a rewrite per slice instead takes it past 3.5.
    /// </summary>
    private const double LinearGrowthCeiling = 2.5;

    private static DurableVectorIndexOptions Options() =>
        DurableIndexHarness.Options(maxItemsPerChunk: 64, ingestBatchSize: 128);

    /// <summary>
    /// The bytes a build writes when the writer hands over one vector per slice
    /// that the build has not yet streamed. Every such write is a plain append:
    /// it lands at the tail and leaves every committed chunk exactly as it was.
    /// </summary>
    private static async Task<long> AppendDuringIngestAsync(int corpus)
    {
        var source = DurableIndexHarness.Source(corpus);
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, Options());

        var slice = 0;
        while (index.Progress.Phase != VectorIndexBuildPhase.Ready)
        {
            await index.BuildStepAsync();
            if (index.Progress.Phase != VectorIndexBuildPhase.Ingesting || index.Count == 0)
            {
                continue;
            }

            // Counted down from the end of the corpus, so it is always an
            // identifier the build's own stream has not reached.
            var id = DurableIndexHarness.Id(corpus - 1 - slice);
            await index.UpsertAsync(id, source[id]);
            slice++;
        }

        Assert.That(slice, Is.GreaterThan(2), "the arm must span several checkpoints to mean anything");
        Assert.That(index.Count, Is.EqualTo(corpus));
        return store.BytesWritten;
    }

    [Test]
    public async Task An_append_the_build_did_not_make_does_not_cost_a_rewrite_of_the_whole_cell()
    {
        var clean = new InMemoryVectorIndexStore();
        await (await DurableIndexHarness.OpenAsync(
            clean, DurableIndexHarness.Source(LargeCorpus), Options())).RunBuildAsync();

        var withAppends = await AppendDuringIngestAsync(LargeCorpus);

        // A handful of appends may add a chunk each and may collide with the
        // build's own stream later on, so this is not free - but it is bounded
        // work, not a pass over the index per slice.
        Assert.That(
            withAppends,
            Is.LessThan(clean.BytesWritten * 2),
            "an append disturbs no committed chunk, so it must not force the checkpoint to rewrite the cell");
    }

    [Test]
    public async Task The_write_volume_of_a_build_taking_writes_grows_with_the_corpus_and_not_with_its_square()
    {
        var small = await AppendDuringIngestAsync(SmallCorpus);
        var large = await AppendDuringIngestAsync(LargeCorpus);

        Assert.That(
            (double)large / small,
            Is.LessThan(LinearGrowthCeiling),
            $"doubling the corpus took the write volume from {small} to {large}, which is the quadratic "
            + "amplification of issue #2691 rather than the linear cost of a build");
    }

    [Test]
    public async Task Retiring_an_identifier_the_index_never_held_costs_nothing()
    {
        var source = DurableIndexHarness.Source(SmallCorpus);
        var options = Options();

        var clean = new InMemoryVectorIndexStore();
        await (await DurableIndexHarness.OpenAsync(clean, source, options)).RunBuildAsync();

        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        while (index.Progress.Phase != VectorIndexBuildPhase.Ready)
        {
            await index.BuildStepAsync();

            // Nothing is mapped under this identifier, so there is no position to
            // vacate and no backfill: the cell is untouched.
            Assert.That(await index.RemoveAsync("never-indexed"), Is.False);
        }

        Assert.That(
            store.BytesWritten,
            Is.EqualTo(clean.BytesWritten),
            "a retirement that found nothing shifted nothing, so it must not make the checkpoint rewrite the cell");
    }

    [Test]
    public async Task A_replacement_the_build_did_not_make_is_still_durable_across_a_restart()
    {
        var source = DurableIndexHarness.Source(SmallCorpus);
        var options = Options();
        var store = new InMemoryVectorIndexStore();

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));

        // An identifier the build has already streamed, so this vacates a
        // position inside the committed prefix and backfills it from the tail.
        var replaced = DurableIndexHarness.Id(0);
        Assert.That(await index.UpsertAsync(replaced, source[DurableIndexHarness.Id(SmallCorpus - 1)]), Is.True);
        await index.BuildStepAsync();

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
        await resumed.RunBuildAsync();

        Assert.That(resumed.Count, Is.EqualTo(SmallCorpus), "the resumed index must hold the whole corpus");
        for (var i = 0; i < SmallCorpus; i++)
        {
            Assert.That(
                resumed.TryGetKey(DurableIndexHarness.Id(i), out _),
                Is.True,
                $"{DurableIndexHarness.Id(i)} was lost across the restart");
        }
    }

    [Test]
    public async Task An_append_the_builds_own_stream_can_no_longer_reach_is_durable_across_a_restart()
    {
        var source = DurableIndexHarness.Source(SmallCorpus);
        var options = Options();
        var store = new InMemoryVectorIndexStore();

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));

        // Sorts below everything the build has streamed, so the source - which
        // resumes strictly AFTER the durable cursor - can never hand it over
        // again. Only the flush the writer takes straight after a write can make
        // it durable, so cheapening the checkpoint must not cheapen that.
        const string Late = "doc-000000-late";
        var vector = source[DurableIndexHarness.Id(7)];
        source.Set(Late, vector);
        Assert.That(await index.UpsertAsync(Late, vector), Is.False, "the arm is only meaningful for an append");
        await index.FlushAsync();

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
        await resumed.RunBuildAsync();

        Assert.That(
            resumed.Count,
            Is.EqualTo(SmallCorpus + 1),
            "an append behind the durable cursor is invisible to a resumed build, so the checkpoint that "
            + "followed it had to make it durable");
    }
}
