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

    /// <summary>
    /// Drives a build to the state a stalled one leaves on the store: a committed
    /// prefix that ends part-way through a chunk.
    /// <para>
    /// Only two writers produce it, and both commit a true vector count rather
    /// than one rounded down to a chunk boundary: a completed ingest checkpoint,
    /// and the full rewrite the checkpoint falls back to when something replaced a
    /// vector mid-build. The second is reproduced here, because it is reachable
    /// while the build is still ingesting and so leaves the index in the state a
    /// restart has to resume from.
    /// </para>
    /// </summary>
    private static async Task<InMemoryVectorIndexStore> PrefixEndingMidChunkAsync(
        ListVectorSource source, DurableVectorIndexOptions options)
    {
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        // Appends the build's own stream can never hand over again, which is what
        // takes the count off a chunk boundary and keeps it off one.
        for (var i = 1; i <= 3; i++)
        {
            var late = $"doc-000000-late{i}";
            source.Set(late, source[DurableIndexHarness.Id(i)]);
            await index.UpsertAsync(late, source[late]);
        }

        // A replacement of an identifier already streamed, which is what
        // legitimately takes the cell out of append-only for one checkpoint.
        await index.UpsertAsync(DurableIndexHarness.Id(0), source[DurableIndexHarness.Id(LargeCorpus - 1)]);
        await index.BuildStepAsync();

        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        return store;
    }

    /// <summary>
    /// The cost of resuming a build must be the cost of the slice it ingests, and
    /// nothing about the size of what is already committed.
    /// <para>
    /// A committed prefix that ends mid-chunk used to be read as a prefix laid out
    /// at some other item count, and the answer to that is to re-lay the cell
    /// whole. But the re-lay commits a true count too, so it lands straight back in
    /// the same state and re-arms the condition for the next load. Every activation
    /// then rewrote the entire cell under a fresh epoch, and because the epoch is
    /// part of the chunk key nothing superseded anything: a log-structured store
    /// retains every pass, so the write-ahead log grew without limit while the
    /// index stood still. An index whose training never completes never leaves that
    /// branch, which is how one deployment reached 38 GB of write-ahead log for a
    /// 55,000 vector index across 215 epochs.
    /// </para>
    /// </summary>
    [Test]
    public async Task Resuming_a_build_whose_committed_prefix_ends_mid_chunk_costs_the_same_on_every_restart()
    {
        var source = DurableIndexHarness.Source(LargeCorpus);
        var options = Options();
        var store = await PrefixEndingMidChunkAsync(source, options);

        var volumes = new List<long>();
        for (var restart = 0; restart < 8; restart++)
        {
            var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
            store.ResetBytesWritten();
            await resumed.BuildStepAsync();
            volumes.Add(store.BytesWritten);
        }

        // Each restart ingests one slice of the same size, so a checkpoint that
        // writes only what arrived is flat across the run. One that re-lays the
        // cell instead grows with the index on every pass, which is the shape
        // being ruled out rather than any particular byte count.
        Assert.That(
            (double)volumes[^1] / volumes[0],
            Is.LessThan(1.25),
            $"resuming cost {volumes[0]} bytes on the first restart and {volumes[^1]} on the last, so the "
            + "checkpoint is re-laying the committed cell on every activation rather than appending to it");
    }

    /// <summary>
    /// Resuming from a prefix that ends mid-chunk must not lose the index.
    /// <para>
    /// The partial chunk is the whole difficulty: an append numbers its chunks from
    /// the committed count up, so resuming without accounting for that tail leaves
    /// it holding fewer vectors than its chunk number claims. A loader then reads
    /// back less than the manifest promises, and because a partially trusted index
    /// is the one thing the coherence contract rules out, it discards the lot. That
    /// failure is silent, arrives one restart later than the change that caused it,
    /// and costs the entire index, so it is worth its own arm.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_prefix_ending_mid_chunk_is_resumed_and_not_discarded()
    {
        var source = DurableIndexHarness.Source(LargeCorpus);
        var options = Options();
        var store = await PrefixEndingMidChunkAsync(source, options);

        var counts = new List<int>();
        for (var restart = 0; restart < 4; restart++)
        {
            var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
            await resumed.BuildStepAsync();
            counts.Add(resumed.Count);

            Assert.That(
                resumed.Count,
                Is.GreaterThan(0),
                $"restart {restart} loaded an empty index, so the committed prefix was discarded");
        }

        Assert.That(counts, Is.Ordered.Ascending, "a resumed build must keep what it had and add to it");
        Assert.That(
            store.KeysWithPrefix("vidx/k/f/"),
            Has.Count.EqualTo(counts[^1]),
            "the identifier mapping and the cell must still describe the same index");
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
