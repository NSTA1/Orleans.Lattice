using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Guards the SIZE of an individual persisted record, which is a different
/// quantity from the total write volume that
/// <see cref="DurableVectorIndexWriteAmplificationTests"/> guards, and is not
/// bounded by it.
/// <para>
/// A chunk is sized in vectors, but a store charges for bytes: one record is one
/// entry in a write-ahead log and one contiguous allocation on the read path. An
/// item count therefore bounds a record only at a fixed dimensionality, and the
/// bound it implies grows linearly with the vector width. At the 768 dimensions
/// a current embedding model produces, a 1,024-item chunk is 3.15 MB - against a
/// 4 MB write batch, so a batch could not coalesce two of them and the batching
/// bound was inert - and it is the payload observed in every
/// <c>WalReadUnderPressureException</c> on the repository-context vector index in
/// issue #2782.
/// </para>
/// <para>
/// The fix derives the item count from the dimensionality, so it is adaptive to
/// the index rather than fitted to a host, and the configured item count becomes
/// a ceiling rather than the written size.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexChunkSizingTests
{
    /// <summary>The width a current embedding model produces, and the rig's.</summary>
    private const int WideDimensions = 768;

    /// <summary>Four bytes a component plus the eight-byte identifier a chunk carries per item.</summary>
    private const int WideStride = (WideDimensions * 4) + 8;

    /// <summary>
    /// Large enough that a partition holds more items than one byte-bounded
    /// chunk can carry. Below that the arms here are vacuous: a corpus split
    /// across the harness's partitions leaves every chunk under the ceiling
    /// whatever item count it was written at, so the ceiling is never put to
    /// the test.
    /// </summary>
    private const int Corpus = 1_600;

    private static DurableVectorIndexOptions WideOptions(
        int maxItemsPerChunk = 1_024, int ingestBatchSize = 128) =>
        DurableIndexHarness.Options(
            dimensions: WideDimensions,
            maxItemsPerChunk: maxItemsPerChunk,
            ingestBatchSize: ingestBatchSize);

    private static ListVectorSource WideSource(int count = Corpus) =>
        DurableIndexHarness.Source(count, dimensions: WideDimensions);

    [Test]
    public async Task A_wide_index_writes_no_record_larger_than_the_chunk_ceiling()
    {
        var store = new InMemoryVectorIndexStore();
        await DurableIndexHarness.BuiltAsync(store, WideSource(), WideOptions());

        Assert.Multiple(() =>
        {
            Assert.That(
                store.LargestRecordBytes,
                Is.GreaterThan(0),
                "the arm is vacuous unless the build actually wrote something");
            Assert.That(
                store.LargestRecordBytes,
                Is.LessThanOrEqualTo(DurableVectorIndexOptions.MaxChunkBytes),
                $"a single record of {store.LargestRecordBytes} bytes is one write-ahead entry and one "
                + "contiguous allocation on the read path, and an item count does not bound it as the "
                + "dimensionality rises");

            // Establishes that the clause above discriminates, on the two counts
            // it could be vacuous on: the configured item count has to be one
            // the ceiling would actually stop, and a partition has to hold more
            // items than one byte-bounded chunk carries. Without both, every
            // record is under the ceiling whatever it was written at.
            var configured = WideOptions().MaxItemsPerChunk;
            Assert.That(
                (long)configured * WideStride,
                Is.GreaterThan(DurableVectorIndexOptions.MaxChunkBytes),
                "the arm is vacuous unless the configured item count would overrun the ceiling at this width");
            Assert.That(
                Corpus / WideOptions().Index.PartitionCount,
                Is.GreaterThan(DurableVectorIndexOptions.ResolveItemsPerChunk(WideDimensions, configured)),
                "the arm is vacuous unless a partition holds more than one byte-bounded chunk");
        });
    }

    [Test]
    public async Task A_write_batch_coalesces_several_chunks_rather_than_carrying_one()
    {
        var store = new InMemoryVectorIndexStore();

        // A checkpoint wide enough to fill a whole write batch, so the question
        // the arm asks - is the bound respected, or exceeded by the last record
        // added - is actually put to the writer. A checkpoint that produces
        // fewer chunks than one batch holds never reaches the bound at all, and
        // the arm passes whichever side of the add the bound is tested on.
        await DurableIndexHarness.BuiltAsync(
            store, WideSource(), WideOptions(ingestBatchSize: Corpus));

        Assert.Multiple(() =>
        {
            Assert.That(
                store.LargestBatchEntries,
                Is.GreaterThanOrEqualTo(4),
                "a batching bound a single record can exceed on its own is inert: the writer hands over "
                + "one chunk per batch and the bound never binds");
            Assert.That(
                store.LargestBatchBytes,
                Is.GreaterThan(DurableVectorIndexOptions.WriteBatchBytes - (2 * store.LargestRecordBytes)),
                "the arm is vacuous unless a batch came within a record of the bound: a batch that never "
                + "approaches it is within it whether the bound is tested before or after the add");
            Assert.That(
                store.LargestBatchBytes,
                Is.LessThanOrEqualTo(DurableVectorIndexOptions.WriteBatchBytes),
                $"a batch of {store.LargestBatchBytes} bytes overruns the {DurableVectorIndexOptions.WriteBatchBytes}-byte "
                + "bound, so the record that crossed it was added before the bound was tested");
        });
    }

    [Test]
    public void A_wide_index_takes_the_largest_item_count_that_fits_the_byte_budget()
    {
        var resolved = DurableVectorIndexOptions.ResolveItemsPerChunk(WideDimensions, 1_024);

        Assert.Multiple(() =>
        {
            Assert.That(
                resolved,
                Is.LessThan(1_024),
                "at this width the byte budget is what binds, so the configured item count cannot be");
            Assert.That(
                resolved * WideStride,
                Is.LessThanOrEqualTo(DurableVectorIndexOptions.MaxChunkBytes),
                "the resolved count must fit the budget it was derived from");

            // Tightness, asserted without restating a header size: a count that
            // used a small fraction of the budget would also satisfy the bound
            // above, and would quietly multiply the record count instead.
            Assert.That(
                resolved * WideStride,
                Is.GreaterThan(DurableVectorIndexOptions.MaxChunkBytes * 0.9),
                "the sizing must take the largest count that fits, not an arbitrarily conservative one");
        });
    }

    [Test]
    public void A_narrow_index_is_still_bounded_by_its_configured_item_count()
    {
        const int Ceiling = 64;
        var resolved = DurableVectorIndexOptions.ResolveItemsPerChunk(DurableIndexHarness.Dimensions, Ceiling);
        var narrowStride = (DurableIndexHarness.Dimensions * 4) + 8;

        Assert.Multiple(() =>
        {
            Assert.That(
                resolved,
                Is.EqualTo(Ceiling),
                "at this width the byte budget would admit thousands of vectors, so the configured count "
                + "is what must bind - otherwise the sizing silently overrides every deployment's tuning");

            // Establishes that the clause above discriminates: the byte budget
            // really would have admitted far more, so the ceiling is what bound
            // the result rather than the two happening to agree.
            Assert.That(
                DurableVectorIndexOptions.MaxChunkBytes / narrowStride,
                Is.GreaterThan(Ceiling * 4),
                "the arm is vacuous unless the byte budget is the slacker of the two bounds here");
        });
    }

    [Test]
    public void An_index_whose_dimensionality_is_unset_falls_back_to_the_configured_item_count()
    {
        // Sizing is read off the index options, whose dimensionality is zero
        // until a deployment sets it. A width that is not yet known cannot be
        // costed in bytes, so the configured count has to survive that case -
        // and it does without a branch, because the stride degenerates to the
        // identifier alone rather than to nothing.
        var options = new DurableVectorIndexOptions { MaxItemsPerChunk = 512 };

        Assert.That(
            options.Index.Dimensions,
            Is.Zero,
            "the arm is vacuous unless the dimensionality really is unset here");
        Assert.That(options.EffectiveItemsPerChunk, Is.EqualTo(512));
    }

    [Test]
    public async Task A_resumed_wide_ingest_appends_rather_than_rewriting_what_it_banked()
    {
        // The reference is the same corpus built without interruption, so the
        // bound below is expressed against an observed write volume rather than
        // against a constant fitted to this corpus.
        var reference = new InMemoryVectorIndexStore();
        await DurableIndexHarness.BuiltAsync(reference, WideSource(), WideOptions());

        var source = WideSource();
        var store = new InMemoryVectorIndexStore();

        // Bank a partial prefix, then drop the index and resume from the store.
        var index = await DurableIndexHarness.OpenAsync(store, source, WideOptions());
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        Assert.That(
            index.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "the arm is only meaningful while the cell is still an append-only ingest prefix");

        // Measure the resume alone, and watch the cursor it hands the source.
        // A resume that can append names the prefix it banked and reads only
        // what is left; one whose ingest boundary does not agree with the size
        // the checkpoint writes chunks at has no durable cursor to name, so it
        // asks for the corpus from the start and re-ingests the prefix.
        store.ResetBytesWritten();
        var resumed = await DurableIndexHarness.OpenAsync(store, source, WideOptions());
        var bankedAtResume = resumed.Count;
        await resumed.BuildStepAsync();
        var resumedFrom = source.LastResumedFrom;
        await resumed.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.Count, Is.EqualTo(Corpus), "the resumed build must hold the whole corpus");
            Assert.That(
                bankedAtResume,
                Is.GreaterThan(0),
                "the arm is vacuous unless the interrupted build really banked a committed prefix");
            Assert.That(
                resumedFrom,
                Is.Not.Null,
                $"the resume re-read the corpus from the start with {bankedAtResume} vectors already "
                + "committed, so it re-ingested and rewrote the prefix instead of appending to it. That "
                + "happens when the boundary the ingest cursor is aligned to is not the size the checkpoint "
                + "writes chunks at, which leaves the durable cursor unable to name the committed prefix");
            Assert.That(
                reference.BytesWritten,
                Is.GreaterThan(0),
                "the arm is vacuous unless the reference build actually wrote something");
            Assert.That(
                store.BytesWritten,
                Is.LessThan(reference.BytesWritten),
                $"the resume wrote {store.BytesWritten} bytes against {reference.BytesWritten} for a whole "
                + "uninterrupted build, so it paid a full build's write volume to finish a partial one");
        });
    }

    [Test]
    public async Task An_ingest_resumed_under_a_different_chunk_size_recovers_the_whole_corpus()
    {
        var source = WideSource();
        var store = new InMemoryVectorIndexStore();

        // Ingest a prefix at one chunk size, so the store holds committed chunks
        // whose item count is known only to the options that wrote them.
        var index = await DurableIndexHarness.OpenAsync(store, source, WideOptions(maxItemsPerChunk: 25));
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        Assert.That(
            index.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "the arm is only meaningful while the cell is still unpartitioned and append-only");

        // Resume it at a LARGER one. This is what byte-bounded sizing does to
        // every tree built before it landed, and it is equally what changing the
        // configured item count between two runs already did: the resume
        // arithmetic is committed = chunks x itemsPerChunk, which is wrong the
        // instant a cell holds chunks at two sizes. The direction matters. A
        // smaller size makes the new chunks start behind where the old ones
        // ended, so the overlap happens to cover everything and the damage is
        // invisible; a larger one makes them start beyond it, leaving the
        // vectors in between written nowhere at all.
        var resumed = await DurableIndexHarness.OpenAsync(store, source, WideOptions(maxItemsPerChunk: 40));
        await resumed.BuildStepAsync();
        Assert.That(
            resumed.Progress.Phase,
            Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "the arm is only meaningful while the prefix the resume committed is still the durable one");

        // Read that prefix back rather than letting any build finish. A
        // completing build rewrites every partition in full, which repairs a
        // mis-laid prefix on the way past and hides whether it was ever
        // coherent. Only a loader that has to trust the committed chunks, and
        // is not given the chance to rewrite them, sees the damage.
        var reloaded = await DurableIndexHarness.OpenAsync(store, source, WideOptions(maxItemsPerChunk: 40));

        Assert.That(
            reloaded.Count,
            Is.GreaterThan(0),
            "the arm is vacuous unless the resume banked something for the reload to recover");

        for (var i = 0; i < reloaded.Count; i++)
        {
            Assert.That(
                reloaded.TryGetKey(DurableIndexHarness.Id(i), out _),
                Is.True,
                $"the reload recovered {reloaded.Count} vectors but {DurableIndexHarness.Id(i)} is not among "
                + "them, so what it recovered is not a prefix of the corpus: chunks already on the store "
                + "hold a different number of items each than the ones appended after them, so the chunk "
                + "index no longer names the range of vectors the manifest's count implies");
        }
    }
}
