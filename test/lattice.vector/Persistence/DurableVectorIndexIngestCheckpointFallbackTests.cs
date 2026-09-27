using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Guards the checkpoint an ingesting index falls back to once a replacement or a
/// removal has cost its single untrained cell the append-only property (#3669).
/// <para>
/// The fallback used to rewrite the whole cell under a fresh epoch. The cell is a
/// dense array that a removal backfills from the tail, so one mutation disturbs
/// at most two of its chunks; rewriting all of them for it made each checkpoint
/// cost the whole index. On a live deployment that meant a complete image of a
/// 71k-vector cell every few dozen banked vectors, and - once the rewrite outgrew
/// the grain call timeout - a retry that could not resume and wrote the whole cell
/// again under the same epoch, thousands of times.
/// </para>
/// <para>
/// The chunk-write assertions count vector-chunk keys only, so they measure the
/// cell rewrite rather than the fixed commit records around it. Every checkpoint
/// in these tests also appends the vectors its own slice streamed, so each bound
/// is the slice's own chunks plus the handful a single mutation can disturb.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexIngestCheckpointFallbackTests
{
    private const int ItemsPerChunk = 4;
    private const int Slice = 40;
    private const int SliceChunks = Slice / ItemsPerChunk;

    // A single mutation disturbs the chunk holding the vacated position and the
    // chunk the tail was moved out of; the re-appended vector lands in the tail.
    private const int ChunksOneMutationDisturbs = 3;

    private static DurableVectorIndexOptions Options(int itemsPerChunk = ItemsPerChunk, int slice = Slice) =>
        DurableIndexHarness.Options(maxItemsPerChunk: itemsPerChunk, ingestBatchSize: slice);

    private static string VectorChunkPrefix(DurableVectorIndexOptions options, long generation) =>
        VectorIndexStorageKeys.GenerationPrefix(options.KeyPrefix, generation) + "v/";

    private static int ChunksWritten(InMemoryVectorIndexStore store, DurableVectorIndexOptions options, long generation)
    {
        var prefix = VectorChunkPrefix(options, generation);
        return store.KeysWritten.Count(key => key.StartsWith(prefix, StringComparison.Ordinal));
    }

    private static async Task IngestUntilAsync(DurableVectorIndex index, int count)
    {
        while (index.Progress.Phase is VectorIndexBuildPhase.NotStarted or VectorIndexBuildPhase.Ingesting &&
               index.Count < count)
        {
            await index.BuildStepAsync();
        }

        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting), "the index must still be ingesting");
    }

    private static float[] Reembedded(float[] vector) => [.. vector.Select(component => -component)];

    private static void AssertHoldsEverySourceVector(DurableVectorIndex index, ListVectorSource source)
    {
        Assert.That(index.Count, Is.EqualTo(source.Ids.Count), "the index must hold every source vector exactly once");
        var missing = source.Ids
            .Where(id => DurableIndexHarness.SearchIds(index, source[id], 1) is not [var found] || found != id)
            .ToList();
        Assert.That(missing, Is.Empty, "every source vector must be its own nearest neighbour in the index");
    }

    [Test]
    public async Task A_replacement_mid_ingest_rewrites_only_the_chunks_it_disturbed()
    {
        var source = DurableIndexHarness.Source(400);
        var options = Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 200);

        var id = DurableIndexHarness.Id(10);
        source.Set(id, Reembedded(source[id]));
        Assert.That(await index.UpsertAsync(id, source[id]), Is.True, "the upsert must replace a streamed vector");

        store.ResetBytesWritten();
        await index.BuildStepAsync();

        var written = ChunksWritten(store, options, index.Generation);
        Assert.That(
            written,
            Is.InRange(SliceChunks, SliceChunks + ChunksOneMutationDisturbs),
            $"one replacement in a {index.Count / ItemsPerChunk}-chunk cell must cost the chunks it disturbed, "
            + "not a rewrite of the whole cell");
    }

    [Test]
    public async Task A_removal_mid_ingest_rewrites_only_the_chunks_it_disturbed()
    {
        var source = DurableIndexHarness.Source(400);
        var options = Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 200);

        var id = DurableIndexHarness.Id(10);
        source.Remove(id);
        Assert.That(await index.RemoveAsync(id), Is.True, "the removal must retire a streamed vector");

        store.ResetBytesWritten();
        await index.BuildStepAsync();

        var written = ChunksWritten(store, options, index.Generation);
        Assert.That(
            written,
            Is.InRange(SliceChunks, SliceChunks + ChunksOneMutationDisturbs),
            $"one removal in a {index.Count / ItemsPerChunk}-chunk cell must cost the chunks it disturbed, "
            + "not a rewrite of the whole cell");
    }

    [Test]
    public async Task A_fallback_checkpoint_retried_after_a_fault_writes_only_the_changed_chunks()
    {
        var source = DurableIndexHarness.Source(400);
        var options = Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 200);

        var id = DurableIndexHarness.Id(10);
        source.Set(id, Reembedded(source[id]));
        await index.UpsertAsync(id, source[id]);

        // The chunk batch lands and the commit record that follows it does not:
        // the shape of a checkpoint that outlived the call timeout.
        var statePrefix = VectorIndexStorageKeys.PartitionStatePrefix(options.KeyPrefix, index.Generation);
        var failures = 0;
        store.FailWrite = batch =>
            failures == 0 &&
            batch.Any(entry => entry.Key.StartsWith(statePrefix, StringComparison.Ordinal)) &&
            ++failures == 1;

        store.ResetBytesWritten();
        Assert.ThrowsAsync<SimulatedStoreFailureException>(() => index.BuildStepAsync());
        var faulted = ChunksWritten(store, options, index.Generation);

        store.ResetBytesWritten();
        await index.BuildStepAsync();
        var retried = ChunksWritten(store, options, index.Generation);

        Assert.That(
            faulted,
            Is.InRange(SliceChunks, SliceChunks + ChunksOneMutationDisturbs),
            "the faulted attempt must have written only the chunks the replacement disturbed");

        // The retry banks the faulted slice and its own, measured against what
        // was last committed; nothing it writes depends on how far the faulted
        // attempt got.
        Assert.That(
            retried,
            Is.InRange(2 * SliceChunks, (2 * SliceChunks) + ChunksOneMutationDisturbs),
            "a retry after a fault must write only the chunks that changed, not the whole cell again");

        store.FailWrite = null;
        await index.RunBuildAsync();
        AssertHoldsEverySourceVector(await DurableIndexHarness.OpenAsync(store, source, options), source);
    }

    [Test]
    public async Task A_load_after_an_incomplete_fallback_checkpoint_restores_the_index_and_resumes_the_build()
    {
        var source = DurableIndexHarness.Source(400);
        var options = Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 200);

        var id = DurableIndexHarness.Id(10);
        source.Set(id, Reembedded(source[id]));
        await index.UpsertAsync(id, source[id]);
        await index.BuildStepAsync();
        var banked = index.Count;

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);
        Assert.That(reopened.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting), "the build must resume");
        Assert.That(reopened.Count, Is.EqualTo(banked), "the load must restore everything the checkpoint banked");
        Assert.That(DurableIndexHarness.SearchIds(reopened, source[id], 1), Is.EqualTo(new[] { id }));

        await reopened.RunBuildAsync();
        AssertHoldsEverySourceVector(reopened, source);
        AssertHoldsEverySourceVector(await DurableIndexHarness.OpenAsync(store, source, options), source);
    }

    [Test]
    public async Task A_load_after_a_complete_fallback_checkpoint_restores_every_vector()
    {
        // Not a multiple of the chunk size, so the completed cell ends mid-chunk.
        var source = DurableIndexHarness.Source(402);
        var options = Options();
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 400);

        var id = DurableIndexHarness.Id(10);
        source.Remove(id);
        await index.RemoveAsync(id);
        await index.BuildStepAsync();
        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Training), "the slice must complete the ingest");

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);
        Assert.That(reopened.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Training), "the build must resume");
        Assert.That(reopened.Count, Is.EqualTo(401), "the load must restore the whole completed cell");

        await reopened.RunBuildAsync();
        AssertHoldsEverySourceVector(reopened, source);
    }

    [Test]
    public async Task A_short_slice_after_a_fallback_checkpoint_does_not_lose_the_vectors_the_fallback_banked()
    {
        // Slices shorter than a chunk, so the slice after the fallback crosses no
        // chunk boundary and has no boundary-aligned prefix of its own to commit.
        // Committing the whole chunks alone would drop below what the fallback
        // made durable while leaving the cursor where the fallback put it.
        var source = DurableIndexHarness.Source(64);
        var options = Options(itemsPerChunk: 8, slice: 2);
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 2);

        var id = DurableIndexHarness.Id(0);
        source.Set(id, Reembedded(source[id]));
        await index.UpsertAsync(id, source[id]);
        await index.BuildStepAsync();
        var banked = index.Count;
        await index.BuildStepAsync();
        Assert.That(index.Count, Is.GreaterThan(banked));

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);
        Assert.That(reopened.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting), "the build must resume");
        Assert.That(reopened.Count, Is.EqualTo(banked), "the load must restore everything the fallback banked");

        await reopened.RunBuildAsync();
        AssertHoldsEverySourceVector(reopened, source);
    }

    [Test]
    public async Task A_slice_crossing_a_chunk_boundary_after_a_fallback_checkpoint_commits_a_loadable_prefix()
    {
        var source = DurableIndexHarness.Source(64);
        var options = Options(itemsPerChunk: 8, slice: 3);
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await IngestUntilAsync(index, 3);

        // The fallback commits six vectors, the next slice fills the partial
        // chunk the fallback committed and crosses into the next one.
        var id = DurableIndexHarness.Id(1);
        source.Set(id, Reembedded(source[id]));
        await index.UpsertAsync(id, source[id]);
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        Assert.That(index.Count, Is.EqualTo(9));

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);
        Assert.That(reopened.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting), "the build must resume");
        Assert.That(reopened.Count, Is.EqualTo(8), "the load must restore the committed chunk, not discard the index");

        await reopened.RunBuildAsync();
        AssertHoldsEverySourceVector(reopened, source);
    }
}
