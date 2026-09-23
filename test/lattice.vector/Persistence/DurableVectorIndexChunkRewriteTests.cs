using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Guards the write volume of an ordinary flush on a trained index.
/// <para>
/// A cell is a dense array: an insert lands at its tail and a removal backfills
/// the vacated position from the tail, so one update disturbs at most two of the
/// cell's chunks. A flush used to rewrite every chunk of every dirty cell under a
/// fresh epoch anyway, so its cost grew with the size of the cell rather than
/// with what changed. With a write-ahead log in front of the store that is where
/// the bytes go: issue #3427 found the vector index appending roughly ten times
/// the bytes of the payload tree it indexes, and retaining most of them.
/// </para>
/// <para>
/// As with <see cref="DurableVectorIndexWriteAmplificationTests"/>, the volume
/// claim is asserted against the SHAPE of the cost - flat in the size of the
/// corpus - rather than against a byte count.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexChunkRewriteTests
{
    private const int ItemsPerChunk = 4;
    private const int Corpus = 2_000;

    private static DurableVectorIndexOptions Options() =>
        DurableIndexHarness.Options(maxItemsPerChunk: ItemsPerChunk, ingestBatchSize: 512);

    private static string VectorChunkPrefix(DurableVectorIndexOptions options, long generation) =>
        VectorIndexStorageKeys.GenerationPrefix(options.KeyPrefix, generation) + "v/";

    private static float[] Replacement(ulong seed) =>
        VectorCorpus.Clustered(1, DurableIndexHarness.Dimensions, 8, seed)[0];

    [Test]
    public async Task Re_embedding_one_vector_rewrites_a_few_chunks_and_not_its_cells()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        var chunksPerCell = Corpus / index.Status.PartitionCount / ItemsPerChunk;
        Assert.That(chunksPerCell, Is.GreaterThan(20), "the claim needs cells of many chunks to mean anything");

        store.ResetBytesWritten();
        Assert.That(await index.UpsertAsync(DurableIndexHarness.Id(5), Replacement(seed: 900)), Is.True);
        await index.FlushAsync();

        var prefix = VectorChunkPrefix(options, index.Generation);
        var rewritten = store.KeysWritten.Count(key => key.StartsWith(prefix, StringComparison.Ordinal));
        TestContext.Out.WriteLine($"{chunksPerCell} chunks per cell; one re-embed rewrote {rewritten} chunks");

        // The vacated position and the tail of the cell it left, and the tail of
        // the cell it joined: three chunks, whichever cells those are.
        Assert.That(rewritten, Is.InRange(1, 3),
            "a re-embed disturbs at most three chunks, so a flush must not rewrite whole cells for it");
    }

    [Test]
    public async Task The_cost_of_flushing_a_fixed_batch_of_updates_does_not_grow_with_the_corpus()
    {
        static async Task<long> FlushCostAsync(int corpus)
        {
            var store = new InMemoryVectorIndexStore();
            var source = DurableIndexHarness.Source(corpus);
            var index = await DurableIndexHarness.BuiltAsync(store, source, Options());

            store.ResetBytesWritten();
            for (var i = 0; i < 16; i++)
            {
                await index.UpsertAsync(DurableIndexHarness.Id(i * 37), Replacement(seed: 1_000 + (ulong)i));
            }

            await index.FlushAsync();
            return store.BytesWritten;
        }

        var small = await FlushCostAsync(Corpus);
        var large = await FlushCostAsync(Corpus * 2);

        // Rewriting whole cells doubles the cost when the cells double in size.
        Assert.That((double)large / small, Is.LessThan(1.5),
            $"the same sixteen updates cost {small} bytes against {Corpus} vectors and {large} against "
            + $"{Corpus * 2}, so the flush is rewriting cells rather than the chunks that changed");
    }

    [Test]
    public async Task Superseded_chunks_are_reclaimed_so_the_store_holds_only_the_live_ones()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var partitions = index.Status.PartitionCount;

        var next = Corpus;
        for (var round = 0; round < 20; round++)
        {
            for (var i = 0; i < 10; i++)
            {
                await index.UpsertAsync(DurableIndexHarness.Id(((round * 37) + (i * 7)) % 1_000), Replacement((ulong)((round * 10) + i)));
                var added = DurableIndexHarness.Id(next++);
                var vector = Replacement((ulong)(5_000 + next));
                source.Set(added, vector);
                await index.UpsertAsync(added, vector);
                var retired = DurableIndexHarness.Id((round * 10) + i + 1_000);
                source.Remove(retired);
                await index.RemoveAsync(retired);
            }

            await index.FlushAsync();
        }

        // Every live chunk is full except, at most, the last of each cell.
        var held = store.KeysWithPrefix(VectorChunkPrefix(options, index.Generation)).Count;
        var ceiling = (index.Count / ItemsPerChunk) + partitions;
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.LessThanOrEqualTo(ceiling),
                $"the store holds {held} vector chunks for {index.Count} vectors, so superseded chunks are leaking");
            Assert.That(reopened.Count, Is.EqualTo(index.Count));
            Assert.That(reopened.Count, Is.EqualTo(source.Ids.Count));
        });
    }

    [Test]
    public async Task A_cell_rewritten_in_part_reloads_to_exactly_what_was_flushed()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        var moved = DurableIndexHarness.Id(11);
        var replacement = Replacement(seed: 901);
        await index.UpsertAsync(moved, replacement);
        await index.FlushAsync();

        var queries = VectorCorpus.Clustered(10, DurableIndexHarness.Dimensions, 8, seed: 902);
        var expected = queries.Select(query => DurableIndexHarness.SearchIds(index, query, 10)).ToList();
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(Corpus));
            Assert.That(queries.Select(query => DurableIndexHarness.SearchIds(reopened, query, 10)).ToList(),
                Is.EqualTo(expected));
            Assert.That(DurableIndexHarness.SearchIds(reopened, replacement, 1), Is.EqualTo(new[] { moved }),
                "the reloaded index must hold the re-embedded vector, not the one it replaced");
        });
    }

    [Test]
    public async Task A_lazily_loaded_index_reads_a_cell_whose_chunks_span_several_epochs()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        var moved = DurableIndexHarness.Id(23);
        var replacement = Replacement(seed: 903);
        await index.UpsertAsync(moved, replacement);
        await index.FlushAsync();

        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);
        var results = new VectorSearchResult[1];
        var outcome = await lazy.SearchAsync(replacement, results);

        Assert.That(outcome.Count, Is.EqualTo(1));
        Assert.That(lazy.TryGetId(results[0].Key, out var id) ? id : null, Is.EqualTo(moved));
    }

    [Test]
    public async Task A_flush_interrupted_before_its_commit_leaves_the_previous_index_loadable()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        var moved = DurableIndexHarness.Id(31);
        var original = source[moved];
        await index.UpsertAsync(moved, Replacement(seed: 904));

        // The changed chunks of the first dirty cell are written, and the record
        // that would name them is not.
        store.FailAfterWrites = store.Writes + 1;
        Assert.That(async () => await index.FlushAsync(), Throws.TypeOf<SimulatedStoreFailureException>());
        store.FailAfterWrites = -1;

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(Corpus), "the previous index must load whole");
            Assert.That(DurableIndexHarness.SearchIds(reopened, original, 1), Is.EqualTo(new[] { moved }),
                "an uncommitted chunk must not be read, so the vector is still the one last committed");
        });

        // And the index that loaded carries on: its next flush commits cleanly.
        var replacement = Replacement(seed: 905);
        await reopened.UpsertAsync(moved, replacement);
        await reopened.FlushAsync();
        var again = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(again.Count, Is.EqualTo(Corpus));
            Assert.That(DurableIndexHarness.SearchIds(again, replacement, 1), Is.EqualTo(new[] { moved }));
        });
    }
}
