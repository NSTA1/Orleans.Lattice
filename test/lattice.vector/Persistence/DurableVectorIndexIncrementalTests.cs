using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Incremental maintenance: a source that is being re-embedded, extended, and
/// pruned must keep its index accurate <i>without</i> rebuilding it, and the cost
/// of a flush must scale with what changed rather than with the corpus.
/// <para>
/// Recall is measured against brute force over the corpus as it stands after the
/// churn, not against the corpus the index was trained on, so a partitioning that
/// had quietly stopped describing its own data would show up here.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexIncrementalTests
{
    private const int Dimensions = 32;
    private const int Clusters = 24;
    private const int Corpus = 3_000;
    private const int K = 10;

    /// <summary>The floor S6 published for a clustered corpus, which is the geometry real embeddings have.</summary>
    private const double ClusteredRecallFloor = 0.95;

    /// <summary>The floor S6 published for an adversarial corpus with no cluster structure to exploit.</summary>
    private const double UnclusteredRecallFloor = 0.55;

    private static DurableVectorIndexOptions Options() => new()
    {
        KeyPrefix = "churn/",
        MaxItemsPerChunk = 256,
        IngestBatchSize = 1_024,
        Index = new VectorIndexOptions { Dimensions = Dimensions },
    };

    private static ListVectorSource Source(float[][] corpus)
    {
        var source = new ListVectorSource(Dimensions);
        for (var i = 0; i < corpus.Length; i++)
        {
            source.Set(DurableIndexHarness.Id(i), corpus[i]);
        }

        return source;
    }

    /// <summary>
    /// Recall of the index against brute force over exactly what the source holds
    /// now, resolved through identifiers so a stale or reassigned key cannot be
    /// mistaken for a hit.
    /// </summary>
    private static double Recall(DurableVectorIndex index, ListVectorSource source, IReadOnlyList<float[]> queries)
    {
        var ids = source.Ids;
        var vectors = new float[ids.Count][];
        var keys = new long[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            vectors[i] = source[ids[i]];
            Assert.That(index.TryGetKey(ids[i], out keys[i]), Is.True, $"'{ids[i]}' is not indexed.");
        }

        var total = 0d;
        var results = new VectorSearchResult[K];
        foreach (var query in queries)
        {
            var exact = VectorCorpus.ExactTopK(vectors, keys, query, K, VectorDistanceMetric.Cosine);
            var found = index.Search(query, results, out _);
            total += VectorCorpus.Recall(results.AsSpan(0, found), exact);
        }

        return total / queries.Count;
    }

    /// <summary>
    /// A realistic re-embed: the document's meaning has not changed out of
    /// recognition, so its vector moves within its own neighbourhood rather than
    /// teleporting to an unrelated region of the space. The perturbation is the
    /// same magnitude as the corpus's own intra-cluster spread, so a re-embedded
    /// vector routinely crosses a cell boundary - which is the case incremental
    /// maintenance has to get right - without pretending the corpus has been
    /// replaced wholesale.
    /// </summary>
    private static float[] Perturb(float[] vector, ref VectorCorpus.TestRandom random, float scale = 0.35f)
    {
        var moved = new float[vector.Length];
        for (var d = 0; d < vector.Length; d++)
        {
            moved[d] = vector[d] + (scale * random.NextGaussian());
        }

        return moved;
    }

    /// <summary>
    /// A realistic maintenance workload: a fifth of the corpus re-embedded, a
    /// tenth retired, and a tenth added from the same distribution the index was
    /// built over.
    /// </summary>
    private static async Task ChurnAsync(
        DurableVectorIndex index, ListVectorSource source, float[][] additions, ulong seed)
    {
        var random = new VectorCorpus.TestRandom(seed);

        for (var i = 0; i < Corpus; i += 5)
        {
            var id = DurableIndexHarness.Id(i);
            var vector = Perturb(source[id], ref random);
            source.Set(id, vector);
            await index.UpsertAsync(id, vector);
        }

        for (var i = 3; i < Corpus; i += 10)
        {
            var id = DurableIndexHarness.Id(i);
            source.Remove(id);
            await index.RemoveAsync(id);
        }

        for (var i = 0; i < additions.Length; i++)
        {
            var id = DurableIndexHarness.Id(Corpus + i);
            source.Set(id, additions[i]);
            await index.UpsertAsync(id, additions[i]);
        }
    }

    [Test]
    public async Task Recall_holds_above_the_clustered_floor_after_a_churn_workload_with_no_rebuild()
    {
        // Additions come from the tail of the same generated corpus, so they are
        // drawn from the distribution the index was built over rather than from a
        // fresh set of cluster centres.
        var all = VectorCorpus.Clustered(Corpus + (Corpus / 10), Dimensions, Clusters, seed: 91);
        var corpus = all[..Corpus];
        var additions = all[Corpus..];
        var queries = VectorCorpus.Clustered(40, Dimensions, Clusters, seed: 94);

        var store = new InMemoryVectorIndexStore();
        var source = Source(corpus);
        var options = Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var generation = index.Generation;
        var before = Recall(index, source, queries);

        await ChurnAsync(index, source, additions, seed: 92);
        await index.FlushAsync();

        var after = Recall(index, source, queries);

        TestContext.Out.WriteLine(
            $"recall@{K} before churn {before:F4}, after churn {after:F4}, corpus {source.Ids.Count}, "
            + $"updates since training {index.UpdatesSinceTraining}");

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.GreaterThanOrEqualTo(ClusteredRecallFloor));
            Assert.That(after, Is.GreaterThanOrEqualTo(ClusteredRecallFloor),
                "Incremental maintenance must hold the published recall floor without a rebuild.");
            Assert.That(index.Generation, Is.EqualTo(generation),
                "The generation must not have moved, so no rebuild happened.");
            Assert.That(index.Count, Is.EqualTo(source.Ids.Count));
        });
    }

    [Test]
    public async Task Recall_holds_above_the_unclustered_floor_after_a_churn_workload()
    {
        var all = VectorCorpus.Uniform(Corpus + (Corpus / 10), Dimensions, seed: 95);
        var corpus = all[..Corpus];
        var additions = all[Corpus..];
        var queries = VectorCorpus.Uniform(40, Dimensions, seed: 98);

        var store = new InMemoryVectorIndexStore();
        var source = Source(corpus);

        var index = await DurableIndexHarness.BuiltAsync(store, source, Options());
        await ChurnAsync(index, source, additions, seed: 96);
        await index.FlushAsync();

        var after = Recall(index, source, queries);
        TestContext.Out.WriteLine($"unclustered recall@{K} after churn {after:F4}");

        Assert.That(after, Is.GreaterThanOrEqualTo(UnclusteredRecallFloor));
    }

    [Test]
    public async Task A_corpus_that_drifts_off_its_partitioning_is_repaired_by_retraining_not_by_rebuilding()
    {
        // The adversarial case incremental maintenance cannot fix on its own: a
        // fifth of the corpus is replaced by vectors drawn around a completely
        // different set of cluster centres, so the trained cells stop describing
        // the data. Every record stays valid and every answer stays honest - the
        // index simply has to probe further to find the same neighbours - which
        // is exactly why the loss is quiet and needs a signal rather than an
        // error.
        var corpus = VectorCorpus.Clustered(Corpus, Dimensions, Clusters, seed: 81);
        var relocated = VectorCorpus.Clustered((Corpus / 5) + 1, Dimensions, Clusters, seed: 82);
        var queries = VectorCorpus.Clustered(40, Dimensions, Clusters, seed: 83);

        var store = new InMemoryVectorIndexStore();
        var source = Source(corpus);
        var options = Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var generation = index.Generation;

        for (var i = 0; i < Corpus; i += 5)
        {
            var id = DurableIndexHarness.Id(i);
            var vector = relocated[i / 5];
            source.Set(id, vector);
            await index.UpsertAsync(id, vector);
        }

        var drifted = Recall(index, source, queries);
        var updates = index.UpdatesSinceTraining;

        await index.RetrainAsync();
        var repaired = Recall(index, source, queries);

        TestContext.Out.WriteLine(
            $"recall@{K} after distribution shift {drifted:F4}, after retraining {repaired:F4}; "
            + $"{updates} updates against a corpus of {index.Count}");

        Assert.Multiple(() =>
        {
            Assert.That(updates, Is.EqualTo(Corpus / 5),
                "The drift signal must count exactly the updates applied since training.");
            Assert.That(index.UpdatesSinceTraining, Is.Zero, "Retraining resets the signal it repairs.");
            Assert.That(repaired, Is.GreaterThanOrEqualTo(ClusteredRecallFloor),
                "Retraining over the resident corpus must restore the published floor.");
            Assert.That(repaired, Is.GreaterThan(drifted));
            Assert.That(index.Generation, Is.GreaterThan(generation),
                "A retrain changes every cell's membership, so it commits a fresh generation.");
        });
    }

    [Test]
    public async Task A_retrained_index_reloads_from_its_new_generation_alone()
    {
        var corpus = VectorCorpus.Clustered(1_500, Dimensions, Clusters, seed: 84);
        var queries = VectorCorpus.Clustered(10, Dimensions, Clusters, seed: 85);

        var store = new InMemoryVectorIndexStore();
        var source = Source(corpus);
        var options = Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var superseded = index.Generation;
        await index.RetrainAsync();

        var expected = queries.Select(query => DurableIndexHarness.SearchIds(index, query, K)).ToList();
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(1_500));
            Assert.That(queries.Select(query => DurableIndexHarness.SearchIds(reopened, query, K)).ToList(),
                Is.EqualTo(expected));
            Assert.That(
                store.KeysWithPrefix(VectorIndexStorageKeys.GenerationPrefix(options.KeyPrefix, superseded)),
                Is.Empty,
                "The superseded generation is reclaimed once the manifest names the new one.");
        });
    }

    [Test]
    public async Task Retraining_is_refused_on_a_lazily_loaded_index()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(300);
        var options = DurableIndexHarness.Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        Assert.That(async () => await lazy.RetrainAsync(), Throws.InvalidOperationException);
    }

    [Test]
    public async Task Churn_survives_a_restart_intact()
    {
        var all = VectorCorpus.Clustered(Corpus + (Corpus / 10), Dimensions, Clusters, seed: 91);
        var corpus = all[..Corpus];
        var additions = all[Corpus..];
        var queries = VectorCorpus.Clustered(20, Dimensions, Clusters, seed: 94);

        var store = new InMemoryVectorIndexStore();
        var source = Source(corpus);
        var options = Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        await ChurnAsync(index, source, additions, seed: 92);
        await index.FlushAsync();

        var expected = queries.Select(query => DurableIndexHarness.SearchIds(index, query, K)).ToList();

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);
        var actual = queries.Select(query => DurableIndexHarness.SearchIds(reopened, query, K)).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(index.Count));
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task Re_embedding_one_identifier_keeps_its_key_and_replaces_its_vector()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(400);
        var options = DurableIndexHarness.Options();
        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        var id = DurableIndexHarness.Id(17);
        Assert.That(index.TryGetKey(id, out var before), Is.True);

        var replacement = VectorCorpus.Clustered(1, DurableIndexHarness.Dimensions, 4, seed: 500)[0];
        Assert.That(await index.UpsertAsync(id, replacement), Is.True, "Re-embedding replaces rather than adds.");
        await index.FlushAsync();

        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(index.TryGetKey(id, out var after), Is.True);
            Assert.That(after, Is.EqualTo(before), "A stable key is what makes a re-embed an in-place update.");
            Assert.That(reopened.Count, Is.EqualTo(400), "The corpus size must not change on a re-embed.");
        });

        var results = new VectorSearchResult[1];
        var found = reopened.Search(replacement, results, out _);
        Assert.That(found, Is.EqualTo(1));
        Assert.That(reopened.TryGetId(results[0].Key, out var top) ? top : null, Is.EqualTo(id),
            "The replacement vector must be what the index now holds for that identifier.");
    }

    [Test]
    public async Task A_flush_after_a_few_updates_rewrites_only_the_cells_that_moved()
    {
        var corpus = VectorCorpus.Clustered(2_000, Dimensions, Clusters, seed: 71);
        var store = new InMemoryVectorIndexStore();
        var source = Source(corpus);
        var options = Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var partitions = index.Status.PartitionCount;
        Assert.That(partitions, Is.GreaterThan(4), "The fixture needs several cells for the claim to mean anything.");

        var baseline = store.Writes;
        await index.FlushAsync();
        var clean = store.Writes - baseline;

        var replacement = VectorCorpus.Clustered(1, Dimensions, Clusters, seed: 72)[0];
        await index.UpsertAsync(DurableIndexHarness.Id(5), replacement);

        baseline = store.Writes;
        await index.FlushAsync();
        var incremental = store.Writes - baseline;

        TestContext.Out.WriteLine(
            $"{partitions} partitions: an unchanged flush costs {clean} writes, a one-update flush costs {incremental}");

        Assert.Multiple(() =>
        {
            Assert.That(clean, Is.EqualTo(1),
                "A flush with nothing dirty writes only the manifest.");
            Assert.That(incremental, Is.LessThan(partitions),
                "A single update must not cost a write per partition, or persistence is not incremental at all.");
        });
    }

    [Test]
    public async Task Inserting_after_the_build_extends_the_index_without_retraining()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(400);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var generation = index.Generation;
        var partitions = index.Status.PartitionCount;

        var additions = VectorCorpus.Clustered(50, DurableIndexHarness.Dimensions, 8, seed: 600);
        for (var i = 0; i < additions.Length; i++)
        {
            var id = DurableIndexHarness.Id(1_000 + i);
            source.Set(id, additions[i]);
            Assert.That(await index.UpsertAsync(id, additions[i]), Is.False, "A new identifier is an add.");
        }

        await index.FlushAsync();
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(index.Generation, Is.EqualTo(generation));
            Assert.That(index.Status.PartitionCount, Is.EqualTo(partitions));
            Assert.That(reopened.Count, Is.EqualTo(450));
        });
    }

    [Test]
    public async Task Deleting_every_vector_leaves_a_loadable_empty_index()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        foreach (var id in source.Ids.ToArray())
        {
            await index.RemoveAsync(id);
        }

        await index.FlushAsync();
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(index.Count, Is.Zero);
            Assert.That(reopened.Count, Is.Zero);
            Assert.That(reopened.Search(source[DurableIndexHarness.Id(0)], new VectorSearchResult[5], out _),
                Is.Zero);
        });
    }

    [Test]
    public async Task An_update_made_before_the_build_finishes_is_not_lost_by_the_build()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(600);
        var options = DurableIndexHarness.Options(ingestBatchSize: 64);

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        var id = "zzz-late-arrival";
        var vector = VectorCorpus.Clustered(1, DurableIndexHarness.Dimensions, 8, seed: 700)[0];
        source.Set(id, vector);
        await index.UpsertAsync(id, vector);

        await index.RunBuildAsync();
        await index.FlushAsync();
        var reopened = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reopened.Count, Is.EqualTo(601));
            Assert.That(reopened.TryGetKey(id, out _), Is.True,
                "A vector written by the host mid-build must survive the build that was running around it.");
        });
    }
}
