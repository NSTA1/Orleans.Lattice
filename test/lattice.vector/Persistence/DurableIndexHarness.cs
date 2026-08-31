using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Shared setup for the durable-index fixtures: a deterministic corpus, a source
/// over it, and options small enough that a test exercises several chunks and
/// several partitions without needing a large corpus.
/// </summary>
internal static class DurableIndexHarness
{
    internal const int Dimensions = 16;

    internal static ListVectorSource Source(
        int count, ulong seed = 11, int dimensions = Dimensions, int clusters = 8)
    {
        var corpus = VectorCorpus.Clustered(count, dimensions, clusters, seed);
        var source = new ListVectorSource(dimensions);
        for (var i = 0; i < count; i++)
        {
            source.Set(Id(i), corpus[i]);
        }

        return source;
    }

    /// <summary>
    /// Identifiers are fixed width so their ordinal order matches their numeric
    /// order, which is what makes a resumable cursor meaningful in a test.
    /// </summary>
    internal static string Id(int ordinal) => $"doc-{ordinal:D6}";

    internal static DurableVectorIndexOptions Options(
        int dimensions = Dimensions,
        int partitions = 8,
        int probes = 4,
        int maxItemsPerChunk = 64,
        int ingestBatchSize = 128,
        string prefix = "vidx/")
        => new()
        {
            KeyPrefix = prefix,
            MaxItemsPerChunk = maxItemsPerChunk,
            IngestBatchSize = ingestBatchSize,
            KeyReservationBlock = 64,
            Index = new VectorIndexOptions
            {
                Dimensions = dimensions,
                PartitionCount = partitions,
                Probes = probes,
                MinimumTrainingCount = 16,
                TrainingSampleSize = 2_048,
            },
        };

    internal static Task<DurableVectorIndex> OpenAsync(
        InMemoryVectorIndexStore store,
        ListVectorSource source,
        DurableVectorIndexOptions options,
        VectorIndexLoadMode loadMode = VectorIndexLoadMode.Full)
        => DurableVectorIndex.OpenAsync(store, source, options, loadMode);

    internal static async Task<DurableVectorIndex> BuiltAsync(
        InMemoryVectorIndexStore store, ListVectorSource source, DurableVectorIndexOptions options)
    {
        var index = await OpenAsync(store, source, options);
        await index.RunBuildAsync();
        return index;
    }

    /// <summary>
    /// The identifiers a search returns, resolved back through the index's own
    /// mapping. A result whose key does not resolve is reported as such rather
    /// than silently dropped, because an unresolvable key is exactly the failure
    /// a hashed identifier mapping would produce.
    /// </summary>
    internal static List<string> SearchIds(DurableVectorIndex index, float[] query, int k)
    {
        var results = new VectorSearchResult[k];
        var found = index.Search(query, results, out _);
        var ids = new List<string>(found);
        for (var i = 0; i < found; i++)
        {
            ids.Add(index.TryGetId(results[i].Key, out var id) ? id : $"<unmapped:{results[i].Key}>");
        }

        return ids;
    }

    internal static List<VectorSearchResult> SearchResults(DurableVectorIndex index, float[] query, int k)
    {
        var results = new VectorSearchResult[k];
        var found = index.Search(query, results, out _);
        return [.. results.AsSpan(0, found)];
    }
}
