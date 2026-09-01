using Orleans.Lattice.Vector;

namespace Orleans.Lattice.Samples.VectorSearch;

/// <summary>
/// Demonstrates the headline capability of <c>Orleans.Lattice.Vector</c>: an
/// approximate nearest-neighbour search whose cost is sub-linear in the corpus,
/// which reports honestly how each answer was produced, and whose accuracy is
/// measured against an exact oracle rather than asserted.
/// </summary>
internal static class Program
{
    private const int Dimensions = 64;
    private const int Clusters = 40;
    private const int VectorsPerCluster = 250;
    private const int TopK = 10;
    private const int QueryCount = 50;

    private static void Main()
    {
        Console.WriteLine("Orleans.Lattice.Vector - approximate nearest-neighbour search");
        Console.WriteLine();

        // A clustered corpus, which is what a real embedding model produces.
        var rng = new Random(20260831);
        var corpus = BuildClusteredCorpus(rng);
        Console.WriteLine($"Corpus: {corpus.Count} vectors of {Dimensions} dimensions in {Clusters} clusters.");

        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = Dimensions,
            Metric = VectorDistanceMetric.Cosine,
        });

        // Reserving up front makes the bulk insert allocation-free.
        index.EnsureCapacity(corpus.Count);
        for (var i = 0; i < corpus.Count; i++)
            index.Add(i, corpus[i]);

        // Before training there is no partitioning, so the index answers exactly
        // by exhaustive scan. That is correct, just not yet sub-linear - and it
        // says so rather than pretending otherwise.
        ReportOneQuery(index, corpus[0], "before Train()");

        index.Train();
        Console.WriteLine($"Trained: {index.PartitionCount} partitions, probing {index.Probes} per query "
            + $"({(double)index.Probes / index.PartitionCount:P1} of the corpus).");
        Console.WriteLine();

        ReportOneQuery(index, corpus[0], "after Train()");

        // Accuracy is measured against a brute-force oracle over the same data,
        // not assumed. This is the number the package publishes.
        var recall = MeasureRecall(index, corpus, rng);
        Console.WriteLine($"Measured recall@{TopK} over {QueryCount} queries: {recall:F4}");
        Console.WriteLine($"Published floor for a clustered corpus: 0.95 - {(recall >= 0.95 ? "met" : "NOT met")}.");
        Console.WriteLine();

        // Deletes are first class: a retired vector never comes back, and there
        // is no tombstone to compact away later.
        var removed = index.Remove(0);
        Span<VectorSearchResult> afterDelete = stackalloc VectorSearchResult[TopK];
        var afterCount = index.Search(corpus[0], afterDelete, out _);
        var stillPresent = false;
        for (var i = 0; i < afterCount; i++)
            stillPresent |= afterDelete[i].Key == 0;

        Console.WriteLine($"Removed key 0: {removed}. Still returned by its own query: {stillPresent}.");
    }

    /// <summary>
    /// Runs one query and reports which path answered it. The mode is the
    /// per-response honesty signal: an approximate answer is never presented as
    /// an exact one.
    /// </summary>
    private static void ReportOneQuery(VectorIndex index, float[] query, string label)
    {
        Span<VectorSearchResult> results = stackalloc VectorSearchResult[TopK];
        var found = index.Search(query, results, out var mode);

        var description = mode == VectorSearchMode.Exhaustive
            ? "exhaustive - every vector scored, so the answer is exact"
            : "approximate - only the probed partitions were scored";

        Console.WriteLine($"Query {label}: state={index.State}, {found} hits, mode={mode} ({description}).");
    }

    /// <summary>
    /// Computes recall@k against an exact brute-force oracle over the same
    /// corpus, using the same ordering the index uses so tie-breaking cannot
    /// confound the comparison.
    /// </summary>
    private static double MeasureRecall(VectorIndex index, List<float[]> corpus, Random rng)
    {
        var hits = 0;
        var total = 0;
        Span<VectorSearchResult> approximate = stackalloc VectorSearchResult[TopK];

        for (var q = 0; q < QueryCount; q++)
        {
            var query = corpus[rng.Next(corpus.Count)];

            var exact = ExactTopK(corpus, query);
            var found = index.Search(query, approximate, out _);

            for (var i = 0; i < found; i++)
            {
                if (exact.Contains(approximate[i].Key))
                    hits++;
            }

            total += exact.Count;
        }

        return total == 0 ? 0d : (double)hits / total;
    }

    /// <summary>Brute-force top-k by cosine similarity: the correctness oracle.</summary>
    private static HashSet<long> ExactTopK(List<float[]> corpus, float[] query)
    {
        var scored = new List<(long Key, float Score)>(corpus.Count);
        for (var i = 0; i < corpus.Count; i++)
            scored.Add((i, VectorSimilarity.Cosine(query, corpus[i])));

        scored.Sort(static (a, b) => b.Score != a.Score
            ? b.Score.CompareTo(a.Score)
            : a.Key.CompareTo(b.Key));

        var top = new HashSet<long>();
        for (var i = 0; i < TopK && i < scored.Count; i++)
            top.Add(scored[i].Key);

        return top;
    }

    /// <summary>
    /// Builds a clustered corpus. Real embeddings are strongly clustered, which
    /// is exactly the structure a partitioned index exploits.
    /// </summary>
    private static List<float[]> BuildClusteredCorpus(Random rng)
    {
        var corpus = new List<float[]>(Clusters * VectorsPerCluster);

        for (var c = 0; c < Clusters; c++)
        {
            var centre = new float[Dimensions];
            for (var d = 0; d < Dimensions; d++)
                centre[d] = (float)(rng.NextDouble() * 2d - 1d);

            for (var v = 0; v < VectorsPerCluster; v++)
            {
                var vector = new float[Dimensions];
                for (var d = 0; d < Dimensions; d++)
                    vector[d] = centre[d] + (float)((rng.NextDouble() - 0.5d) * 0.2d);

                corpus.Add(vector);
            }
        }

        return corpus;
    }
}
