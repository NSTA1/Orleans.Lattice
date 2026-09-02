using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The committed recall measurement for the wired-up approximate retrieval path,
/// taken against the exact oracle on the same corpus.
/// <para>
/// This is the acceptance evidence for making approximate retrieval the default.
/// The oracle is <see cref="RepoContextKnnRanker"/> - the same ranking kernel the
/// shipped exact scan uses - run over the identical candidate set, so it is
/// complete-recall by construction and the comparison measures the index rather
/// than a re-derivation of it. The measured path is
/// <see cref="AnnRepoContextSemanticIndex"/>, the index the search service
/// actually binds, so the figure is the one a caller receives rather than one
/// taken against the raw vector library beneath it.
/// </para>
/// <para>
/// Both corpora are generated from a fixed seed and the index trains from a fixed
/// seed, so the measurement is reproducible and carries no dependence on a clock,
/// an ordering, or a garbage collection.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnRecallTests
{
    private const string RepoId = "acme";
    private const int Dimensions = 32;
    private const int CorpusSize = 3_000;
    private const int QueryCount = 200;
    private const int K = 10;

    /// <summary>
    /// The published floor for a clustered corpus - real embedding geometry - as
    /// established by the vector package's own measurements. Documented as the
    /// contract the approximate default carries.
    /// </summary>
    private const double ClusteredFloor = 0.95d;

    /// <summary>
    /// The published floor for an adversarially unclustered corpus, where no
    /// partitioning can describe the data well. It is deliberately far lower: it
    /// is the worst case, not the expected one.
    /// </summary>
    private const double UnclusteredFloor = 0.55d;

    private static readonly EmbeddingSpaceTag Space =
        new("recall-model", Dimensions, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task Recall_at_ten_on_a_clustered_corpus_meets_the_published_floor()
    {
        var recall = await MeasureAsync(clustered: true, Ct);

        TestContext.Out.WriteLine(
            $"Clustered corpus: {CorpusSize} vectors, {Dimensions} dimensions, {QueryCount} queries, "
            + $"measured recall@{K} = {recall:F4} against a floor of {ClusteredFloor:F2}.");

        Assert.That(recall, Is.GreaterThanOrEqualTo(ClusteredFloor),
            "Approximate retrieval is the default, so its recall against the exact oracle is a shipped contract.");
    }

    [Test]
    public async Task Recall_at_ten_on_an_adversarial_unclustered_corpus_meets_the_published_floor()
    {
        var recall = await MeasureAsync(clustered: false, Ct);

        TestContext.Out.WriteLine(
            $"Unclustered corpus: {CorpusSize} vectors, {Dimensions} dimensions, {QueryCount} queries, "
            + $"measured recall@{K} = {recall:F4} against a floor of {UnclusteredFloor:F2}.");

        Assert.That(recall, Is.GreaterThanOrEqualTo(UnclusteredFloor),
            "The adversarial floor is the worst case the published contract admits, and it must hold too.");
    }

    [Test]
    public async Task The_exact_oracle_is_a_strict_upper_bound_the_measurement_is_taken_against()
    {
        // A guard against a vacuous measurement: if the oracle returned fewer than K
        // results, or the two paths shared an implementation, a recall of 1.0 would
        // prove nothing. This asserts the oracle really does rank the whole corpus.
        var factory = new InMemoryAnnBackingFactory();
        var source = factory.For(RepoId, Space).Source;
        Seed(source, clustered: true);

        var query = Normalize(Cluster(0, new Random(99)));
        var oracle = RepoContextKnnRanker.Rank(query, Space, source.Candidates(), K);

        Assert.Multiple(() =>
        {
            Assert.That(source.Candidates(), Has.Count.EqualTo(CorpusSize),
                "The oracle ranks the whole corpus, which is what makes it complete-recall.");
            Assert.That(oracle, Has.Count.EqualTo(K), "The oracle returns a full result set to compare against.");
        });
    }

    private static async Task<double> MeasureAsync(bool clustered, CancellationToken cancellationToken)
    {
        var factory = new InMemoryAnnBackingFactory();
        var source = factory.For(RepoId, Space).Source;
        Seed(source, clustered);

        // Production shaping: the partition count and probe budget are chosen by the
        // index from the corpus, exactly as a deployment would leave them. Only the
        // background build is disabled, so the measurement never races a task.
        using var registry = new RepoContextAnnIndexRegistry(
            factory,
            new RepoContextAnnOptions(),
            NullLogger<RepoContextAnnIndexRegistry>.Instance);
        await registry.EnsureBuiltAsync(RepoId, Space, cancellationToken);

        var index = new AnnRepoContextSemanticIndex(
            registry,
            ThrowingExactIndex.Instance,
            NullLogger<AnnRepoContextSemanticIndex>.Instance);

        var candidates = source.Candidates();
        var queries = new Random(4_242);
        var hits = 0;
        var possible = 0;

        for (var q = 0; q < QueryCount; q++)
        {
            var query = clustered
                ? Normalize(Cluster(queries.Next(ClusterCount), queries))
                : Normalize(Uniform(queries));

            var expected = RepoContextKnnRanker.Rank(query, Space, candidates, K);
            var actual = await index.SearchAsync(RepoId, query, Space, K, cancellationToken);

            var truth = new HashSet<string>(expected.Select(match => match.VectorId), StringComparer.Ordinal);
            foreach (var match in actual)
            {
                if (truth.Contains(match.VectorId))
                {
                    hits++;
                }
            }

            possible += expected.Count;
        }

        return possible == 0 ? 0d : (double)hits / possible;
    }

    private const int ClusterCount = 24;

    private static void Seed(InMemoryRepoContextVectorSource source, bool clustered)
    {
        var random = new Random(1_830);
        for (var i = 0; i < CorpusSize; i++)
        {
            var vector = clustered ? Cluster(i % ClusterCount, random) : Uniform(random);
            source.Set(
                $"vec-{i:D6}",
                RepoContextKeys.File(RepoId, $"src/File{i}.cs"),
                Normalize(vector));
        }
    }

    /// <summary>
    /// A vector drawn near one of a fixed set of centres, which is what real
    /// embedding geometry looks like: documents cluster by topic.
    /// </summary>
    private static float[] Cluster(int cluster, Random random)
    {
        var centre = new Random(7_000 + cluster);
        var vector = new float[Dimensions];
        for (var d = 0; d < Dimensions; d++)
        {
            vector[d] = (float)(centre.NextDouble() - 0.5d) + (float)((random.NextDouble() - 0.5d) * 0.25d);
        }

        return vector;
    }

    /// <summary>
    /// A vector drawn uniformly, with no cluster structure at all. No partitioning
    /// can describe such a corpus well, which is exactly why it is the adversarial
    /// case rather than the expected one.
    /// </summary>
    private static float[] Uniform(Random random)
    {
        var vector = new float[Dimensions];
        for (var d = 0; d < Dimensions; d++)
        {
            vector[d] = (float)(random.NextDouble() - 0.5d);
        }

        return vector;
    }

    private static float[] Normalize(float[] vector)
    {
        var sum = 0d;
        for (var i = 0; i < vector.Length; i++)
        {
            sum += vector[i] * (double)vector[i];
        }

        var norm = Math.Sqrt(sum);
        if (norm <= 0d)
        {
            vector[0] = 1f;
            return vector;
        }

        for (var i = 0; i < vector.Length; i++)
        {
            vector[i] = (float)(vector[i] / norm);
        }

        return vector;
    }

    /// <summary>
    /// The fall-back the measurement must never reach. Falling through to the exact
    /// scan would measure the oracle against itself and report a perfect score, so
    /// the fall-back throws instead of quietly making the test vacuous.
    /// </summary>
    private sealed class ThrowingExactIndex : IRepoContextSemanticIndex
    {
        internal static readonly ThrowingExactIndex Instance = new();

        public string RetrievalPath => RepoContextRetrievalPath.SemanticExact;

        public Task<IReadOnlyList<RepoContextVectorMatch>> SearchAsync(
            string repoId,
            ReadOnlyMemory<float> query,
            EmbeddingSpaceTag querySpace,
            int k,
            CancellationToken cancellationToken)
            => throw new InvalidOperationException(
                "The recall measurement must be answered by the approximate index, never by the exact oracle.");
    }
}
