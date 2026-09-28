using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The stack-space ceiling on the lazy-search fast path. A query wanting more
/// probes than fit in the fixed stack buffer must decline the fast path rather
/// than slice past the end of it.
/// </summary>
/// <remarks>
/// The fork's cold side needs a differently-configured index, not merely a
/// different query: every other lazy fixture configures a handful of probes, so
/// the ceiling is never approached and the decline was never taken. Removing the
/// check entirely turns the very next line - which slices the fixed buffer to the
/// wanted length - into an out-of-range slice, so this is a guard whose absence
/// throws rather than one that merely tidies.
/// </remarks>
[TestFixture]
public sealed class DurableVectorIndexResidentProbeLimitTests
{
    private const int Corpus = 1_600;
    private const int Partitions = 80;
    private const int K = 5;

    // Mirrors DurableVectorIndex.ResidentProbeStackLimit, which is private. The
    // boundary pair below fails loudly if the two ever drift apart: the
    // at-the-limit case would stop exercising the fast path.
    private const int StackLimit = 64;

    private static DurableVectorIndexOptions Options(int probes) => new()
    {
        KeyPrefix = "probe-limit/",
        MaxItemsPerChunk = 64,
        IngestBatchSize = 512,
        Index = new VectorIndexOptions
        {
            Dimensions = DurableIndexHarness.Dimensions,
            PartitionCount = Partitions,
            Probes = probes,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 2_048,
        },
    };

    [TestCase(StackLimit, TestName = "At the stack limit the fast path is taken")]
    [TestCase(StackLimit + 1, TestName = "Above the stack limit the fast path declines")]
    public async Task A_lazy_search_answers_identically_on_both_sides_of_the_probe_stack_limit(int probes)
    {
        Assert.That(Partitions, Is.GreaterThan(StackLimit),
            "The index must have enough partitions for the probe count to reach the limit at all.");

        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options(probes);

        var resident = await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var results = new VectorSearchResult[K];
        for (var i = 0; i < Corpus; i += 211)
        {
            var query = source[DurableIndexHarness.Id(i)];
            var expected = DurableIndexHarness.SearchResults(resident, query, K);

            var outcome = await lazy.SearchAsync(query, results);

            Assert.That(results.AsSpan(0, outcome.Count).ToArray(), Is.EqualTo(expected),
                "Declining the fast path takes the asynchronous route, which must change nothing "
                + "about the answer.");
            Assert.That(outcome.Mode, Is.EqualTo(VectorSearchMode.Approximate));
        }
    }

    [Test]
    public async Task A_warm_index_above_the_probe_stack_limit_keeps_answering_correctly()
    {
        // Warming matters: with every probed cell resident, the fast path would
        // answer every query if it were reached. Above the limit it is not, so
        // this is the repeated-query shape that would expose a decline which
        // corrupted state on the way out rather than one that merely costs a
        // frame.
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options(StackLimit + 1);

        var resident = await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var query = source[DurableIndexHarness.Id(7)];
        var expected = DurableIndexHarness.SearchResults(resident, query, K);
        var results = new VectorSearchResult[K];

        for (var repeat = 0; repeat < 4; repeat++)
        {
            var outcome = await lazy.SearchAsync(query, results);
            Assert.That(results.AsSpan(0, outcome.Count).ToArray(), Is.EqualTo(expected),
                $"Repeat {repeat} diverged, so the declined path is not idempotent.");
        }
    }
}
