using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests that the warm <see cref="RepoContextVectorCache"/> behind
/// <see cref="ExactKnnSemanticIndex"/> is transparent: a cache hit reproduces
/// byte-identical ranking and recall to the uncached store scan, a local write
/// invalidates it, and the disabled (zero-TTL) cache reproduces the original
/// per-query scan exactly.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and the reserved vector trees) via <see cref="RepoContextMcpHarness"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class ExactKnnSemanticIndexCacheTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpace Space = new("fake-embed-v1", 4, normalized: true);
    private static readonly EmbeddingSpaceTag SpaceTag = EmbeddingSpaceTag.FromSpace(Space);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async Task SeedAsync(RepoContextVectorWriter writer, CancellationToken ct)
    {
        await writer.StoreAsync(RepoId, RepoContextKeys.File(RepoId, "src/A.cs"), Space,
            new ReadOnlyMemory<float>[] { new float[] { 1f, 0f, 0f, 0f } }, ct);
        await writer.StoreAsync(RepoId, RepoContextKeys.File(RepoId, "src/B.cs"), Space,
            new ReadOnlyMemory<float>[] { new float[] { 0f, 1f, 0f, 0f } }, ct);
        await writer.StoreAsync(RepoId, RepoContextKeys.File(RepoId, "src/C.cs"), Space,
            new ReadOnlyMemory<float>[] { new float[] { 0f, 0f, 1f, 0f } }, ct);
    }

    private static ExactKnnSemanticIndex Uncached(RepoContextMcpHarness harness)
        => new(
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions { VectorCacheTtl = TimeSpan.Zero }));

    [Test]
    public async Task A_cache_hit_returns_the_same_results_as_the_first_scan()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var index = harness.Services.GetRequiredService<IRepoContextSemanticIndex>();
        await SeedAsync(writer, Ct);

        var query = new float[] { 1f, 0f, 0f, 0f };
        var first = await index.SearchAsync(RepoId, query, SpaceTag, 3, Ct);   // miss, populates cache
        var second = await index.SearchAsync(RepoId, query, SpaceTag, 3, Ct);  // hit

        Assert.That(second, Is.EqualTo(first).AsCollection,
            "A warm cache hit reproduces the exact ranking and recall of the first scan.");
    }

    [Test]
    public async Task The_cached_index_matches_the_uncached_index_result()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var cached = harness.Services.GetRequiredService<IRepoContextSemanticIndex>();
        await SeedAsync(writer, Ct);

        var query = new float[] { 0f, 1f, 0f, 0f };
        _ = await cached.SearchAsync(RepoId, query, SpaceTag, 3, Ct); // warm the cache
        var cachedResult = await cached.SearchAsync(RepoId, query, SpaceTag, 3, Ct);
        var uncachedResult = await Uncached(harness).SearchAsync(RepoId, query, SpaceTag, 3, Ct);

        Assert.That(cachedResult, Is.EqualTo(uncachedResult).AsCollection,
            "The cache-backed index and a scan-every-time index rank identically.");
    }

    [Test]
    public async Task A_write_invalidates_the_cache_so_a_later_search_sees_the_new_vector()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var index = harness.Services.GetRequiredService<IRepoContextSemanticIndex>();
        await SeedAsync(writer, Ct);

        var query = new float[] { 0f, 0f, 0f, 1f };
        var before = await index.SearchAsync(RepoId, query, SpaceTag, 10, Ct); // warms cache with 3 vectors

        // A new source lands after the cache was warmed; the writer's invalidation
        // must drop the stale set so the next search observes the fourth vector.
        await writer.StoreAsync(RepoId, RepoContextKeys.File(RepoId, "src/D.cs"), Space,
            new ReadOnlyMemory<float>[] { new float[] { 0f, 0f, 0f, 1f } }, Ct);
        var after = await index.SearchAsync(RepoId, query, SpaceTag, 10, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(before, Has.Count.EqualTo(3), "The warmed search saw the three seeded vectors.");
            Assert.That(after, Has.Count.EqualTo(4), "After the write invalidated the cache the fourth vector is visible.");
        });
    }
}
