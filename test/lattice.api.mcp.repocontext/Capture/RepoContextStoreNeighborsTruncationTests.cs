using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration test for the neighbor-cap branch of
/// <see cref="RepoContextStore.NeighborsAsync"/>. A seed memory entry carrying two
/// valid link targets, walked with a node budget of one, must return the first
/// neighbor and then stop, reporting the walk as truncated rather than exceeding
/// the requested cap. This pins the "count reached the clamped maximum" break that
/// only fires when a second in-budget target is available.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreNeighborsTruncationTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static async Task SeedFileAsync(RepoContextMcpHarness harness, string path, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<FileNode>>();
        var clock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var node = new FileNode { RepoId = RepoId, Path = path, Digest = RepoContextValues.Lww("d-" + path, clock) };
        await harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.File(RepoId, path), serializer.SerializeToArray(node), ct);
    }

    [Test]
    public async Task Neighbors_with_a_node_budget_below_the_link_count_truncates()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedFileAsync(harness, "a.cs", Ct);
        await SeedFileAsync(harness, "b.cs", Ct);
        var store = Store(harness);

        var links = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            ["related"] = new[] { RepoContextKeys.File(RepoId, "a.cs"), RepoContextKeys.File(RepoId, "b.cs") },
        };
        var seed = await store.RememberAsync(
            RepoId, "concepts", id: "c1", MemoryKind.Note, title: "seed", body: "b",
            author: null, provenance: null, tags: null, addLinks: links, removeLinks: null, ttlSeconds: null, Ct);

        var result = await store.NeighborsAsync(seed.Key, relation: null, depth: 1, maxNodes: 1, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Exists, Is.True, "The seed entry exists.");
            Assert.That(result.Truncated, Is.True,
                "A second in-budget target trips the neighbor cap and truncates the walk.");
            Assert.That(result.Neighbors, Has.Count.EqualTo(1),
                "Exactly one neighbor is returned before the cap stops the walk.");
        });
    }

    [Test]
    public async Task Neighbors_with_a_budget_at_the_link_count_does_not_truncate()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedFileAsync(harness, "a.cs", Ct);
        await SeedFileAsync(harness, "b.cs", Ct);
        var store = Store(harness);

        var links = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            ["related"] = new[] { RepoContextKeys.File(RepoId, "a.cs"), RepoContextKeys.File(RepoId, "b.cs") },
        };
        var seed = await store.RememberAsync(
            RepoId, "concepts", id: "c2", MemoryKind.Note, title: "seed", body: "b",
            author: null, provenance: null, tags: null, addLinks: links, removeLinks: null, ttlSeconds: null, Ct);

        var result = await store.NeighborsAsync(seed.Key, relation: null, depth: 1, maxNodes: 2, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Truncated, Is.False,
                "A budget that admits every target does not truncate.");
            Assert.That(result.Neighbors, Has.Count.EqualTo(2),
                "Both link targets are returned as neighbors.");
        });
    }
}
