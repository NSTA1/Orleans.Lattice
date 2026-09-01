using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for <see cref="RepoContextStore.ListRepoIdsAsync"/>, the
/// summary-free repository walk.
/// <para>
/// A full <see cref="RepoContextStore.ListReposAsync"/> summary carries
/// <c>embeddedVectorCount</c>, which is scanned from the vector-membership tree -
/// the largest tree in the store, and on a real repository over 81,000 records.
/// The retrieval warmup runs at startup, uses nothing but the repository id, and
/// was paying that scan per repository while the vector trees were still replaying
/// and the scan was at its slowest. It timed out and failed the warmup on a real
/// deployment (issue #1819).
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreListRepoIdsTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async Task SeedRepoMarkerAsync(
        RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        var bytes = serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        var structural = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await structural.SetAsync(RepoContextKeys.Repo(repoId), bytes, ct);
    }

    [Test]
    public async Task No_repositories_yields_an_empty_list()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        Assert.That(await store.ListRepoIdsAsync(Ct), Is.Empty,
            "A store with nothing registered enumerates no repositories.");
    }

    [Test]
    public async Task Every_registered_repository_is_listed_once()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        await SeedRepoMarkerAsync(harness, "alpha", Ct);
        await SeedRepoMarkerAsync(harness, "beta", Ct);

        var ids = await store.ListRepoIdsAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.EquivalentTo(new[] { "alpha", "beta" }));
            Assert.That(ids, Has.Count.EqualTo(2), "each exactly once, not once per subtree key.");
        });
    }

    /// <summary>
    /// The id walk must agree with the full listing, or the cheap path would be a
    /// different answer rather than the same answer computed cheaply.
    /// </summary>
    [Test]
    public async Task The_id_walk_agrees_with_the_full_listing()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        // A sibling whose id extends another after a separator ordering below '/'
        // is the case the walk's advance logic exists for, so include one.
        await SeedRepoMarkerAsync(harness, "svc", Ct);
        await SeedRepoMarkerAsync(harness, "svc-api", Ct);

        var ids = await store.ListRepoIdsAsync(Ct);
        var full = await store.ListReposAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.EquivalentTo(full.Repos.Select(static r => r.RepoId)),
                "The cheap walk finds exactly the repositories the full listing does,");
            Assert.That(ids, Does.Contain("svc-api"),
                "including a sibling that sorts inside the preceding repository's key gap.");
        });
    }

    /// <summary>
    /// The point of the separate path: it must not read the membership tree at all.
    /// Asserted by embedding a source, then checking the id walk still succeeds and
    /// reports nothing about it while the full listing does - so a future change
    /// that reinstated the summary here would be visible.
    /// </summary>
    [Test]
    public async Task The_id_walk_reports_no_embedding_information()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        await SeedRepoMarkerAsync(harness, "acme", Ct);
        await writer.AddMembersAsync("acme", new[] { RepoContextKeys.File("acme", "src/A.cs") }, Ct);

        var ids = await store.ListRepoIdsAsync(Ct);
        var full = await store.ListReposAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.EquivalentTo(new[] { "acme" }),
                "The id walk returns bare ids - there is no count on them to be stale or slow.");
            Assert.That(full.Repos.Single().EmbeddedVectorCount, Is.EqualTo(1),
                "while the full listing still reports the embedded count, so the cheap path "
                + "narrowed what is read rather than losing the diagnostic.");
        });
    }
}
