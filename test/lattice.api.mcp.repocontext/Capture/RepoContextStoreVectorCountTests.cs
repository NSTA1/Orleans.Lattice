using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for the durable embedded-vector tally that
/// <see cref="RepoContextStore.ListReposAsync"/> reports as
/// <see cref="RepoContextRepoSummary.EmbeddedVectorCount"/>. The count is read
/// from the store of record (the vector-membership add-wins set the vector writer
/// maintains), never from a run's in-flight progress, so it is a restart-durable
/// diagnostic: it is exactly the number of files whose embedding has landed and
/// converges on the repository's file count as vectorisation completes.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and the reserved structural and vector trees) via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreVectorCountTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static (RepoContextStore Store, RepoContextVectorWriter Writer) Resolve(RepoContextMcpHarness harness)
        => (harness.Services.GetRequiredService<RepoContextStore>(),
            harness.Services.GetRequiredService<RepoContextVectorWriter>());

    private static async Task SeedRepoMarkerAsync(RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        // ListReposAsync discovers a repository from its structural keys, so a
        // marker must exist for the repo to be enumerated at all. A default node
        // (identity only) is enough - the tally under test comes from the
        // vector-membership tree, not this marker.
        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        var bytes = serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        var structural = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await structural.SetAsync(RepoContextKeys.Repo(repoId), bytes, ct);
    }

    private static async Task<long> EmbeddedVectorCountAsync(RepoContextStore store, string repoId, CancellationToken ct)
    {
        var list = await store.ListReposAsync(ct);
        var summary = list.Repos.Single(r => r.RepoId == repoId);
        return summary.EmbeddedVectorCount;
    }

    [Test]
    public async Task EmbeddedVectorCount_is_zero_for_a_repo_with_no_embeddings()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, _) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var count = await EmbeddedVectorCountAsync(store, RepoId, Ct);

        Assert.That(count, Is.EqualTo(0L),
            "A repository whose embeddings have not started landing reports a zero tally, not an error.");
    }

    [Test]
    public async Task EmbeddedVectorCount_matches_the_live_membership_set_size()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, writer) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);

        var count = await EmbeddedVectorCountAsync(store, RepoId, Ct);

        Assert.That(count, Is.EqualTo(2L),
            "The tally is the live size of the vector-membership set - one per file whose embedding landed.");
    }

    [Test]
    public async Task EmbeddedVectorCount_drops_when_a_source_is_retired()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var (store, writer) = Resolve(harness);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);

        var keyA = RepoContextKeys.File(RepoId, "src/A.cs");
        var keyB = RepoContextKeys.File(RepoId, "src/B.cs");
        await writer.AddMembersAsync(RepoId, new[] { keyA, keyB }, Ct);
        await writer.RetireAsync(RepoId, keyA, Ct);

        var count = await EmbeddedVectorCountAsync(store, RepoId, Ct);

        Assert.That(count, Is.EqualTo(1L),
            "Retiring a deleted source observed-removes it from the set, so the tally stays honest.");
    }
}
