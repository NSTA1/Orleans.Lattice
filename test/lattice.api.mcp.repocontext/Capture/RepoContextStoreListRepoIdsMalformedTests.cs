using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration test for the defensive unparseable-key arm of
/// <see cref="RepoContextStore.ListRepoIdsAsync"/>. A key that sits inside the
/// repository namespace range but does not parse to a repository id (an empty
/// repo-id segment) must be stepped over rather than added or looped on forever, so
/// a listing that also contains one valid repository marker returns only the valid
/// id.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreListRepoIdsMalformedTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task List_repo_ids_steps_over_an_unparseable_key()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var structural = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);

        // "repo//malformed" is inside the "repo/" scan range and sorts before
        // "repo/acme" ('/' orders below any id character), so the listing hits it
        // first. Its empty repo-id segment fails the key parser, exercising the
        // step-past-and-continue arm before the valid marker is reached.
        await structural.SetAsync("repo//malformed", new byte[] { 1 }, Ct);
        await structural.SetAsync(RepoContextKeys.Repo("acme"), new byte[] { 1 }, Ct);

        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var repoIds = await store.ListRepoIdsAsync(Ct);

        Assert.That(repoIds, Is.EqualTo(new[] { "acme" }),
            "The unparseable key yields no id and is skipped; only the valid repository is listed.");
    }
}
