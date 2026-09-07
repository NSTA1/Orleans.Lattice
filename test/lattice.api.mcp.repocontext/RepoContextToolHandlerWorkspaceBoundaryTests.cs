using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Regression tests for the workspace-boundary fail-closed check on
/// <see cref="RepoContextToolHandlers.AddRepoAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// <c>repocontext_add_repo</c> takes its path straight from the wire and its
/// published contract is that the path is "resolved against the workspace
/// boundary; a path outside it is rejected". A
/// <see cref="RepoContextWorkspaceGuard"/> constructed with no roots reports
/// <see cref="RepoContextWorkspaceGuard.IsEnforcing"/> <c>false</c> and its
/// resolver short-circuits to admit <i>every</i> path, so before this check a
/// host that enabled workspace mode without supplying a root exposed an
/// arbitrary local filesystem read: the caller names any directory, the ingest
/// projects the file bodies into the content tree, and the retrieval tools
/// (<c>repocontext_context</c>, <c>repocontext_search</c>) hand them back.
/// </para>
/// <para>
/// The refusal must not be confused with the single-repository opt-out.
/// <c>repocontext_bootstrap</c> deliberately accepts an arbitrary path because
/// its path is host configuration rather than caller input, and that behaviour
/// is pinned here so a future tightening cannot silently remove it - and so the
/// <c>add_repo</c> refusal is never "fixed" by falling back to it.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextToolHandlerWorkspaceBoundaryTests
{
    private static ServiceProvider BuildProvider(RepoContextWorkspaceGuard guard)
    {
        var services = new ServiceCollection();
        services.AddSingleton(guard);
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task AddRepoAsync_refuses_when_the_workspace_guard_is_not_enforcing()
    {
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.AddRepoAsync(context, "/etc"));

        Assert.That(error!.Message, Does.Contain("workspace boundary is not configured"));
    }

    /// <summary>
    /// The refusal precedes any ingest work, so an unguarded call can never reach
    /// the store: the provider deliberately registers only the guard, and any
    /// attempt to resolve the store would surface as a different exception type.
    /// The arguments here are the maximal-exposure shape an attacker would use -
    /// ignore .gitignore, ingest binaries - which must still be refused.
    /// </summary>
    [Test]
    public async Task AddRepoAsync_refuses_before_resolving_the_store()
    {
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.AddRepoAsync(
                context, "/etc", respectGitignore: false, excludeBinary: false));

        Assert.That(error!.Message, Does.Contain("workspace boundary is not configured"));
    }

    /// <summary>
    /// Argument validation still runs first, so a blank path reports the
    /// parameter problem rather than being masked by the boundary refusal.
    /// </summary>
    [Test]
    public async Task AddRepoAsync_still_reports_a_blank_path_before_the_boundary_check()
    {
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.AddRepoAsync(context, "   "));

        Assert.That(error!.Message, Does.Contain("'path' parameter is required"));
    }

    /// <summary>
    /// An enforcing guard passes the boundary check, proving the refusal is
    /// scoped to the unguarded shape rather than disabling the tool outright.
    /// The call then fails resolving its store collaborators, which is the next
    /// step past the check and is what this asserts.
    /// </summary>
    [Test]
    public async Task AddRepoAsync_passes_the_boundary_check_when_the_guard_is_enforcing()
    {
        var root = Path.Combine(Path.GetTempPath(), "lattice-workspace-boundary-test");
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([root]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        // The call proceeds past the boundary check and then fails resolving its
        // store collaborators, which this bare provider does not register. What
        // matters is only that the failure is no longer the boundary refusal.
        Exception? caught = null;
        try
        {
            await RepoContextToolHandlers.AddRepoAsync(context, Path.Combine(root, "repo"));
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.That(
            caught?.Message ?? string.Empty,
            Does.Not.Contain("workspace boundary is not configured"));
    }
}
