using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Regression tests for the workspace-boundary fail-closed check shared by both
/// path-taking onboarding tools, <see cref="RepoContextToolHandlers.AddRepoAsync"/>
/// and <see cref="RepoContextToolHandlers.BootstrapAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// Both tools take their path straight from the wire and the published contract
/// of each is that the path is "resolved against the workspace boundary; a path
/// outside it is rejected". A <see cref="RepoContextWorkspaceGuard"/> constructed
/// with no roots reports <see cref="RepoContextWorkspaceGuard.IsEnforcing"/>
/// <c>false</c> and its resolver short-circuits to admit <i>every</i> path, so
/// without this check a host that enabled writes without supplying a root exposed
/// an arbitrary local filesystem read: the caller names any directory, the ingest
/// projects the file bodies into the content tree, and the retrieval tools
/// (<c>repocontext_context</c>, <c>repocontext_search</c>) hand them back.
/// </para>
/// <para>
/// <b>Bootstrap is not an exemption.</b> An earlier revision of this fixture
/// asserted that <c>repocontext_bootstrap</c> "deliberately accepts an arbitrary
/// path because its path is host configuration rather than caller input". That
/// justification was simply false: bootstrap's <c>repoRoot</c> is a tool argument
/// like any other, supplied by whoever calls the tool, and the mutating
/// repository-context tools require only a data-plane write grant rather than an
/// administrative one. The exemption is therefore removed and its absence pinned
/// here, so the <c>add_repo</c> refusal can never be "fixed" by falling back to
/// bootstrap.
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

    // ---- bootstrap: the same boundary, previously exempted ----------------

    /// <summary>
    /// The core regression. <c>repocontext_bootstrap</c> once reached
    /// <c>StartIndexAsync</c> with no boundary check at all, so under the
    /// documented single-repository default -
    /// <c>AddRepoContextTools(enableWrites: true)</c> with no
    /// <c>workspaceRoot</c> - any caller holding a write grant could name any
    /// absolute path on the host and have its contents indexed and then read back
    /// through the retrieval tools.
    /// </summary>
    [Test]
    public async Task BootstrapAsync_refuses_when_the_workspace_guard_is_not_enforcing()
    {
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.BootstrapAsync(context, "/etc", "acme"));

        Assert.That(error!.Message, Does.Contain("workspace boundary is not configured"));
    }

    /// <summary>
    /// The refusal precedes any ingest work, exactly as it does for
    /// <c>add_repo</c>: the provider registers only the guard, so reaching the
    /// store would surface as a different exception type. The arguments are the
    /// maximal-exposure shape an attacker would use - ignore .gitignore, ingest
    /// binaries - which must still be refused.
    /// </summary>
    [Test]
    public async Task BootstrapAsync_refuses_before_resolving_the_store()
    {
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.BootstrapAsync(
                context, "/etc", "acme", respectGitignore: false, excludeBinary: false));

        Assert.That(error!.Message, Does.Contain("workspace boundary is not configured"));
    }

    /// <summary>
    /// The refusal is scoped to the unguarded shape: with an enforcing guard the
    /// call proceeds past the boundary check, so bootstrap keeps working for the
    /// hosts that configured a root (the shipped container among them).
    /// </summary>
    [Test]
    public async Task BootstrapAsync_passes_the_boundary_check_when_the_guard_is_enforcing()
    {
        var root = Path.Combine(Path.GetTempPath(), "lattice-bootstrap-boundary-test");
        Directory.CreateDirectory(root);
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([root]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        Exception? caught = null;
        try
        {
            await RepoContextToolHandlers.BootstrapAsync(context, root, "acme");
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.That(
            caught?.Message ?? string.Empty,
            Does.Not.Contain("workspace boundary is not configured"));
    }

    /// <summary>
    /// An enforcing guard is not enough on its own: a path outside the configured
    /// root must still be refused, which is the boundary doing its ordinary job
    /// once bootstrap is subject to it.
    /// </summary>
    [Test]
    public async Task BootstrapAsync_refuses_a_root_outside_the_configured_workspace()
    {
        var root = Path.Combine(Path.GetTempPath(), "lattice-bootstrap-boundary-inside");
        var outside = Path.Combine(Path.GetTempPath(), "lattice-bootstrap-boundary-outside");
        Directory.CreateDirectory(root);
        Directory.CreateDirectory(outside);
        using var provider = BuildProvider(new RepoContextWorkspaceGuard([root]));
        var context = await RepoContextRequestContexts.CreateAsync(provider);

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.BootstrapAsync(context, outside, "acme"));

        Assert.That(error!.Message, Does.Contain("outside the mounted workspace"));
    }

    /// <summary>
    /// Advertisement must match enforcement. The handler refusal above is the
    /// boundary; withholding the tool is what stops a caller discovering an
    /// onboarding path that can only fail, and what stops the group substituting
    /// bootstrap for the equally-withheld <c>add_repo</c>.
    /// </summary>
    [Test]
    public void RepoContextToolGroup_withholds_bootstrap_when_the_workspace_is_not_guarded()
    {
        var unguarded = new RepoContextToolGroup(
            enableWrites: true, workspaceMode: false, workspaceGuarded: false);
        var guarded = new RepoContextToolGroup(
            enableWrites: true, workspaceMode: false, workspaceGuarded: true);

        Assert.Multiple(() =>
        {
            Assert.That(
                unguarded.Tools.Select(t => t.ProtocolTool.Name),
                Does.Not.Contain("repocontext_bootstrap"));
            Assert.That(
                guarded.Tools.Select(t => t.ProtocolTool.Name),
                Does.Contain("repocontext_bootstrap"));
        });
    }
}
