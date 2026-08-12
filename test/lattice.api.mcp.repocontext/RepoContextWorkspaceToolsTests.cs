using System.IO;
using System.Text;
using System.Text.Json;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// End-to-end tests for the workspace-mode repository tools
/// (<c>repocontext_add_repo</c>, <c>repocontext_list_repos</c>,
/// <c>repocontext_remove_repo</c>) over the real MCP protocol via
/// <see cref="RepoContextMcpHarness"/>: fail-closed authorization gating, the
/// register/list/remove lifecycle, repo-id derivation, and the fail-closed
/// workspace boundary that refuses a path escaping the mounted root.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an
/// in-process MCP server and drives the full streamable-HTTP handshake. The guard
/// and walker logic are covered by fast unit fixtures under <c>Bootstrap/</c>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextWorkspaceToolsTests
{
    private const string AddRepo = "repocontext_add_repo";
    private const string ListRepos = "repocontext_list_repos";
    private const string RemoveRepo = "repocontext_remove_repo";

    private readonly List<string> _tempRoots = new();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [TearDown]
    public void TearDown()
    {
        foreach (var root in _tempRoots)
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }

        _tempRoots.Clear();
    }

    private string NewWorkspace()
    {
        var root = Path.Combine(Path.GetTempPath(), "rcb-ws-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private static string WriteRepo(string workspace, string repoName, params (string Path, string Content)[] files)
    {
        var repoRoot = Path.Combine(workspace, repoName);
        Directory.CreateDirectory(repoRoot);
        foreach (var (path, content) in files)
        {
            var full = Path.Combine(repoRoot, path.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);
            File.WriteAllText(full, content, Encoding.UTF8);
        }

        return repoRoot;
    }

    private Task<RepoContextMcpHarness> StartAsync(string workspace, RepoContextMcpAuthPosture posture)
        => RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = posture,
                WorkspaceMode = true,
                WorkspaceRoot = workspace,
            },
            Ct);

    // -- Authorization gating --------------------------------------------------

    [Test]
    public async Task Writer_is_offered_the_workspace_write_tools_and_not_bootstrap()
    {
        var workspace = NewWorkspace();
        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain(AddRepo));
            Assert.That(names, Does.Contain(RemoveRepo));
            Assert.That(names, Does.Contain(ListRepos));
            Assert.That(names, Does.Not.Contain("repocontext_bootstrap"),
                "Workspace mode supersedes the single-repository bootstrap tool.");
        });
    }

    [Test]
    public async Task Reader_is_offered_list_repos_but_not_the_write_tools()
    {
        var workspace = NewWorkspace();
        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Reader);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain(ListRepos),
                "list_repos is read-only, so a reader is offered it.");
            Assert.That(names, Does.Not.Contain(AddRepo));
            Assert.That(names, Does.Not.Contain(RemoveRepo));
        });
    }

    [Test]
    public async Task Unauthenticated_caller_is_offered_no_tools()
    {
        var workspace = NewWorkspace();
        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Unauthenticated);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Is.Empty, "A fail-closed session is offered no tools at all.");
    }

    // -- Register / list / remove lifecycle ------------------------------------

    [Test]
    public async Task Add_repo_ingests_and_derives_the_repo_id_from_the_path()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "my-repo",
            ("src/Program.cs", "class Program {}"),
            ("README.md", "# my-repo"));

        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = repoRoot }, cancellationToken: Ct);
        var json = result.RequireStructuredContent();

        Assert.Multiple(() =>
        {
            Assert.That(json.GetProperty("repoId").GetString(), Is.EqualTo("my-repo"),
                "The repo id is derived from the final path segment when omitted.");
            Assert.That(json.GetProperty("filesScanned").GetInt32(), Is.EqualTo(2));
            Assert.That(json.GetProperty("filesAdded").GetInt32(), Is.EqualTo(2));
        });

        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        Assert.That(tree.GetAsync(RepoContextKeys.Repo("my-repo"), Ct).Result, Is.Not.Null);
    }

    [Test]
    public async Task List_repos_reports_every_registered_repo_with_its_file_count()
    {
        var workspace = NewWorkspace();
        var alpha = WriteRepo(workspace, "alpha", ("a.cs", "one"));
        var beta = WriteRepo(workspace, "beta", ("b.cs", "two"), ("c.cs", "three"));

        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        await client.CallToolAsync(AddRepo, new Dictionary<string, object?> { ["path"] = alpha }, cancellationToken: Ct);
        await client.CallToolAsync(AddRepo, new Dictionary<string, object?> { ["path"] = beta }, cancellationToken: Ct);

        var list = (await client.CallToolAsync(ListRepos, new Dictionary<string, object?>(), cancellationToken: Ct))
            .RequireStructuredContent();

        Assert.That(list.GetProperty("count").GetInt32(), Is.EqualTo(2));

        var repos = list.GetProperty("repos").EnumerateArray()
            .ToDictionary(r => r.GetProperty("repoId").GetString()!, r => r);

        Assert.Multiple(() =>
        {
            Assert.That(repos.Keys, Is.EquivalentTo(new[] { "alpha", "beta" }));
            Assert.That(repos["alpha"].GetProperty("fileCount").GetInt64(), Is.EqualTo(1));
            Assert.That(repos["beta"].GetProperty("fileCount").GetInt64(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Remove_repo_drops_every_record_and_the_repo_from_the_list()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "gone", ("a.cs", "one"), ("b.cs", "two"));

        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        await client.CallToolAsync(AddRepo, new Dictionary<string, object?> { ["path"] = repoRoot }, cancellationToken: Ct);

        var removal = (await client.CallToolAsync(
                RemoveRepo, new Dictionary<string, object?> { ["repoId"] = "gone" }, cancellationToken: Ct))
            .RequireStructuredContent();

        Assert.Multiple(() =>
        {
            Assert.That(removal.GetProperty("repoId").GetString(), Is.EqualTo("gone"));
            // At least the two files plus the repository root marker.
            Assert.That(removal.GetProperty("entriesDeleted").GetInt32(), Is.GreaterThanOrEqualTo(3));
        });

        var list = (await client.CallToolAsync(ListRepos, new Dictionary<string, object?>(), cancellationToken: Ct))
            .RequireStructuredContent();
        Assert.That(list.GetProperty("count").GetInt32(), Is.EqualTo(0));

        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        Assert.That(tree.GetAsync(RepoContextKeys.Repo("gone"), Ct).Result, Is.Null);
    }

    [Test]
    public async Task Remove_of_an_unknown_repo_is_a_no_op()
    {
        var workspace = NewWorkspace();
        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        var removal = (await client.CallToolAsync(
                RemoveRepo, new Dictionary<string, object?> { ["repoId"] = "never-added" }, cancellationToken: Ct))
            .RequireStructuredContent();

        Assert.That(removal.GetProperty("entriesDeleted").GetInt32(), Is.EqualTo(0));
    }

    // -- Workspace boundary ----------------------------------------------------

    [Test]
    public async Task Add_repo_refuses_a_path_outside_the_workspace()
    {
        var workspace = NewWorkspace();
        // A sibling directory outside the workspace root.
        var outside = NewWorkspace();
        WriteRepo(outside, "secret", ("a.cs", "one"));
        var escapePath = Path.Combine(outside, "secret");

        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = escapePath }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True,
            "A path resolving outside the mounted workspace must be refused.");
    }

    [Test]
    public async Task Add_repo_refuses_a_dotdot_escape_from_inside_the_workspace()
    {
        var workspace = NewWorkspace();
        var outside = NewWorkspace();
        WriteRepo(outside, "secret", ("a.cs", "one"));

        // A path that lexically sits under the workspace but climbs out with '..'.
        var escapePath = Path.Combine(workspace, "..",
            Path.GetFileName(outside.TrimEnd(Path.DirectorySeparatorChar)), "secret");

        await using var harness = await StartAsync(workspace, RepoContextMcpAuthPosture.Writer);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = escapePath }, cancellationToken: Ct);

        Assert.That(result.IsError, Is.True,
            "A '..' escape out of the workspace must be refused.");
    }
}
