using System.Text;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// End-to-end tests for the mount-versus-git mutual exclusion at the one seam that
/// registers a mounted path, <c>repocontext_add_repo</c>. A repository whose truth is
/// declared to live in a git remote must never also be indexed from whatever a caller
/// happened to mount: the two sources would race to define the same path space and the
/// commit anchor would stop naming what is actually served.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an in-process
/// MCP server and drives the full streamable-HTTP handshake. The routing decision
/// itself is covered by the fast unit fixtures under <c>Source/</c>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextGitSourceMountExclusionTests
{
    private const string AddRepo = "repocontext_add_repo";
    private const string GitSourcedRepoId = "git-sourced";

    private readonly List<string> _tempRoots = new();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [TearDown]
    public void TearDown()
    {
        foreach (var root in _tempRoots.Where(Directory.Exists))
        {
            Directory.Delete(root, recursive: true);
        }

        _tempRoots.Clear();
    }

    private string NewWorkspace()
    {
        var root = Path.Combine(Path.GetTempPath(), "rcb-git-x-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private string WriteRepo(string workspace, string repoName)
    {
        var repoRoot = Path.Combine(workspace, repoName);
        Directory.CreateDirectory(repoRoot);
        File.WriteAllText(Path.Combine(repoRoot, "a.cs"), "class A { }", Encoding.UTF8);
        return repoRoot;
    }

    private Task<RepoContextMcpHarness> StartAsync(string workspace) =>
        RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                WorkspaceMode = true,
                WorkspaceRoot = workspace,

                // Registered before AddRepoContextTools, so this declared registry
                // wins over the environment-derived one the package would build.
                ConfigureServices = services => services.AddSingleton(
                    new RepoContextGitSourceRegistry(
                        [
                            new RepoContextGitSourceOptions
                            {
                                RepoId = GitSourcedRepoId,
                                RemoteUrl = "https://git.example/git-sourced.git",
                            }
                        ],
                        Path.Combine(Path.GetTempPath(), "lattice-repocontext-git-exclusion"))),
            },
            Ct);

    [Test]
    public async Task Add_repo_refuses_a_repository_that_is_already_git_sourced()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, GitSourcedRepoId);

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync(
            AddRepo,
            new Dictionary<string, object?> { ["path"] = repoRoot, ["repoId"] = GitSourcedRepoId },
            cancellationToken: Ct);

        Assert.That(result.IsError, Is.True,
            "A git-sourced repository must not also be registered from a mounted path.");
    }

    [Test]
    public async Task Add_repo_still_accepts_a_repository_that_is_not_git_sourced()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "mounted");

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync(
            AddRepo,
            new Dictionary<string, object?> { ["path"] = repoRoot, ["repoId"] = "mounted" },
            cancellationToken: Ct);

        Assert.That(result.IsError, Is.Not.True,
            "Declaring one git source must not regress the mounted default for every other repository.");
    }
}
