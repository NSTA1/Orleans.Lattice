using System.Text.Json;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Source;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// The end-to-end proof that the opt-in git-ref source strategy actually wires up:
/// a repository declared only in configuration (no <c>add_repo</c> call, no mounted
/// path) is fetched from a real - but local and offline - git remote, indexed from
/// the commit the configured ref resolved to, and its repository record is stamped
/// with that commit so <c>list_repos</c> can answer "which revision am I serving".
/// <para>
/// This is the seam every unit fixture under <c>Source/</c> stops short of: it drives
/// the environment-variable configuration through the real DI registration, the
/// arming background service, the self-index grain, the runner, and the bootstrap
/// service's anchor stamping.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo and an in-process MCP
/// server. Marked <c>NonParallelizable</c> because it configures the feature through
/// process-wide environment variables, which are cleared in teardown. It never
/// touches the network: the remote is a local repository created in a temp directory.
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class RepoContextGitSourceEndToEndTests
{
    private const string RepoId = "gitrepo";

    private readonly List<string> _tempRoots = new();
    private LocalGitRemoteFixture? _remote;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [TearDown]
    public void TearDown()
    {
        foreach (var name in new[]
                 {
                     RepoContextGitSourceRegistry.ReposVariable,
                     RepoContextGitSourceRegistry.StagingRootVariable,
                     RepoContextGitSourceRegistry.VariableName(RepoId, "URL"),
                     RepoContextGitSourceRegistry.VariableName(RepoId, "REF"),
                     RepoContextGitSourceRegistry.VariableName(RepoId, "AUTH"),
                     RepoContextGitSourceRegistry.VariableName(RepoId, "DEPTH"),
                 })
        {
            Environment.SetEnvironmentVariable(name, null);
        }

        _remote?.Dispose();
        _remote = null;

        foreach (var root in _tempRoots)
        {
            // The staging tree is a real git repository, and git marks its pack files
            // read-only: a plain recursive delete refuses them on Windows.
            LocalGitRemoteFixture.ForceDelete(root);
        }

        _tempRoots.Clear();
    }

    private string NewDirectory(string prefix)
    {
        var root = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N")[..12]);
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    [Test]
    public async Task A_configured_git_source_indexes_its_ref_and_stamps_the_resolved_commit()
    {
        _remote = LocalGitRemoteFixture.Create(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["README.md"] = "# sample\n",
                ["src/One.cs"] = "public sealed class One { }\n",
            });

        var workspace = NewDirectory("rcb-git-e2e-ws-");
        var staging = NewDirectory("rcb-git-e2e-staging-");

        Environment.SetEnvironmentVariable(RepoContextGitSourceRegistry.ReposVariable, RepoId);
        Environment.SetEnvironmentVariable(RepoContextGitSourceRegistry.StagingRootVariable, staging);
        Environment.SetEnvironmentVariable(
            RepoContextGitSourceRegistry.VariableName(RepoId, "URL"), _remote.OriginPath);
        Environment.SetEnvironmentVariable(
            RepoContextGitSourceRegistry.VariableName(RepoId, "REF"), _remote.BranchRef);
        Environment.SetEnvironmentVariable(RepoContextGitSourceRegistry.VariableName(RepoId, "AUTH"), "anonymous");
        Environment.SetEnvironmentVariable(RepoContextGitSourceRegistry.VariableName(RepoId, "DEPTH"), "0");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                WorkspaceMode = true,
                WorkspaceRoot = workspace,
            },
            Ct);
        await using var client = await harness.ConnectAsync(Ct);

        // The arming service onboards the declared source a few seconds after start,
        // so the repository appears without any add_repo call ever being made.
        var progress = await client.WaitForIndexAsync(RepoId, Ct, TimeSpan.FromMinutes(2));
        Assert.That(progress.GetProperty("status").GetString(), Is.EqualTo("Completed"),
            "The declared git source indexes without any mounted path being registered.");

        var repo = await FindRepoAsync(client);
        Assert.Multiple(() =>
        {
            Assert.That(repo.GetProperty("indexedCommit").GetString(), Is.EqualTo(_remote.HeadSha()),
                "The generation is stamped with the commit the configured ref resolved to.");
            Assert.That(repo.GetProperty("fileCount").GetInt32(), Is.EqualTo(2),
                "Exactly the commit's tracked files are indexed.");
        });
    }

    private async Task<JsonElement> FindRepoAsync(McpClient client)
    {
        var result = await client.CallToolAsync(
            "repocontext_list_repos", new Dictionary<string, object?>(), cancellationToken: Ct);
        var repos = result.RequireStructuredContent().GetProperty("repos");

        foreach (var repo in repos.EnumerateArray())
        {
            if (repo.GetProperty("repoId").GetString() == RepoId)
            {
                return repo;
            }
        }

        Assert.Fail($"The repository '{RepoId}' was not listed.");
        return default;
    }
}
