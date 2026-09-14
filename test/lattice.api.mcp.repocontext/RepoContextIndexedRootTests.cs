using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// End-to-end tests that <c>repocontext_list_repos</c> reports the filesystem root each
/// repository was actually indexed from (issue #2617).
/// <para>
/// The defect was not a wrong answer. Every record served under the id was internally
/// consistent and the index was genuinely healthy - it was simply about a different tree,
/// and no surface a caller reads said so. The decisive fixture here is therefore not
/// "the field is populated" but
/// <see cref="The_indexed_root_is_the_only_field_that_separates_a_correct_index_from_the_issue_2617_state"/>,
/// which registers both shapes in one workspace with every other reported field held
/// equal by construction and asserts that the root is the sole discriminator. An
/// assertion that a field is present cannot fail in the way this defect failed.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an in-process MCP
/// server and drives the full streamable-HTTP handshake. The mismatch predicate behind the
/// startup warning is covered by the fast unit fixture
/// <c>Bootstrap/RepoContextIndexedRootMismatchTests</c>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextIndexedRootTests
{
    private const string AddRepo = "repocontext_add_repo";
    private const string ListRepos = "repocontext_list_repos";
    private const string ResetIndex = "repocontext_reset_index";

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
        var root = Path.Combine(Path.GetTempPath(), "rcb-root-" + Guid.NewGuid().ToString("N"));
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

    private Task<RepoContextMcpHarness> StartAsync(string workspace)
        => RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                WorkspaceMode = true,
                WorkspaceRoot = workspace,
            },
            Ct);

    private static async Task<Dictionary<string, JsonElement>> ListAsync(McpClient client, CancellationToken ct)
    {
        var list = (await client.CallToolAsync(ListRepos, new Dictionary<string, object?>(), cancellationToken: ct))
            .RequireStructuredContent();

        return list.GetProperty("repos").EnumerateArray()
            .ToDictionary(r => r.GetProperty("repoId").GetString()!, r => r, StringComparer.Ordinal);
    }

    private static string? RootOf(JsonElement row)
        => row.TryGetProperty("indexedRoot", out var root) && root.ValueKind != JsonValueKind.Null
            ? root.GetString()
            : null;

    [Test]
    public async Task List_repos_reports_the_root_a_repository_was_indexed_from()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "alpha", ("a.cs", "one"));

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);

        await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = repoRoot }, cancellationToken: Ct);
        await client.WaitForIndexAsync("alpha", Ct);

        // Compare against the root as the workspace guard resolves it, which is what the
        // index actually walked. Comparing against the raw temp path would be a different
        // assertion on any platform whose temp directory is reached through a symlink.
        var expected = harness.Services.GetRequiredService<RepoContextWorkspaceGuard>().Resolve(repoRoot);

        var repos = await ListAsync(client, Ct);

        Assert.That(RootOf(repos["alpha"]), Is.EqualTo(expected),
            "The listing must name the tree the records describe, not restate the id.");
    }

    /// <summary>
    /// The issue #2617 reproduction and the positive control for the whole change.
    /// <para>
    /// Two repositories are registered in one workspace. The first is an ordinary correct
    /// registration. The second reproduces the incident exactly: a directory whose name is
    /// NOT the repository id, registered under the base repository's id, which is what a
    /// stack composed from a git worktree with the default workspace root produces. Both
    /// are given the same number of files so that <c>fileCount</c> is equal BY
    /// CONSTRUCTION rather than by luck, and both are indexed so both carry a
    /// <c>lastIngested</c> marker.
    /// </para>
    /// <para>
    /// The assertion is therefore not merely that the root is reported, but that with every
    /// other reported field held equal the root is the ONLY field that separates a correct
    /// index from an index of the wrong tree. That is the precise claim the issue makes and
    /// the precise claim that was false before this change.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_indexed_root_is_the_only_field_that_separates_a_correct_index_from_the_issue_2617_state()
    {
        var workspace = NewWorkspace();

        // Identical content on both sides: same file count, same bytes. Anything that
        // differs between the two rows below is therefore a difference this change
        // introduced, not an artefact of the fixture.
        var files = new[] { ("a.cs", "one"), ("b.cs", "two") };
        var correct = WriteRepo(workspace, "healthy-repo", files);
        var worktree = WriteRepo(workspace, "generated-worktree-name", files);

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);

        await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = correct }, cancellationToken: Ct);

        // The incident: the id says one thing, the directory is another.
        await client.CallToolAsync(
            AddRepo,
            new Dictionary<string, object?> { ["path"] = worktree, ["repoId"] = "base-repo" },
            cancellationToken: Ct);

        await client.WaitForIndexAsync("healthy-repo", Ct);
        await client.WaitForIndexAsync("base-repo", Ct);

        var guard = harness.Services.GetRequiredService<RepoContextWorkspaceGuard>();
        var repos = await ListAsync(client, Ct);

        var healthy = repos["healthy-repo"];
        var incident = repos["base-repo"];

        Assert.Multiple(() =>
        {
            // 1. Every other reported signal is indistinguishable between the two.
            Assert.That(
                incident.GetProperty("fileCount").GetInt64(),
                Is.EqualTo(healthy.GetProperty("fileCount").GetInt64()),
                "Both indexed the same number of files, so the count cannot separate them.");
            Assert.That(
                incident.GetProperty("lastIngested").ValueKind, Is.Not.EqualTo(JsonValueKind.Null),
                "The wrongly-rooted index is genuinely healthy: it carries a current ingest marker.");
            Assert.That(
                healthy.GetProperty("lastIngested").ValueKind, Is.Not.EqualTo(JsonValueKind.Null));

            // 2. The root is present, correct, and different on each row.
            Assert.That(RootOf(healthy), Is.EqualTo(guard.Resolve(correct)));
            Assert.That(RootOf(incident), Is.EqualTo(guard.Resolve(worktree)));
            Assert.That(RootOf(incident), Is.Not.EqualTo(RootOf(healthy)),
                "The root is the discriminator; if these were equal the field would be useless.");

            // 3. And the detector that turns that difference into a warning fires on the
            //    incident row and stays silent on the healthy one. Asserting only the
            //    difference would leave the operator to notice it unaided, which is what
            //    did not happen for the life of a gate run.
            Assert.That(
                RepoContextIndexedRootReporter.IsIdRootMismatch("base-repo", RootOf(incident)),
                Is.True,
                "The mismatch must be detectable, not merely present in the payload.");
            Assert.That(
                RepoContextIndexedRootReporter.IsIdRootMismatch("healthy-repo", RootOf(healthy)),
                Is.False,
                "The control: a correct registration must not warn.");
        });
    }

    /// <summary>
    /// A reset clears the durable index request the root is read from, so the root joins
    /// the three nulls a reset already reports. That is the honest answer - the repository
    /// has no indexed root until it is re-added - and reporting the pre-reset root would
    /// reintroduce, on this field, exactly the stale-but-confident reading the reset path
    /// was changed to avoid.
    /// </summary>
    [Test]
    public async Task A_reset_repository_reports_no_indexed_root_until_it_is_re_added()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "reset-me", ("a.cs", "one"));

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);

        await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = repoRoot }, cancellationToken: Ct);
        await client.WaitForIndexAsync("reset-me", Ct);

        // Control: the root is there before the reset, so its absence afterwards is the
        // reset's doing and not a field that never populated.
        var before = await ListAsync(client, Ct);
        Assert.That(RootOf(before["reset-me"]), Is.Not.Null);

        await client.CallToolAsync(
            ResetIndex, new Dictionary<string, object?> { ["repoId"] = "reset-me" }, cancellationToken: Ct);

        var after = await ListAsync(client, Ct);
        Assert.That(after.ContainsKey("reset-me"), Is.True,
            "A reset repository stays listed - that is how its preserved memory stays reachable.");
        Assert.That(RootOf(after["reset-me"]), Is.Null,
            "No index, no indexed root.");

        // And re-adding repopulates it.
        await client.CallToolAsync(
            AddRepo, new Dictionary<string, object?> { ["path"] = repoRoot }, cancellationToken: Ct);
        await client.WaitForIndexAsync("reset-me", Ct);

        var reindexed = await ListAsync(client, Ct);
        Assert.That(
            RootOf(reindexed["reset-me"]),
            Is.EqualTo(harness.Services.GetRequiredService<RepoContextWorkspaceGuard>().Resolve(repoRoot)));
    }
}
