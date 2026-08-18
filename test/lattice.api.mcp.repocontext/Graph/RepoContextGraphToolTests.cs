using System.IO;
using System.Text;
using System.Text.Json;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Graph;

/// <summary>
/// End-to-end tests for the three read-only structural-graph tools
/// (<c>repocontext_outline</c>, <c>repocontext_changed</c>, and
/// <c>repocontext_related</c>) over the real MCP protocol via
/// <see cref="RepoContextMcpHarness"/>. Each test onboards a repository under a
/// mounted workspace, waits for the index to settle, then drives the tool and asserts
/// its projected JSON: the outline skeleton and full-read token cost, the git-free
/// drift report (added and removed by digest), and the related neighbourhood
/// (outbound imports, inbound dependents, and test linkage).
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an in-process
/// MCP server. The unit behaviour of the underlying derivations, key builders, result
/// records, and reverse-index maintenance is covered by fast fixtures elsewhere.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextGraphToolTests
{
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
        var root = Path.Combine(Path.GetTempPath(), "rcg-" + Guid.NewGuid().ToString("N"));
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
            Write(repoRoot, path, content);
        }

        return repoRoot;
    }

    private static void Write(string repoRoot, string relativePath, string content)
    {
        var full = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content, Encoding.UTF8);
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

    private async Task<string> OnboardAsync(McpClient client, string repoRoot, string repoId)
    {
        await client.CallToolAsync(
            "repocontext_add_repo", new Dictionary<string, object?> { ["path"] = repoRoot }, cancellationToken: Ct);
        await client.WaitForIndexAsync(repoId, Ct);
        return repoId;
    }

    private static IReadOnlyList<string> Strings(JsonElement array)
        => array.EnumerateArray().Select(e => e.GetString()!).ToList();

    [Test]
    public async Task Outline_reports_the_symbol_skeleton_and_full_read_token_cost()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "app",
            ("src/Widget.cs", "namespace N;\npublic class Widget\n{\n    public void Run() { }\n}\n"));

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);
        await OnboardAsync(client, repoRoot, "app");

        var json = (await client.CallToolAsync(
            "repocontext_outline",
            new Dictionary<string, object?> { ["repoId"] = "app", ["path"] = "src/Widget.cs" },
            cancellationToken: Ct)).RequireStructuredContent();

        var kinds = json.GetProperty("symbols").EnumerateArray()
            .ToDictionary(s => s.GetProperty("fullyQualifiedName").GetString()!, s => s.GetProperty("kind").GetString());

        Assert.Multiple(() =>
        {
            Assert.That(json.GetProperty("exists").GetBoolean(), Is.True);
            Assert.That(json.GetProperty("path").GetString(), Is.EqualTo("src/Widget.cs"));
            Assert.That(kinds.ContainsKey("N.Widget"), Is.True, "the declared type appears in the skeleton");
            Assert.That(kinds["N.Widget"], Is.EqualTo("Type"));
            Assert.That(kinds.ContainsKey("N.Widget.Run()"), Is.True, "the declared method appears in the skeleton");
            Assert.That(json.GetProperty("fullReadTokenCount").GetInt32(), Is.GreaterThan(0),
                "the file's full-read token cost is reported so an agent can weigh a full read");
        });
    }

    [Test]
    public async Task Outline_reports_a_missing_file_as_absent()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "app", ("src/Widget.cs", "namespace N; public class Widget { }"));

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);
        await OnboardAsync(client, repoRoot, "app");

        var json = (await client.CallToolAsync(
            "repocontext_outline",
            new Dictionary<string, object?> { ["repoId"] = "app", ["path"] = "src/Absent.cs" },
            cancellationToken: Ct)).RequireStructuredContent();

        Assert.Multiple(() =>
        {
            Assert.That(json.GetProperty("exists").GetBoolean(), Is.False);
            Assert.That(json.GetProperty("symbols").GetArrayLength(), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Changed_detects_files_added_and_removed_since_the_index_without_git()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "app",
            ("src/A.cs", "namespace N; public class A { }"),
            ("src/B.cs", "namespace N; public class B { }"));

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);
        await OnboardAsync(client, repoRoot, "app");

        // Mutate the workspace after indexing: add one file, delete another, leave B.cs.
        Write(repoRoot, "src/C.cs", "namespace N; public class C { }");
        File.Delete(Path.Combine(repoRoot, "src", "A.cs"));

        var json = (await client.CallToolAsync(
            "repocontext_changed",
            new Dictionary<string, object?> { ["repoId"] = "app", ["path"] = repoRoot },
            cancellationToken: Ct)).RequireStructuredContent();

        Assert.Multiple(() =>
        {
            Assert.That(Strings(json.GetProperty("added")), Does.Contain("src/C.cs"));
            Assert.That(Strings(json.GetProperty("removed")), Does.Contain("src/A.cs"));
            Assert.That(Strings(json.GetProperty("added")), Does.Not.Contain("src/B.cs"),
                "an unchanged file is neither added nor removed");
        });
    }

    [Test]
    public async Task Related_reports_outbound_imports_inbound_dependents_and_tests()
    {
        var workspace = NewWorkspace();
        var repoRoot = WriteRepo(workspace, "app",
            ("src/A.cs", "namespace N; public class A { public B Dep { get; set; } }"),
            ("src/B.cs", "namespace N; public class B { }"),
            ("test/BTests.cs", "namespace N; public class BTests { }"));

        await using var harness = await StartAsync(workspace);
        await using var client = await harness.ConnectAsync(Ct);
        await OnboardAsync(client, repoRoot, "app");

        var forA = (await client.CallToolAsync(
            "repocontext_related",
            new Dictionary<string, object?> { ["repoId"] = "app", ["path"] = "src/A.cs" },
            cancellationToken: Ct)).RequireStructuredContent();

        var forB = (await client.CallToolAsync(
            "repocontext_related",
            new Dictionary<string, object?> { ["repoId"] = "app", ["path"] = "src/B.cs" },
            cancellationToken: Ct)).RequireStructuredContent();

        var dependentsOfB = forB.GetProperty("dependents").EnumerateArray()
            .Select(e => e.GetProperty("symbol").GetString()).ToList();
        var testsOfB = forB.GetProperty("tests").EnumerateArray()
            .Select(e => e.GetProperty("symbol").GetString()).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(Strings(forA.GetProperty("imports")), Does.Contain("B"),
                "A's outbound imports include the type it references");
            Assert.That(dependentsOfB, Does.Contain("N.A"),
                "B's inbound dependents include the referencing type");
            Assert.That(testsOfB, Does.Contain("N.BTests"),
                "B is linked to its conventionally-named test type");
        });
    }
}
