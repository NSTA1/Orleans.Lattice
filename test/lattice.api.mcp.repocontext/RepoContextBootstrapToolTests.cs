using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// End-to-end tests for the <c>repocontext_bootstrap</c> tool over the real MCP
/// protocol via <see cref="RepoContextMcpHarness"/>: fail-closed authorization
/// gating (offered to a writer, withheld from a reader, denied to an
/// unauthenticated caller) and the structural ingestion lifecycle (cold bootstrap,
/// re-run no-op, incremental change, deletion pruning, and resume after an
/// interrupted run without duplication).
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo and an
/// in-process MCP server and drives the full streamable-HTTP handshake, so it is
/// excluded from the fast unit dev loop. The walker, digest, and diff logic are
/// covered by fast unit fixtures under <c>Bootstrap/</c>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextBootstrapToolTests
{
    private const string RepoId = "sample-repo";
    private const string ToolName = "repocontext_bootstrap";

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

    private string NewRepo()
    {
        var root = Path.Combine(Path.GetTempPath(), "rcb-tool-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private static void Write(string root, string relativePath, string content)
    {
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content, Encoding.UTF8);
    }

    private static Dictionary<string, object?> Args(string root) =>
        new() { ["repoRoot"] = root, ["repoId"] = RepoId };

    private async Task<JsonElement> BootstrapAsync(McpClient client, string root)
    {
        var result = await client.CallToolAsync(ToolName, Args(root), cancellationToken: Ct);
        return result.RequireStructuredContent();
    }

    // -- Authorization gating --------------------------------------------------

    [Test]
    public async Task Writer_is_offered_the_bootstrap_tool()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Does.Contain(ToolName),
            "A writer (write opt-in) must be offered the mutating bootstrap tool.");
    }

    [Test]
    public async Task Reader_is_offered_health_but_not_the_bootstrap_tool()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Reader }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain("repocontext_health"),
                "A reader still sees the read-only surface.");
            Assert.That(names, Does.Not.Contain(ToolName),
                "A reader (no write opt-in) is never shown the mutating bootstrap tool.");
        });
    }

    [Test]
    public async Task Unauthenticated_caller_cannot_call_bootstrap()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Unauthenticated }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var names = await client.ListToolNamesAsync(Ct);
        Assert.That(names, Is.Empty, "A fail-closed session is offered no tools at all.");

        Assert.That(
            () => client.CallToolAsync(ToolName, Args(NewRepo()), cancellationToken: Ct).AsTask(),
            Throws.InstanceOf<McpException>(),
            "An unauthenticated caller is denied the tool at the protocol layer.");
    }

    // -- Structural ingestion lifecycle ---------------------------------------

    [Test]
    public async Task Cold_bootstrap_ingests_every_file_and_persists_records()
    {
        var root = NewRepo();
        Write(root, "src/Program.cs", "class Program {}");
        Write(root, "src/Util.cs", "class Util {}");
        Write(root, "README.md", "# sample");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var json = await BootstrapAsync(client, root);

        Assert.Multiple(() =>
        {
            Assert.That(json.GetProperty("repoId").GetString(), Is.EqualTo(RepoId));
            Assert.That(json.GetProperty("filesScanned").GetInt32(), Is.EqualTo(3));
            Assert.That(json.GetProperty("filesAdded").GetInt32(), Is.EqualTo(3));
            Assert.That(json.GetProperty("filesUpdated").GetInt32(), Is.EqualTo(0));
            Assert.That(json.GetProperty("filesRemoved").GetInt32(), Is.EqualTo(0));
            Assert.That(json.GetProperty("filesUnchanged").GetInt32(), Is.EqualTo(0));
            Assert.That(json.GetProperty("symbolsCaptured").GetInt32(), Is.EqualTo(0));
        });

        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        Assert.Multiple(() =>
        {
            Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "src/Program.cs"), Ct).Result, Is.Not.Null);
            Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "src/Util.cs"), Ct).Result, Is.Not.Null);
            Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "README.md"), Ct).Result, Is.Not.Null);
            Assert.That(tree.GetAsync(RepoContextKeys.Repo(RepoId), Ct).Result, Is.Not.Null);
        });
    }

    [Test]
    public async Task Rerun_over_an_unchanged_repo_is_a_no_op()
    {
        var root = NewRepo();
        Write(root, "a.cs", "one");
        Write(root, "b.cs", "two");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);
        var second = await BootstrapAsync(client, root);

        Assert.Multiple(() =>
        {
            Assert.That(second.GetProperty("filesScanned").GetInt32(), Is.EqualTo(2));
            Assert.That(second.GetProperty("filesAdded").GetInt32(), Is.EqualTo(0));
            Assert.That(second.GetProperty("filesUpdated").GetInt32(), Is.EqualTo(0));
            Assert.That(second.GetProperty("filesRemoved").GetInt32(), Is.EqualTo(0));
            Assert.That(second.GetProperty("filesUnchanged").GetInt32(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task An_incremental_change_updates_only_the_changed_file()
    {
        var root = NewRepo();
        Write(root, "a.cs", "one");
        Write(root, "b.cs", "two");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);
        Write(root, "b.cs", "two-changed");
        var second = await BootstrapAsync(client, root);

        Assert.Multiple(() =>
        {
            Assert.That(second.GetProperty("filesAdded").GetInt32(), Is.EqualTo(0));
            Assert.That(second.GetProperty("filesUpdated").GetInt32(), Is.EqualTo(1));
            Assert.That(second.GetProperty("filesRemoved").GetInt32(), Is.EqualTo(0));
            Assert.That(second.GetProperty("filesUnchanged").GetInt32(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_deleted_file_is_pruned_from_the_store()
    {
        var root = NewRepo();
        Write(root, "keep.cs", "keep");
        Write(root, "gone.cs", "gone");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        await BootstrapAsync(client, root);
        File.Delete(Path.Combine(root, "gone.cs"));
        var second = await BootstrapAsync(client, root);

        Assert.Multiple(() =>
        {
            Assert.That(second.GetProperty("filesRemoved").GetInt32(), Is.EqualTo(1));
            Assert.That(second.GetProperty("filesUnchanged").GetInt32(), Is.EqualTo(1));
            Assert.That(second.GetProperty("filesAdded").GetInt32(), Is.EqualTo(0));
        });

        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        Assert.Multiple(() =>
        {
            Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "gone.cs"), Ct).Result, Is.Null,
                "The pruned file's structural record is removed.");
            Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "keep.cs"), Ct).Result, Is.Not.Null);
        });
    }

    [Test]
    public async Task An_interrupted_run_resumes_without_duplication()
    {
        var root = NewRepo();
        Write(root, "a.cs", "one");
        Write(root, "b.cs", "two");

        // A vectorisation seam that throws on its first invocation simulates a crash
        // after the structural writes have committed but before the run completes.
        var ingestor = new ThrowOnceVectorIngestor();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureServices = services =>
                    services.AddSingleton<IRepoContextVectorIngestor>(ingestor),
            },
            Ct);
        await using var client = await harness.ConnectAsync(Ct);

        // First run: structural writes commit, then the seam throws, so the tool
        // reports an error - the "interrupted" run.
        var interrupted = await client.CallToolAsync(ToolName, Args(root), cancellationToken: Ct);
        Assert.That(interrupted.IsError, Is.True, "The seam fault surfaces as a tool error.");

        // The structural records committed before the fault, proving the writes are
        // durable at the point of interruption.
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "a.cs"), Ct).Result, Is.Not.Null);

        // Second run resumes: the already-written files match by digest, so nothing
        // is re-added or duplicated - the resume is a clean no-op over committed work.
        var resumed = await BootstrapAsync(client, root);
        Assert.Multiple(() =>
        {
            Assert.That(resumed.GetProperty("filesAdded").GetInt32(), Is.EqualTo(0));
            Assert.That(resumed.GetProperty("filesUpdated").GetInt32(), Is.EqualTo(0));
            Assert.That(resumed.GetProperty("filesUnchanged").GetInt32(), Is.EqualTo(2));
            Assert.That(ingestor.Invocations, Is.EqualTo(1),
                "The interrupted run reached the seam once; the resumed run is a pure "
                + "no-op over already-committed work and never re-enters it.");
        });
    }

    /// <summary>
    /// A test vectorisation seam that throws on its first call (to simulate a crash
    /// mid-run) and is inert thereafter, counting invocations.
    /// </summary>
    private sealed class ThrowOnceVectorIngestor : IRepoContextVectorIngestor
    {
        private int _invocations;

        public int Invocations => _invocations;

        public ValueTask IngestAsync(
            string repoId,
            string repoRoot,
            IReadOnlyList<RepoFileEntry> changedFiles,
            CancellationToken cancellationToken)
        {
            var count = Interlocked.Increment(ref _invocations);
            if (count == 1)
            {
                throw new InvalidOperationException("Simulated interruption during vectorisation.");
            }

            return ValueTask.CompletedTask;
        }
    }
}
