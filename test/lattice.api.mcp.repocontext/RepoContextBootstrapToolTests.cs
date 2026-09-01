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
        var start = await client.CallToolAsync(ToolName, Args(root), cancellationToken: Ct);
        // Onboarding starts asynchronously: the call returns the running job's
        // acceptance snapshot, so await the job's terminal state to observe counts.
        start.RequireStructuredContent();
        return await client.WaitForIndexAsync(RepoId, Ct);
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
            Assert.That(json.GetProperty("status").GetString(), Is.EqualTo("Completed"));
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

        // First run: the job starts asynchronously and is accepted. The background
        // run commits the structural writes, then the seam throws, so the job
        // settles in Failed - the "interrupted" run.
        var accepted = await client.CallToolAsync(ToolName, Args(root), cancellationToken: Ct);
        Assert.That(accepted.IsError, Is.Not.True, "Starting the job is accepted.");
        var failed = await client.WaitForIndexAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(failed.GetProperty("status").GetString(), Is.EqualTo("Failed"),
                "The seam fault settles the job in Failed.");
            Assert.That(failed.GetProperty("error").GetString(), Is.Not.Null.And.Not.Empty,
                "The failure reason is recorded on the snapshot.");
        });

        // The structural records committed before the fault, proving the writes are
        // durable at the point of interruption.
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        Assert.That(tree.GetAsync(RepoContextKeys.File(RepoId, "a.cs"), Ct).Result, Is.Not.Null);

        // Second run resumes: the already-written files match by digest, so nothing
        // is re-added or duplicated - the structural resume is a clean no-op over
        // committed work. Vectorising still runs (it always does), so the ingestor
        // is re-entered exactly once to back-fill the embeddings the interrupted run
        // never wrote; this time the seam is inert, so the resume settles cleanly.
        var resumed = await BootstrapAsync(client, root);
        Assert.Multiple(() =>
        {
            Assert.That(resumed.GetProperty("filesAdded").GetInt32(), Is.EqualTo(0));
            Assert.That(resumed.GetProperty("filesUpdated").GetInt32(), Is.EqualTo(0));
            Assert.That(resumed.GetProperty("filesUnchanged").GetInt32(), Is.EqualTo(2));
            Assert.That(ingestor.Invocations, Is.EqualTo(2),
                "The interrupted run reached the seam once and failed; the resumed run "
                + "re-enters it once more to back-fill the embeddings that never landed.");
        });
    }

    /// <summary>
    /// A failing embedding arm no longer vetoes its siblings. The symbol arm and the
    /// memory arm are independent passes over the same store, so a symbol-arm fault
    /// (a timeout on a large repository, say) must not cost the memory arm its pass -
    /// while still leaving the run reported as failed, so it is retried rather than
    /// silently reported as a clean success over a partial view.
    /// </summary>
    [Test]
    public async Task A_failing_symbol_arm_does_not_stop_the_memory_arm()
    {
        var root = NewRepo();
        Write(root, "a.cs", "class A {}");

        var ingestor = new SymbolArmFailsVectorIngestor();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureServices = services =>
                    services.AddSingleton<IRepoContextVectorIngestor>(ingestor),
            },
            Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var accepted = await client.CallToolAsync(ToolName, Args(root), cancellationToken: Ct);
        Assert.That(accepted.IsError, Is.Not.True, "Starting the job is accepted.");
        var settled = await client.WaitForIndexAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            // The point of the fix: the arm downstream of the fault still ran.
            Assert.That(ingestor.MemoryArmRuns, Is.GreaterThan(0),
                "The memory arm runs even though the symbol arm threw before it.");

            // Paired positive control, so the assertion above cannot pass vacuously
            // on a run that simply never reached the vectorising phase at all.
            Assert.That(ingestor.SymbolArmRuns, Is.GreaterThan(0),
                "The symbol arm was reached, so the memory arm genuinely ran after a real fault.");
            Assert.That(ingestor.FileArmRuns, Is.GreaterThan(0),
                "The file arm ran, so the run reached vectorising rather than failing earlier.");

            // And the failure is still surfaced: continuing past a faulted arm must not
            // launder a partial pass into a reported success.
            Assert.That(settled.GetProperty("status").GetString(), Is.EqualTo("Failed"),
                "The arm fault still settles the job in Failed, so the run is retried.");
            Assert.That(settled.GetProperty("error").GetString(), Does.Contain("symbol arm"),
                "The recorded failure is the symbol arm's, not a later substitute.");
        });
    }

    /// <summary>
    /// The status tool reports <c>None</c> for a repository that was never
    /// onboarded, so a caller can distinguish "no job" from a running or settled
    /// one without the tool erroring.
    /// </summary>
    [Test]
    public async Task Index_status_reports_none_for_an_unknown_repo()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await using var client = await harness.ConnectAsync(Ct);

        var result = await client.CallToolAsync(
            "repocontext_index_status",
            new Dictionary<string, object?> { ["repoId"] = "never-onboarded" },
            cancellationToken: Ct);
        var status = result.RequireStructuredContent();

        Assert.Multiple(() =>
        {
            Assert.That(status.GetProperty("status").GetString(), Is.EqualTo("None"));
            Assert.That(status.GetProperty("repoId").GetString(), Is.EqualTo("never-onboarded"));
            Assert.That(status.GetProperty("attempt").GetInt32(), Is.EqualTo(0));
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

        public ValueTask<int> IngestAsync(
            string repoId,
            string repoRoot,
            IReadOnlyList<RepoFileEntry> changedFiles,
            IReadOnlyList<RepoFileEntry> unchangedFiles,
            Func<int, CancellationToken, ValueTask>? onProgress,
            CancellationToken cancellationToken)
        {
            var count = Interlocked.Increment(ref _invocations);
            if (count == 1)
            {
                throw new InvalidOperationException("Simulated interruption during vectorisation.");
            }

            return ValueTask.FromResult(0);
        }

        public Task RetireAsync(
            string repoId,
            IReadOnlyList<string> removedPaths,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<int> IngestSymbolsAsync(
            string repoId,
            IReadOnlyCollection<string> changedSymbolKeys,
            IReadOnlyCollection<string> prunedSymbolKeys,
            CancellationToken cancellationToken) => Task.FromResult(0);

        public Task<int> IngestMemoryAsync(
            string repoId,
            IReadOnlyCollection<string> changedMemoryKeys,
            IReadOnlyCollection<string> retiredMemoryKeys,
            CancellationToken cancellationToken) => Task.FromResult(0);
    }

    /// <summary>
    /// A test vectorisation seam whose symbol arm always faults, while the file and
    /// memory arms record that they ran. It proves the arms are independent: a fault
    /// in one must not cost the others their pass.
    /// </summary>
    private sealed class SymbolArmFailsVectorIngestor : IRepoContextVectorIngestor
    {
        private int _fileArmRuns;
        private int _symbolArmRuns;
        private int _memoryArmRuns;

        public int FileArmRuns => Volatile.Read(ref _fileArmRuns);

        public int SymbolArmRuns => Volatile.Read(ref _symbolArmRuns);

        public int MemoryArmRuns => Volatile.Read(ref _memoryArmRuns);

        public ValueTask<int> IngestAsync(
            string repoId,
            string repoRoot,
            IReadOnlyList<RepoFileEntry> changedFiles,
            IReadOnlyList<RepoFileEntry> unchangedFiles,
            Func<int, CancellationToken, ValueTask>? onProgress,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _fileArmRuns);
            return ValueTask.FromResult(0);
        }

        public Task RetireAsync(
            string repoId,
            IReadOnlyList<string> removedPaths,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<int> IngestSymbolsAsync(
            string repoId,
            IReadOnlyCollection<string> changedSymbolKeys,
            IReadOnlyCollection<string> prunedSymbolKeys,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _symbolArmRuns);
            throw new InvalidOperationException("Simulated symbol arm fault.");
        }

        public Task<int> IngestMemoryAsync(
            string repoId,
            IReadOnlyCollection<string> changedMemoryKeys,
            IReadOnlyCollection<string> retiredMemoryKeys,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _memoryArmRuns);
            return Task.FromResult(0);
        }
    }
}
