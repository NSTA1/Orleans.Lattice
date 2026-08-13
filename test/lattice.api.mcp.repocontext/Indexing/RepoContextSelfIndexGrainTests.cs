using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Integration tests for <see cref="IRepoContextSelfIndexGrain"/> and the failed-run
/// re-drive it relies on, against a live in-memory Lattice cluster via
/// <see cref="RepoContextMcpHarness"/>. They prove <c>EnsureRunningAsync</c> is the
/// single onboarding entry point that arms the grain and drives the idempotent
/// index to completion, that <c>StopAsync</c> is an idempotent teardown safe to call
/// on a grain that never ran, and that a job whose last run outright failed is
/// re-driven by <see cref="IRepoIndexJobGrain.EnsureIndexedAsync"/> - the mechanism
/// the self-index scan uses to rescue a failed onboarding without a client call.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (memory grain
/// storage and reminders) and reads a temp repo off disk, so it is excluded from
/// the fast unit dev loop. The paged gap scan and membership presence check are
/// covered by the faster gap-scanner and ingestor fixtures under <c>Retrieval/</c>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextSelfIndexGrainTests
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

    private string NewRepo(params (string Path, string Content)[] files)
    {
        var root = Path.Combine(Path.GetTempPath(), "rc-selfindex-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        foreach (var (path, content) in files)
        {
            var full = Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);
            File.WriteAllText(full, content);
        }

        return root;
    }

    private async Task<RepoIndexProgress> WaitForTerminalAsync(RepoContextMcpHarness harness, string repoId)
    {
        var job = harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(repoId);
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            var progress = await job.GetProgressAsync();
            if (progress.Status is RepoIndexStatus.Completed or RepoIndexStatus.Failed)
            {
                return progress;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException($"The indexing job for '{repoId}' did not settle in time.");
            }

            await Task.Delay(25, Ct);
        }
    }

    [Test]
    public async Task EnsureRunningAsync_rejects_a_null_request()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>("acme");

        Assert.That(() => grain.EnsureRunningAsync(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task StopAsync_on_a_never_started_grain_is_a_harmless_no_op()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>("never-started");

        Assert.DoesNotThrowAsync(async () => await grain.StopAsync());
    }

    [Test]
    public async Task EnsureRunningAsync_drives_the_initial_index_to_completion()
    {
        const string RepoId = "acme";
        var root = NewRepo(("src/Program.cs", "class Program {}"), ("README.md", "# sample"));

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>(RepoId);

        var accepted = await grain.EnsureRunningAsync(new RepoIndexJobRequest { RepoRoot = root, RepoId = RepoId });
        Assert.Multiple(() =>
        {
            Assert.That(accepted.RepoId, Is.EqualTo(RepoId));
            Assert.That(accepted.Status, Is.Not.EqualTo(RepoIndexStatus.None),
                "EnsureRunningAsync drives a run, so the accepted snapshot is not the never-started state.");
        });

        var terminal = await WaitForTerminalAsync(harness, RepoId);
        Assert.Multiple(() =>
        {
            Assert.That(terminal.Status, Is.EqualTo(RepoIndexStatus.Completed),
                "The onboarding run the grain drives settles Completed.");
            Assert.That(terminal.FilesAdded, Is.EqualTo(2), "Both files are ingested by the driven run.");
        });
    }

    [Test]
    public async Task StopAsync_after_a_run_is_idempotent()
    {
        const string RepoId = "acme";
        var root = NewRepo(("a.cs", "class A {}"));

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>(RepoId);

        await grain.EnsureRunningAsync(new RepoIndexJobRequest { RepoRoot = root, RepoId = RepoId });
        await WaitForTerminalAsync(harness, RepoId);

        Assert.DoesNotThrowAsync(async () =>
        {
            await grain.StopAsync();
            await grain.StopAsync();
        }, "Stopping an armed grain, and stopping it again, both tear down cleanly.");
    }

    [Test]
    public async Task EnsureIndexedAsync_re_drives_a_job_whose_last_run_failed()
    {
        const string RepoId = "acme";
        var root = NewRepo(("a.cs", "class A {}"), ("b.cs", "class B {}"));

        // A vectorisation seam that throws on its first call sends the first run to
        // Failed; the second (self-index re-drive) run then completes. This is the
        // "failed before any structural detection" hole the status re-drive closes.
        var ingestor = new FailOnceVectorIngestor();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureServices = services =>
                    services.AddSingleton<IRepoContextVectorIngestor>(ingestor),
            },
            Ct);

        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>(RepoId);
        await grain.EnsureRunningAsync(new RepoIndexJobRequest { RepoRoot = root, RepoId = RepoId });

        var failed = await WaitForTerminalAsync(harness, RepoId);
        Assert.That(failed.Status, Is.EqualTo(RepoIndexStatus.Failed), "The seam fault settles the first run Failed.");

        // Re-drive as the self-index scan would, from the persisted request, with no
        // client call.
        var job = harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);
        var triggered = await job.EnsureIndexedAsync();
        Assert.That(triggered, Is.True, "A failed job is re-driven from its persisted request.");

        var recovered = await WaitForTerminalAsync(harness, RepoId);
        Assert.That(recovered.Status, Is.EqualTo(RepoIndexStatus.Completed),
            "The re-driven run completes, rescuing the failed onboarding.");
    }

    [Test]
    public async Task A_reconcile_picks_up_a_file_edited_on_disk_after_onboarding()
    {
        const string RepoId = "acme";
        var root = NewRepo(("a.cs", "class A {}"), ("b.cs", "class B {}"));

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                // Force every reconcile to a full walk so an in-place content edit -
                // which does not bump a directory's modification time and is invisible
                // to directory-modification-time pruning - is detected deterministically.
                // This is the default production regime, where the full-walk interval is
                // at or below the reconcile interval, so every reconcile is a full sweep.
                ConfigureServices = services =>
                    services.AddSingleton(new RepoContextIndexingOptions { FullWalkInterval = TimeSpan.Zero }),
            },
            Ct);
        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>(RepoId);

        await grain.EnsureRunningAsync(new RepoIndexJobRequest { RepoRoot = root, RepoId = RepoId });
        var initial = await WaitForTerminalAsync(harness, RepoId);
        Assert.That(initial.FilesAdded, Is.EqualTo(2), "Both files are ingested by onboarding.");

        // Edit one file on disk. The content (and digest) changes, so the reconcile's
        // stat fast-path misses and the file is re-read and detected as an update.
        File.WriteAllText(Path.Combine(root, "a.cs"), "class A { int X; }");

        // Drive the reconcile exactly as the periodic self-index scan would: from the
        // persisted request, with no client call.
        var job = harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);
        var triggered = await job.EnsureIndexedAsync();
        Assert.That(triggered, Is.True, "A completed job is re-driven to reconcile on-disk changes.");

        var reconciled = await WaitForTerminalAsync(harness, RepoId);
        Assert.Multiple(() =>
        {
            Assert.That(reconciled.Status, Is.EqualTo(RepoIndexStatus.Completed));
            Assert.That(reconciled.FilesUpdated, Is.EqualTo(1), "The edited file is detected as an update.");
            Assert.That(reconciled.FilesRemoved, Is.EqualTo(0), "Nothing is pruned by an edit.");
        });
    }

    [Test]
    public async Task A_reconcile_prunes_a_file_deleted_on_disk_after_onboarding()
    {
        const string RepoId = "acme";
        var root = NewRepo(("keep.cs", "class Keep {}"), ("gone.cs", "class Gone {}"));

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                // Force every reconcile to a full walk so the reconcile-diff assertion is
                // independent of the test filesystem's directory-modification-time behaviour.
                ConfigureServices = services =>
                    services.AddSingleton(new RepoContextIndexingOptions { FullWalkInterval = TimeSpan.Zero }),
            },
            Ct);
        var grain = harness.GrainFactory.GetGrain<IRepoContextSelfIndexGrain>(RepoId);

        await grain.EnsureRunningAsync(new RepoIndexJobRequest { RepoRoot = root, RepoId = RepoId });
        await WaitForTerminalAsync(harness, RepoId);

        // Delete a file on disk. The reconcile's walk no longer produces it, so the
        // diff lists its stored path for pruning.
        File.Delete(Path.Combine(root, "gone.cs"));

        var job = harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);
        var triggered = await job.EnsureIndexedAsync();
        Assert.That(triggered, Is.True, "A completed job is re-driven to reconcile a deletion.");

        var reconciled = await WaitForTerminalAsync(harness, RepoId);
        Assert.Multiple(() =>
        {
            Assert.That(reconciled.Status, Is.EqualTo(RepoIndexStatus.Completed));
            Assert.That(reconciled.FilesRemoved, Is.EqualTo(1), "The deleted file is pruned.");
            Assert.That(reconciled.FilesScanned, Is.EqualTo(1), "Only the surviving file is scanned.");
        });
    }

    /// <summary>
    /// A test vectorisation seam that throws on its first ingest (to fail the first
    /// run) and is inert thereafter, so a re-drive succeeds.
    /// </summary>
    private sealed class FailOnceVectorIngestor : IRepoContextVectorIngestor
    {
        private int _invocations;

        public ValueTask<int> IngestAsync(
            string repoId,
            string repoRoot,
            IReadOnlyList<RepoFileEntry> changedFiles,
            IReadOnlyList<RepoFileEntry> unchangedFiles,
            Func<int, CancellationToken, ValueTask>? onProgress,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _invocations) == 1)
            {
                throw new InvalidOperationException("Simulated vectorisation failure on the first run.");
            }

            return ValueTask.FromResult(0);
        }

        public Task RetireAsync(
            string repoId, IReadOnlyList<string> removedPaths, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<int> IngestSymbolsAsync(
            string repoId,
            IReadOnlyCollection<string> changedSymbolKeys,
            IReadOnlyCollection<string> prunedSymbolKeys,
            CancellationToken cancellationToken) => Task.FromResult(0);
    }
}
