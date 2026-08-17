using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Content;

/// <summary>
/// Unit tests for <see cref="RepoContextContentReconciler"/>'s stale-content-tree
/// tolerance. The content tree is a rebuildable derived projection, not a store of
/// record, so a terminally-stale content leaf (its durable checkpoint fell off the
/// write-ahead log with no covering snapshot, surfaced as
/// <see cref="LeafProjectionStaleException"/>) must degrade the content pass to a
/// no-op that leaves every file unmarked for retry - never fail the whole repository
/// index. These tests substitute a faulting content tree so the terminal fault the
/// silo harness cannot easily provoke is exercised directly, and confirm the guard
/// is scoped to that one exception type (an unrelated fault still propagates).
/// </summary>
[TestFixture]
public sealed class RepoContextContentReconcilerStaleTests
{
    private const string RepoId = "acme";

    private static readonly Serializer<ContentRecord> ContentSerializer =
        new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer<ContentRecord>>();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private string _repoRoot = string.Empty;

    [SetUp]
    public void SetUp()
    {
        _repoRoot = Path.Combine(Path.GetTempPath(), $"rccs-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_repoRoot);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (Directory.Exists(_repoRoot))
            {
                Directory.Delete(_repoRoot, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup; a locked temp file must not fail the run.
        }
    }

    private RepoFileEntry WriteFile(string relativePath, string content)
    {
        var full = Path.Combine(_repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        var digest = FileDigest.Compute(Encoding.UTF8.GetBytes(content));
        return new RepoFileEntry(relativePath, digest, content.Length, "csharp");
    }

    private static (RepoContextContentReconciler Reconciler, CapturingLoggerProvider Log) BuildFaulting(
        Exception fault)
    {
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(fault);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Content).Returns(tree);

        var log = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(b => b.AddProvider(log));
        var logger = loggerFactory.CreateLogger<RepoContextContentReconciler>();

        return (new RepoContextContentReconciler(grainFactory, ContentSerializer, logger), log);
    }

    [Test]
    public async Task Stale_content_tree_degrades_to_an_empty_result_without_throwing()
    {
        var (reconciler, log) = BuildFaulting(new LeafProjectionStaleException("simulated stale content leaf"));
        var file = WriteFile("src/Widget.cs", "namespace N; public class Widget { }");

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.ContentCaptured, Is.EqualTo(0),
                "A terminally-stale content tree captures no records.");
            Assert.That(result.ProcessedPaths, Is.Empty,
                "No file is marked content-processed, so the back-fill retries it once the tree is healed.");
            Assert.That(
                log.Entries.Any(e => e.Level == LogLevel.Warning
                    && e.Exception is LeafProjectionStaleException
                    && e.Message.Contains("content projection is stale", StringComparison.Ordinal)),
                Is.True,
                "The degrade path logs a warning that carries the stale-projection exception.");
        });
    }

    [Test]
    public async Task Stale_content_tree_leaves_backfill_files_unmarked_for_retry()
    {
        var (reconciler, _) = BuildFaulting(new LeafProjectionStaleException("simulated stale content leaf"));
        var file = WriteFile("src/Old.cs", "namespace N; public class Old { }");

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: [], backfill: [file], Ct);

        Assert.That(result.ProcessedPaths, Does.Not.Contain("src/Old.cs"),
            "A stale content tree must not mark a back-fill file processed, or it would never be re-projected.");
    }

    [Test]
    public void A_non_stale_write_fault_still_propagates()
    {
        // The guard is scoped to LeafProjectionStaleException; any other fault is a
        // genuine failure of the index pass and must not be swallowed.
        var (reconciler, _) = BuildFaulting(new InvalidOperationException("unrelated write fault"));
        var file = WriteFile("src/Boom.cs", "namespace N; public class Boom { }");

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await reconciler.ReconcileAsync(
                RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [], Ct));
    }
}
