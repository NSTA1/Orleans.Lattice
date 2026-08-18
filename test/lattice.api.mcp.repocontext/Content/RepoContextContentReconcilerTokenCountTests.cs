using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Content;

/// <summary>
/// Unit tests for the token-count output of <see cref="RepoContextContentReconciler"/>.
/// A non-faulting content tree is substituted so the reconcile succeeds and its
/// <see cref="ContentReconcileResult.TokenCountsByPath"/> can be asserted against the
/// same <see cref="IRepoContextTokenCounter"/> over the decoded body. This proves the
/// reconciler computes the per-file count exactly once, from the body it already read,
/// for added, updated, and back-filled files, and omits files it never projected.
/// </summary>
[TestFixture]
public sealed class RepoContextContentReconcilerTokenCountTests
{
    private const string RepoId = "acme";

    private static readonly Serializer<ContentRecord> ContentSerializer =
        new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer<ContentRecord>>();

    private static readonly IRepoContextTokenCounter Counter =
        new TiktokenRepoContextTokenCounter(new RepoContextIndexingOptions());

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private string _repoRoot = string.Empty;

    [SetUp]
    public void SetUp()
    {
        _repoRoot = Path.Combine(Path.GetTempPath(), $"rctc-{Guid.NewGuid():N}");
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

    private static RepoContextContentReconciler BuildReconciler()
    {
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Content).Returns(tree);

        return new RepoContextContentReconciler(
            grainFactory, ContentSerializer, Counter, NullLogger<RepoContextContentReconciler>.Instance);
    }

    [Test]
    public async Task Reconciling_an_added_file_records_its_token_count_consistent_with_the_counter()
    {
        const string body = "namespace N; public class Widget { public int Answer => 42; }";
        var reconciler = BuildReconciler();
        var file = WriteFile("src/Widget.cs", body);

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.TokenCountsByPath.ContainsKey("src/Widget.cs"), Is.True);
            Assert.That(result.TokenCountsByPath["src/Widget.cs"], Is.EqualTo(Counter.CountTokens(body)));
            Assert.That(result.TokenCountsByPath["src/Widget.cs"], Is.GreaterThan(0));
            Assert.That(result.ProcessedPaths, Does.Contain("src/Widget.cs"));
        });
    }

    [Test]
    public async Task Reconciling_an_updated_file_records_its_token_count()
    {
        const string body = "The quick brown fox jumps over the lazy dog.";
        var reconciler = BuildReconciler();
        var file = WriteFile("docs/note.txt", body);

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [file], removedPaths: [], backfill: [], Ct);

        Assert.That(result.TokenCountsByPath["docs/note.txt"], Is.EqualTo(Counter.CountTokens(body)));
    }

    [Test]
    public async Task Reconciling_a_backfill_file_records_its_token_count()
    {
        // A back-fill file is one whose node predates the register; it is projected
        // now exactly like an added file, so its token count is computed on this pass.
        const string body = "public static class Program { }";
        var reconciler = BuildReconciler();
        var file = WriteFile("src/Program.cs", body);

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: [], backfill: [file], Ct);

        Assert.That(result.TokenCountsByPath["src/Program.cs"], Is.EqualTo(Counter.CountTokens(body)));
    }

    [Test]
    public async Task Reconciling_an_empty_file_records_a_zero_token_count()
    {
        var reconciler = BuildReconciler();
        var file = WriteFile("src/Empty.cs", string.Empty);

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.ProcessedPaths, Does.Contain("src/Empty.cs"),
                "An empty file is still projected (and marked processed) so it is not re-selected forever.");
            Assert.That(result.TokenCountsByPath["src/Empty.cs"], Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Reconciling_no_files_produces_no_token_counts()
    {
        var reconciler = BuildReconciler();

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: [], backfill: [], Ct);

        Assert.That(result.TokenCountsByPath, Is.Empty);
    }

    [Test]
    public async Task Token_counts_only_cover_the_files_projected_this_pass()
    {
        var reconciler = BuildReconciler();
        var a = WriteFile("src/A.cs", "public class A { }");
        var b = WriteFile("src/B.cs", "public class B { }");

        var result = await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [a], updated: [b], removedPaths: ["src/Gone.cs"], backfill: [], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.TokenCountsByPath.Keys,
                Is.EquivalentTo(new[] { "src/A.cs", "src/B.cs" }));
            Assert.That(result.TokenCountsByPath.ContainsKey("src/Gone.cs"), Is.False,
                "A removed path is deleted, never token-counted.");
        });
    }
}
