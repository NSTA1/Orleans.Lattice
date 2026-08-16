using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Content;

/// <summary>
/// Integration tests for <see cref="RepoContextContentReconciler"/>. Each test
/// co-hosts a real Orleans silo (memory grain storage and the dedicated content
/// tree) via <see cref="RepoContextMcpHarness"/> and drives the reconciler against
/// on-disk files under a temp repository root, asserting the per-file
/// <see cref="ContentRecord"/> it writes, its processed set, and its deletion of a
/// removed file's content record.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextContentReconcilerTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private string _repoRoot = string.Empty;

    [SetUp]
    public void SetUp()
    {
        _repoRoot = Path.Combine(Path.GetTempPath(), $"rcc-{Guid.NewGuid():N}");
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

    private static RepoContextContentReconciler Reconciler(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextContentReconciler>();

    private async Task<ContentRecord?> ReadContentAsync(RepoContextMcpHarness harness, string path)
    {
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Content);
        var bytes = await tree.GetAsync(RepoContextKeys.Content(RepoId, path), Ct);
        return bytes is null
            ? null
            : harness.Services.GetRequiredService<Serializer<ContentRecord>>().Deserialize(bytes);
    }

    [Test]
    public async Task Added_file_captures_its_body_text()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var file = WriteFile("src/Gadget.cs", "namespace N; public class Gadget { public void Run() { } }");

        var result = await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [], Ct);

        var record = await ReadContentAsync(harness, "src/Gadget.cs");
        Assert.Multiple(() =>
        {
            Assert.That(result.ContentCaptured, Is.EqualTo(1));
            Assert.That(result.ProcessedPaths, Does.Contain("src/Gadget.cs"));
            Assert.That(record, Is.Not.Null);
            Assert.That(RepoContextValues.ReadString(record!.Text), Does.Contain("class Gadget"),
                "The stored content projection carries the file body so keyword search matches content.");
        });
    }

    [Test]
    public async Task Backfill_file_captures_its_body_text()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var file = WriteFile("src/Old.cs", "namespace N; public class Old { }");

        var result = await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: [], backfill: [file], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.ProcessedPaths, Does.Contain("src/Old.cs"),
                "A pre-existing file with no content record yet is healed by the back-fill.");
            Assert.That(ReadContentAsync(harness, "src/Old.cs").Result, Is.Not.Null);
        });
    }

    [Test]
    public async Task Removed_file_deletes_its_content_record()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var file = WriteFile("src/Gone.cs", "namespace N; public class Gone { }");
        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [], Ct);

        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: ["src/Gone.cs"], backfill: [], Ct);

        Assert.That(await ReadContentAsync(harness, "src/Gone.cs"), Is.Null,
            "A pruned file's content record is retired so the projection stays honest.");
    }

    [Test]
    public async Task Updated_file_reprojects_the_new_body()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var first = WriteFile("src/Edit.cs", "namespace N; public class Edit { public void First() { } }");
        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [first], updated: [], removedPaths: [], backfill: [], Ct);

        var second = WriteFile("src/Edit.cs", "namespace N; public class Edit { public void Second() { } }");
        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [second], removedPaths: [], backfill: [], Ct);

        var record = await ReadContentAsync(harness, "src/Edit.cs");
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(record!.Text), Does.Contain("Second"));
            Assert.That(RepoContextValues.ReadString(record.Text), Does.Not.Contain("First"),
                "Re-projection overwrites the prior body with the current one.");
        });
    }
}
