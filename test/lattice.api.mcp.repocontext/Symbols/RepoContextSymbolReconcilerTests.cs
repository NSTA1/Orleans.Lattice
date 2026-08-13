using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Integration tests for <see cref="RepoContextSymbolReconciler"/>. Each test
/// co-hosts a real Orleans silo (memory grain storage and the dedicated symbol
/// tree) via <see cref="RepoContextMcpHarness"/> and drives the reconciler against
/// on-disk source files under a temp repository root, asserting the durable symbol
/// records it writes, its multi-file (partial-type) ownership set, and its pruning
/// once no file declares a symbol any more.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextSymbolReconcilerTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private string _repoRoot = string.Empty;

    [SetUp]
    public void SetUp()
    {
        _repoRoot = Path.Combine(Path.GetTempPath(), $"rcs-{Guid.NewGuid():N}");
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

    private static RepoContextSymbolReconciler Reconciler(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextSymbolReconciler>();

    private async Task<SymbolRecord?> ReadSymbolAsync(RepoContextMcpHarness harness, string fqName)
    {
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var bytes = await tree.GetAsync(RepoContextKeys.Symbol(RepoId, fqName), Ct);
        if (bytes is null)
        {
            return null;
        }

        return harness.Services.GetRequiredService<Serializer<SymbolRecord>>().Deserialize(bytes);
    }

    private static IReadOnlySet<string> DeclaringFiles(SymbolRecord record)
        => record.DeclaringFiles.Elements()
            .Select(e => Encoding.UTF8.GetString(e))
            .ToHashSet(StringComparer.Ordinal);

    [Test]
    public async Task Added_file_captures_its_symbols()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var file = WriteFile("src/Gadget.cs", "namespace N; public class Gadget { public void Run() { } }");

        var result = await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [],
            storedMeta: new Dictionary<string, StoredFileMeta>(), Ct);

        var type = await ReadSymbolAsync(harness, "N.Gadget");
        var method = await ReadSymbolAsync(harness, "N.Gadget.Run()");

        Assert.Multiple(() =>
        {
            Assert.That(result.SymbolsCaptured, Is.GreaterThanOrEqualTo(3), "namespace, type, and method are upserted");
            Assert.That(type, Is.Not.Null);
            Assert.That(DeclaringFiles(type!), Does.Contain("src/Gadget.cs"));
            Assert.That(method, Is.Not.Null);
            Assert.That(result.DeclaredByPath["src/Gadget.cs"], Does.Contain("N.Gadget.Run()"));
            Assert.That(result.ChangedSymbolKeys, Does.Contain(RepoContextKeys.Symbol(RepoId, "N.Gadget.Run()")),
                "an upserted symbol's canonical key is surfaced so its embedding is refreshed");
            Assert.That(result.PrunedSymbolKeys, Is.Empty, "nothing is pruned when a file is added");
        });
    }

    [Test]
    public async Task Partial_type_across_two_files_is_owned_by_both()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var a = WriteFile("A.cs", "namespace N; public partial class C { public void FromA() { } }");
        var b = WriteFile("B.cs", "namespace N; public partial class C { public void FromB() { } }");

        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [a, b], updated: [], removedPaths: [], backfill: [],
            storedMeta: new Dictionary<string, StoredFileMeta>(), Ct);

        var type = await ReadSymbolAsync(harness, "N.C");

        Assert.That(type, Is.Not.Null);
        Assert.That(DeclaringFiles(type!), Is.EquivalentTo(new[] { "A.cs", "B.cs" }),
            "a partial type is declared by every file that contributes to it");
    }

    [Test]
    public async Task Symbol_survives_while_one_sibling_still_declares_it_and_prunes_when_the_last_is_removed()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var a = WriteFile("A.cs", "namespace N; public partial class C { }");
        var b = WriteFile("B.cs", "namespace N; public partial class C { }");
        var stored = new Dictionary<string, StoredFileMeta>(StringComparer.Ordinal);
        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [a, b], updated: [], removedPaths: [], backfill: [], storedMeta: stored, Ct);

        // Both files declared N.C on the first pass; model that in stored metadata.
        stored["A.cs"] = new StoredFileMeta(a.Digest, "csharp", a.SizeBytes, 0, ["N.C"]);
        stored["B.cs"] = new StoredFileMeta(b.Digest, "csharp", b.SizeBytes, 0, ["N.C"]);

        // Remove A.cs: N.C must survive because B.cs still declares it.
        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: ["A.cs"], backfill: [], storedMeta: stored, Ct);

        var afterFirstRemoval = await ReadSymbolAsync(harness, "N.C");

        // Remove B.cs: the last declarer is gone, so N.C must be pruned.
        var lastRemoval = await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: ["B.cs"], backfill: [], storedMeta: stored, Ct);

        var afterLastRemoval = await ReadSymbolAsync(harness, "N.C");

        Assert.Multiple(() =>
        {
            Assert.That(afterFirstRemoval, Is.Not.Null, "the symbol survives while a sibling still declares it");
            Assert.That(DeclaringFiles(afterFirstRemoval!), Is.EquivalentTo(new[] { "B.cs" }));
            Assert.That(afterLastRemoval, Is.Null, "the record is pruned once no file declares it");
            Assert.That(lastRemoval.PrunedSymbolKeys, Does.Contain(RepoContextKeys.Symbol(RepoId, "N.C")),
                "a pruned symbol's canonical key is surfaced so its embedding is retired");
        });
    }

    [Test]
    public async Task Updated_file_that_drops_a_symbol_prunes_it()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var v1 = WriteFile("C.cs", "namespace N; public class C { public void Keep() { } public void Drop() { } }");
        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [v1], updated: [], removedPaths: [], backfill: [],
            storedMeta: new Dictionary<string, StoredFileMeta>(), Ct);

        Assert.That(await ReadSymbolAsync(harness, "N.C.Drop()"), Is.Not.Null);

        // Rewrite the file without Drop(); its prior declared set includes it.
        var v2 = WriteFile("C.cs", "namespace N; public class C { public void Keep() { } }");
        var stored = new Dictionary<string, StoredFileMeta>(StringComparer.Ordinal)
        {
            ["C.cs"] = new StoredFileMeta(v1.Digest, "csharp", v1.SizeBytes, 0,
                ["N", "N.C", "N.C.Keep()", "N.C.Drop()"]),
        };

        await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [v2], removedPaths: [], backfill: [], storedMeta: stored, Ct);

        var dropped = await ReadSymbolAsync(harness, "N.C.Drop()");
        var kept = await ReadSymbolAsync(harness, "N.C.Keep()");

        Assert.Multiple(() =>
        {
            Assert.That(dropped, Is.Null, "the dropped symbol is pruned");
            Assert.That(kept, Is.Not.Null, "the retained symbol survives");
        });
    }

    [Test]
    public async Task Reconcile_is_idempotent_on_a_repeated_pass()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var file = WriteFile("C.cs", "namespace N; public class C { }");
        var reconciler = Reconciler(harness);

        await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [file], updated: [], removedPaths: [], backfill: [],
            storedMeta: new Dictionary<string, StoredFileMeta>(), Ct);
        var first = await ReadSymbolAsync(harness, "N.C");

        // Re-running the same file (as an update) must not grow the declaring set.
        var stored = new Dictionary<string, StoredFileMeta>(StringComparer.Ordinal)
        {
            ["C.cs"] = new StoredFileMeta(file.Digest, "csharp", file.SizeBytes, 0, ["N", "N.C"]),
        };
        await reconciler.ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [file], removedPaths: [], backfill: [], storedMeta: stored, Ct);
        var second = await ReadSymbolAsync(harness, "N.C");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
            Assert.That(DeclaringFiles(second!), Is.EquivalentTo(new[] { "C.cs" }),
                "a deterministic per-file tag keeps the declaring set stable across re-runs");
        });
    }

    [Test]
    public async Task Backfill_file_captures_its_symbols_like_an_added_file()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        // A content-unchanged file whose node predates symbol extraction: it is
        // offered as back-fill (not added/updated) with no prior declared set.
        var file = WriteFile("src/Legacy.cs", "namespace N; public class Legacy { public void Run() { } }");

        var result = await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: [], backfill: [file],
            storedMeta: new Dictionary<string, StoredFileMeta>(), Ct);

        var type = await ReadSymbolAsync(harness, "N.Legacy");
        var method = await ReadSymbolAsync(harness, "N.Legacy.Run()");

        Assert.Multiple(() =>
        {
            Assert.That(type, Is.Not.Null, "a back-fill file's symbols are captured");
            Assert.That(DeclaringFiles(type!), Does.Contain("src/Legacy.cs"));
            Assert.That(method, Is.Not.Null);
            Assert.That(result.DeclaredByPath.ContainsKey("src/Legacy.cs"), Is.True,
                "a back-filled file is recorded so its node is stamped symbol-processed");
            Assert.That(result.DeclaredByPath["src/Legacy.cs"], Does.Contain("N.Legacy.Run()"));
        });
    }

    [Test]
    public async Task Backfill_of_supported_file_with_no_symbols_is_recorded_as_processed()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        // A supported file that declares nothing still must be recorded, so its node
        // is stamped processed and the back-fill scan never re-selects it.
        var file = WriteFile("src/Empty.cs", "// only a comment, no declarations\n");

        var result = await Reconciler(harness).ReconcileAsync(
            RepoId, _repoRoot, added: [], updated: [], removedPaths: [], backfill: [file],
            storedMeta: new Dictionary<string, StoredFileMeta>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.DeclaredByPath.ContainsKey("src/Empty.cs"), Is.True,
                "a supported file with zero symbols is still recorded as processed");
            Assert.That(result.DeclaredByPath["src/Empty.cs"], Is.Empty);
        });
    }
}
