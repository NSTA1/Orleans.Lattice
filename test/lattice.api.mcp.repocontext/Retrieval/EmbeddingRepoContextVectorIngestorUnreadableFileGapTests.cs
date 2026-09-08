using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins what a file that is selected for embedding but cannot be READ does to the
/// gap sweep, which is the one mechanism that produces a never-converging back-fill
/// (issue #2208) with no contention anywhere in the picture.
/// <para>
/// The file arm reads each selected file before embedding it. A read that throws
/// <see cref="IOException"/> or <see cref="UnauthorizedAccessException"/> yields
/// null and the file is skipped without a coverage marker, deliberately, so a later
/// pass retries it once the file is readable. That is right for a TRANSIENT
/// failure. For a PERSISTENT one - a file deleted between the walk and the read, or
/// held open by another process - the file is never covered, so the always-on gap
/// sweep re-selects it on every subsequent pass, forever. Note
/// <see cref="FileNotFoundException"/> and <see cref="DirectoryNotFoundException"/>
/// both derive from <see cref="IOException"/>, so a MISSING file takes this path;
/// it does not need a lock, and a pass on this deployment runs 6 to 17 minutes,
/// which is ample time for a build artifact to vanish underneath one.
/// </para>
/// <para>
/// This matters for diagnosis more than for throughput. The shape it produces - a
/// constant gap set with a constant digest - is exactly the shape produced by a
/// saturated vector plane deferring the same batches, and by a presence check that
/// reports a live vector absent. Three different mechanisms, one signature, so the
/// field evidence that looked decisive for any one of them was never decisive at
/// all. The contrast with the adjacent zero-window case is the point: a file that
/// reads but chunks to no passages IS retired from the gap set by a contentless
/// marker, and a file that cannot be read has no equivalent retirement.
/// </para>
/// <para>
/// These tests assert the behaviour as it stands, so they are characterisation, not
/// a specification. If a later change gives an unreadable file a retirement path,
/// this fixture should be updated to assert convergence rather than deleted - the
/// distinction it draws is what makes the three mechanisms separable.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and reads file content off a temp repo, so
/// it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorUnreadableFileGapTests
{
    private const string RepoId = "acme";

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
        var root = Path.Combine(Path.GetTempPath(), "rc-unreadable-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    /// <summary>
    /// Writes a readable file and returns the entry describing it.
    /// </summary>
    private static RepoFileEntry WriteFile(string root, string relativePath)
    {
        var content = "namespace Acme;\npublic sealed class C\n{\n    public int V => 1;\n}\n";
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        return new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
    }

    /// <summary>
    /// Describes a file the walk believes exists but which is not on disk, so the
    /// read fails. This is the deleted-underneath-the-pass case, and it needs no
    /// file lock to reproduce.
    /// </summary>
    private static RepoFileEntry MissingFile(string relativePath)
        => new(relativePath, "digest-" + relativePath, 128, "csharp");

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, IEmbeddingProvider provider)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            provider);

    [Test]
    public async Task An_unreadable_file_is_re_selected_on_every_pass_and_never_converges()
    {
        var root = NewRepo();
        var readable = new[] { WriteFile(root, "src/A.cs"), WriteFile(root, "src/B.cs") };
        var corpus = new[] { readable[0], readable[1], MissingFile("src/obj/Gone.cs") };

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);

        var cold = await ingestor.IngestAsync(
            RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var second = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);
        var third = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                cold.FilesEmbedded,
                Is.EqualTo(2),
                "only the two readable files can be embedded; the missing one yields null and is skipped");

            // The readable files converge, so the residual gap is attributable to the
            // unreadable file alone rather than to a corpus-wide failure.
            Assert.That(
                second.GapsSelected,
                Is.EqualTo(1),
                "the unreadable file has no coverage marker, so the gap sweep selects it");
            Assert.That(
                second.FilesEmbedded,
                Is.Zero,
                "selecting it achieves nothing, because it still cannot be read");
            Assert.That(
                second.CoverageEstablished,
                Is.True,
                "the probe completed, so the verdict is meaningful and not a probe failure");
            Assert.That(
                second.Converged,
                Is.False,
                "a non-empty gap set is correctly reported as not converged");

            // A fixed point, not a decay: this is the never-converging shape, and no
            // contention was involved in producing it.
            Assert.That(
                third.GapsSelected,
                Is.EqualTo(1),
                "the same file is re-selected on every subsequent pass, forever");
            Assert.That(third.FilesEmbedded, Is.Zero);
            Assert.That(third.Converged, Is.False);
        });
    }

    [Test]
    public async Task A_corpus_whose_readable_files_all_converge_still_reports_the_unreadable_residue()
    {
        var root = NewRepo();
        var readable = new[] { WriteFile(root, "src/A.cs"), WriteFile(root, "src/B.cs") };
        var missing = new[] { MissingFile("bin/Debug/X.deps.json"), MissingFile("obj/project.assets.json") };
        var corpus = new[] { readable[0], readable[1], missing[0], missing[1] };

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);

        await ingestor.IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        await ingestor.IngestAsync(RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        var passagesBefore = provider.CapturedTexts.Count;
        var quiet = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            // The residue is exactly the unreadable count, which is what makes a
            // repeating gap count in the field readable as "this many files can never
            // be embedded" rather than as a saturated write path.
            Assert.That(
                quiet.GapsSelected,
                Is.EqualTo(missing.Length),
                "the residual gap set is exactly the files that cannot be read");
            Assert.That(quiet.FilesEmbedded, Is.Zero);
            Assert.That(
                provider.CapturedTexts.Count,
                Is.EqualTo(passagesBefore),
                "an unreadable file never reaches the embedder, so it costs no model call");
        });
    }
}
