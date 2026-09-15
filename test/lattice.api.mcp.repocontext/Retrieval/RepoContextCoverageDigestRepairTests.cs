using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for <b>targeted</b> gap repair through the per-page coverage
/// digest (issue #2486). Before the digest, having spent O(sources) to discover a
/// gap the only available remedy was a whole-repository re-ingest pass, so one
/// missing vector cost the same as ten thousand.
/// <para>
/// The assertion here is deliberately on the <b>number of embeds</b>, read off the
/// embedding provider itself, and never inferred from a timing improvement. A
/// faster pass is consistent with any number of causes; an embed count of one is
/// consistent with only one.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and reads file content off a temp repo, so
/// it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextCoverageDigestRepairTests
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
        var root = Path.Combine(Path.GetTempPath(), "rc-digest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private static RepoFileEntry Write(string root, string relativePath, string content)
    {
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        return new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
    }

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, IEmbeddingProvider provider)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            provider);

    [Test]
    public async Task Repairing_one_gap_in_a_converged_repository_costs_exactly_one_embed()
    {
        var root = NewRepo();
        var files = new List<RepoFileEntry>();
        for (var i = 0; i < 40; i++)
        {
            files.Add(Write(root, $"src/File{i:d3}.cs", $"class File{i:d3} {{ void M{i}() {{}} }}"));
        }

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        // Converge the repository, then build the digest.
        await ingestor.IngestAsync(RepoId, root, files, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        var digest = await writer.LoadCoverageDigestAsync(RepoId, Ct);
        Assert.That(digest.IsBuilt, Is.True);

        // Plant the gap: retire exactly one file's vector, leaving 39 covered. This is
        // the shape a lost embedding actually has - the file is unchanged on disk, so
        // no changed-file signal will ever select it; only coverage detection will.
        var stranded = files[17];
        await writer.RetireAsync(RepoId, RepoContextKeys.File(RepoId, stranded.RelativePath), Ct);

        provider.CapturedTexts.Clear();
        var before = provider.EmbedCallCount;

        // Re-offer every file as UNCHANGED. Nothing changed on disk, so every embed
        // this pass performs is a gap repair and nothing else.
        var result = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), files, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.CapturedTexts,
                Has.Count.EqualTo(1),
                "One missing vector must cost one embed. This is the whole of the "
                + "targeted-repair claim, asserted on the embedder rather than inferred "
                + "from how long the pass took.");
            Assert.That(
                provider.CapturedTexts[0],
                Does.Contain($"M17"),
                "And it must be the RIGHT one embed: a pass that embedded exactly one "
                + "arbitrary file would satisfy a bare count.");
            Assert.That(
                provider.EmbedCallCount - before,
                Is.EqualTo(1),
                "One batch, not one batch per page.");
            Assert.That(result.FilesEmbedded, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_converged_repository_with_no_gap_embeds_nothing()
    {
        // The negative control for the test above. Without it, an ingestor that
        // embedded exactly one file on every pass regardless of coverage would pass
        // the repair test and be badly broken.
        var root = NewRepo();
        var files = new List<RepoFileEntry>();
        for (var i = 0; i < 40; i++)
        {
            files.Add(Write(root, $"src/File{i:d3}.cs", $"class File{i:d3} {{ void M{i}() {{}} }}"));
        }

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);

        await ingestor.IngestAsync(RepoId, root, files, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        await harness.Services.GetRequiredService<RepoContextVectorWriter>()
            .LoadCoverageDigestAsync(RepoId, Ct);

        provider.CapturedTexts.Clear();
        var result = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), files, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(provider.CapturedTexts, Is.Empty);
            Assert.That(result.FilesEmbedded, Is.Zero);
        });
    }

    [Test]
    public async Task The_digest_path_repairs_a_gap_the_changed_file_signal_cannot_see()
    {
        // The end-to-end statement of what this item is for: the file is byte-identical
        // on disk and its structural record is committed, so nothing but coverage
        // detection can ever select it. If the digest arm under-reported coverage the
        // pass would re-embed everything; if it over-reported, the gap would never heal.
        var root = NewRepo();
        var files = new List<RepoFileEntry>
        {
            Write(root, "src/Alpha.cs", "class Alpha { void Alpha1() {} }"),
            Write(root, "src/Beta.cs", "class Beta { void Beta1() {} }"),
            Write(root, "src/Gamma.cs", "class Gamma { void Gamma1() {} }"),
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        await ingestor.IngestAsync(RepoId, root, files, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        await writer.LoadCoverageDigestAsync(RepoId, Ct);
        await writer.RetireAsync(RepoId, RepoContextKeys.File(RepoId, "src/Beta.cs"), Ct);

        var digest = await writer.LoadCoverageDigestAsync(RepoId, Ct);
        var coverage = digest.ProjectOnto(
            files.Select(f => VectorCodec.SourceId(RepoContextKeys.File(RepoId, f.RelativePath))));

        provider.CapturedTexts.Clear();
        await ingestor.IngestAsync(RepoId, root, Array.Empty<RepoFileEntry>(), files, onProgress: null, Ct);

        var repaired = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                coverage.IsCovered(VectorCodec.SourceId(RepoContextKeys.File(RepoId, "src/Beta.cs"))),
                Is.False,
                "Detection saw the gap from the digest alone.");
            Assert.That(provider.CapturedTexts, Has.Count.EqualTo(1));
            Assert.That(provider.CapturedTexts[0], Does.Contain("Beta"));
            Assert.That(
                repaired,
                Does.Contain(VectorCodec.SourceId(RepoContextKeys.File(RepoId, "src/Beta.cs"))),
                "And the repair landed: the source is a live member again.");
        });
    }
}
