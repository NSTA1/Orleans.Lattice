using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for the embedding back-fill in
/// <see cref="EmbeddingRepoContextVectorIngestor"/> against a live in-memory
/// Lattice cluster and a deterministic <see cref="FakeEmbeddingProvider"/>. They
/// pin the durability contract that motivated the membership presence set: a run
/// embeds every changed file plus every unchanged file that has no live embedding
/// yet, records presence per batch, and converges so a re-offer of already-embedded
/// files embeds nothing - and a removed file's vector is retired.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (the reserved
/// vector trees) via <see cref="RepoContextMcpHarness"/> and reads file content off
/// a temp repo, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorBackfillTests
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
        var root = Path.Combine(Path.GetTempPath(), "rc-ingest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private RepoFileEntry Write(string root, string relativePath, string content)
    {
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        return new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
    }

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, IEmbeddingProvider? provider)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            provider);

    private static async Task<bool> IsEmbeddedAsync(
        RepoContextMcpHarness harness, string relativePath, CancellationToken ct)
    {
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, ct);
        var sourceId = VectorCodec.SourceId(RepoContextKeys.File(RepoId, relativePath));
        return members.Contains(sourceId);
    }

    [Test]
    public async Task Ingest_embeds_changed_files_and_records_membership()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A { void Alpha() {} }");
        var b = Write(root, "b.cs", "class B { void Beta() {} }");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        var embedded = (await ingestor.IngestAsync(
            RepoId, root, new[] { a, b }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct)).FilesEmbedded;

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(2), "Both changed files are embedded.");
            Assert.That(IsEmbeddedAsync(harness, "a.cs", Ct).Result, Is.True);
            Assert.That(IsEmbeddedAsync(harness, "b.cs", Ct).Result, Is.True);
        });
    }

    [Test]
    public async Task Ingest_converges_a_re_offer_of_embedded_files_embeds_nothing()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A {}");
        var b = Write(root, "b.cs", "class B {}");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestAsync(RepoId, root, new[] { a, b }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        // Re-offer the same files as unchanged: both already have a live vector, so
        // the back-fill embeds nothing. This is the convergence that proves the
        // presence check is a fixed point, not an endless re-embed.
        var second = (await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), new[] { a, b }, onProgress: null, Ct)).FilesEmbedded;

        Assert.That(second, Is.EqualTo(0), "Every unchanged file already has a live embedding, so nothing re-embeds.");
    }

    [Test]
    public async Task Ingest_back_fills_only_the_unchanged_file_that_has_no_embedding()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A {}");
        var b = Write(root, "b.cs", "class B {}");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        // Embed only A. Then a run offers A and B both as unchanged (the durability
        // gap: B's structural record was committed but its embedding never landed).
        await ingestor.IngestAsync(RepoId, root, new[] { a }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        var backfilled = (await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), new[] { a, b }, onProgress: null, Ct)).FilesEmbedded;

        Assert.Multiple(() =>
        {
            Assert.That(backfilled, Is.EqualTo(1), "Only B (the file with no live embedding) is back-filled.");
            Assert.That(IsEmbeddedAsync(harness, "a.cs", Ct).Result, Is.True);
            Assert.That(IsEmbeddedAsync(harness, "b.cs", Ct).Result, Is.True, "The gap is now closed.");
        });
    }

    [Test]
    public async Task Ingest_with_no_provider_embeds_nothing()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A {}");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, provider: null);

        var embedded = (await ingestor.IngestAsync(
            RepoId, root, new[] { a }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct)).FilesEmbedded;

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(0), "With no embedding provider the ingestor fails closed and records nothing.");
            Assert.That(IsEmbeddedAsync(harness, "a.cs", Ct).Result, Is.False);
        });
    }

    [Test]
    public async Task Ingest_with_an_unavailable_provider_embeds_nothing()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A {}");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider { Available = false };
        var ingestor = Ingestor(harness, provider);

        var embedded = (await ingestor.IngestAsync(
            RepoId, root, new[] { a }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct)).FilesEmbedded;

        Assert.That(embedded, Is.EqualTo(0), "An unavailable provider degrades to keyword recall: nothing is embedded.");
    }

    [Test]
    public async Task Ingest_skips_a_contentless_file_without_failing_the_batch()
    {
        var root = NewRepo();
        var real = Write(root, "a.cs", "class A {}");
        var empty = Write(root, "empty.cs", string.Empty);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        // The real Onyx server rejects a batch containing an empty string; the fake
        // models that, so this proves the ingestor filters the empty file out before
        // it can poison the batch.
        var provider = new FakeEmbeddingProvider { RejectEmptyStrings = true };
        var ingestor = Ingestor(harness, provider);

        var embedded = (await ingestor.IngestAsync(
            RepoId, root, new[] { real, empty }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct)).FilesEmbedded;

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "The contentless file is skipped; the real file still embeds.");
            Assert.That(IsEmbeddedAsync(harness, "a.cs", Ct).Result, Is.True);
            Assert.That(IsEmbeddedAsync(harness, "empty.cs", Ct).Result, Is.False);
        });
    }

    [Test]
    public async Task Retire_drops_a_removed_file_vector_and_membership()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A {}");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        await ingestor.IngestAsync(RepoId, root, new[] { a }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        Assert.That(IsEmbeddedAsync(harness, "a.cs", Ct).Result, Is.True, "Precondition: the file is embedded.");

        await ingestor.RetireAsync(RepoId, new[] { "a.cs" }, Ct);

        Assert.That(IsEmbeddedAsync(harness, "a.cs", Ct).Result, Is.False,
            "Deleting the file retires its embedding so the membership tally stays honest.");
    }

    [Test]
    public async Task Ingest_reports_incremental_progress_per_batch()
    {
        var root = NewRepo();
        var files = new List<RepoFileEntry>();
        for (var i = 0; i < 3; i++)
        {
            files.Add(Write(root, $"f{i}.cs", $"class F{i} {{ void M{i}() {{}} }}"));
        }

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        var progress = new List<int>();
        var embedded = (await ingestor.IngestAsync(
            RepoId,
            root,
            files,
            Array.Empty<RepoFileEntry>(),
            (count, _) =>
            {
                progress.Add(count);
                return ValueTask.CompletedTask;
            },
            Ct)).FilesEmbedded;

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(3));
            Assert.That(progress, Is.Not.Empty, "A run reports incremental progress after each batch lands.");
            Assert.That(progress[^1], Is.EqualTo(3), "The final progress callback carries the total embedded count.");
        });
    }

    [Test]
    public async Task Ingest_marks_a_contentless_file_so_a_re_offer_as_unchanged_embeds_nothing()
    {
        var root = NewRepo();
        var empty = Write(root, "empty.cs", "   \n\t  ");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        // First pass: the whitespace-only file carries no passage, so nothing embeds,
        // but it is recorded as a "considered, no passages" marker.
        var first = (await ingestor.IngestAsync(
            RepoId, root, new[] { empty }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct)).FilesEmbedded;

        // Second pass offers it as unchanged (the reconcile the bug looped on). Because
        // it is now covered by the marker, it is not re-selected and nothing re-reads it.
        var second = (await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), new[] { empty }, onProgress: null, Ct)).FilesEmbedded;

        var coverage = await writer.LoadCoverageAsync(RepoId, Ct);
        var sourceId = VectorCodec.SourceId(RepoContextKeys.File(RepoId, "empty.cs"));
        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(0), "A contentless file embeds no vector.");
            Assert.That(second, Is.EqualTo(0),
                "The marked contentless file is covered, so a reconcile re-offer stops re-driving it (the #1553 loop).");
            Assert.That(coverage.Contentless, Does.Contain(sourceId),
                "The file is recorded as considered-but-contentless.");
            Assert.That(coverage.IsCovered(sourceId), Is.True, "A marked file is covered, not a gap.");
        });
    }

    [Test]
    public async Task Ingest_does_not_count_a_contentless_marker_as_an_embedded_vector()
    {
        var root = NewRepo();
        var real = Write(root, "a.cs", "class A { void Alpha() {} }");
        var empty = Write(root, "empty.cs", string.Empty);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        await ingestor.IngestAsync(RepoId, root, new[] { real, empty }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var emptyId = VectorCodec.SourceId(RepoContextKeys.File(RepoId, "empty.cs"));
        var count = await writer.ScanEmbeddedAsync(RepoId, Ct);
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        var covered = await writer.LoadCoveredSourceIdsAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(1),
                "embeddedVectorCount counts only sources with a real landed vector, not contentless markers.");
            Assert.That(members, Does.Not.Contain(emptyId), "A marker is not a real embedded member.");
            Assert.That(covered, Does.Contain(emptyId), "But the covered set the gap sweep reads includes the marker.");
        });
    }

    [Test]
    public async Task Ingest_clears_the_contentless_marker_when_the_file_gains_content()
    {
        var root = NewRepo();
        var empty = Write(root, "grows.cs", string.Empty);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        // Considered contentless first, so it is marked.
        await ingestor.IngestAsync(RepoId, root, new[] { empty }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        // The file gains content and is re-offered as changed: it now embeds for real,
        // and the stale marker is cleared so the real embedding - not the marker - covers it.
        var grown = Write(root, "grows.cs", "class Grows { void Now() {} }");
        var embedded = (await ingestor.IngestAsync(
            RepoId, root, new[] { grown }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct)).FilesEmbedded;

        var sourceId = VectorCodec.SourceId(RepoContextKeys.File(RepoId, "grows.cs"));
        var coverage = await writer.LoadCoverageAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "The now-content-ful file embeds a real vector.");
            Assert.That(coverage.Embedded, Does.Contain(sourceId), "It is a real embedded member.");
            Assert.That(coverage.Contentless, Does.Not.Contain(sourceId),
                "The stale contentless marker is cleared on the content-gain transition.");
        });
    }

    [Test]
    public async Task Retire_clears_a_contentless_marker()
    {
        var root = NewRepo();
        var empty = Write(root, "empty.cs", string.Empty);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        await ingestor.IngestAsync(RepoId, root, new[] { empty }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        var sourceId = VectorCodec.SourceId(RepoContextKeys.File(RepoId, "empty.cs"));
        var before = await writer.LoadCoverageAsync(RepoId, Ct);
        Assert.That(before.Contentless, Does.Contain(sourceId), "Precondition: the empty file is marked.");

        // Deleting the file retires it: the marker must go too, or it lingers past the
        // file's deletion and keeps the source falsely covered.
        await ingestor.RetireAsync(RepoId, new[] { "empty.cs" }, Ct);

        var after = await writer.LoadCoverageAsync(RepoId, Ct);
        Assert.That(after.Contentless, Does.Not.Contain(sourceId),
            "Retiring a contentless file clears its marker so it is no longer covered.");
    }
}
