using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins the embedding back-fill as a fixed point at a corpus size that crosses the
/// membership probe's batching boundary, against a live in-memory Lattice cluster.
/// <para>
/// Issue #2208 reports a deployed repository whose zero-change pass keeps embedding
/// roughly 43 files, flat across 179 consecutive passes, so the index never
/// converges. Two mechanisms could produce that: batches that are embedded and then
/// discarded because the write contends on the vector trees (issues #2233 and
/// #2131), or a presence-check defect that reports a live vector absent and
/// re-embeds it regardless of contention. The two are distinguishable, and the
/// second is the one that would live in this code.
/// </para>
/// <para>
/// The existing convergence coverage in
/// <see cref="EmbeddingRepoContextVectorIngestorBackfillTests"/> re-offers two
/// files, which fits inside a single membership probe batch and a single embedding
/// batch, so it cannot see a defect that only appears once the probe fans out.
/// <see cref="RepoContextVectorWriter"/> probes membership in batches of 256 keys
/// and writes two keys per source (the embedded marker and the contentless
/// marker), so a corpus only converges across batches if every batch's result is
/// merged into one coverage set. This fixture offers a corpus several batches wide
/// and asserts the second pass embeds nothing, which is the acceptance criterion
/// #2208 states and the regression this file exists to catch.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and reads file content off a temp repo, so
/// it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorScaleConvergenceTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// The corpus size. The membership probe batches 256 keys and this arm probes
    /// two keys per source, so 300 sources is 600 keys: three probe batches, with
    /// the last one partial. That is the smallest shape that exercises both a batch
    /// boundary and a short final flush, which is where a merge defect would hide.
    /// </summary>
    private const int CorpusSize = 300;

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
        var root = Path.Combine(Path.GetTempPath(), "rc-scale-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    /// <summary>
    /// Writes a corpus whose files have distinct content, so each one chunks to at
    /// least one real passage and none is filtered out as contentless.
    /// </summary>
    private RepoFileEntry[] WriteCorpus(string root, int count)
    {
        var entries = new RepoFileEntry[count];
        for (var i = 0; i < count; i++)
        {
            var relativePath = "src/Type" + i.ToString("D4") + ".cs";
            var content = "namespace Acme.Generated;\n"
                + "public sealed class Type" + i.ToString("D4") + "\n"
                + "{\n"
                + "    public int Value" + i.ToString("D4") + " => " + i + ";\n"
                + "}\n";

            var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);
            File.WriteAllText(full, content);
            entries[i] = new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
        }

        return entries;
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
    public async Task A_second_quiet_pass_over_a_multi_batch_corpus_embeds_nothing()
    {
        var root = NewRepo();
        var corpus = WriteCorpus(root, CorpusSize);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);

        // The cold pass embeds the whole corpus as changed files.
        var cold = await ingestor.IngestAsync(
            RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        // The quiet pass offers the identical corpus as unchanged, which is exactly
        // what a zero-change reconcile does on the deployment. Every source already
        // has a live vector, so the gap sweep must select nothing and embed nothing.
        var quiet = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                cold.FilesEmbedded,
                Is.EqualTo(CorpusSize),
                "the cold pass must embed every changed file before convergence means anything");
            Assert.That(
                quiet.CoverageEstablished,
                Is.True,
                "the quiet pass must have completed its membership probe, or its verdict is worthless");
            Assert.That(
                quiet.GapsSelected,
                Is.Zero,
                "no unchanged file may be selected as a gap once its vector is live");
            Assert.That(
                quiet.FilesEmbedded,
                Is.Zero,
                "a second consecutive quiet pass must embed nothing (issue #2208)");
            Assert.That(
                quiet.Converged,
                Is.True,
                "a quiet pass that established coverage with no gaps is the converged verdict");
        });
    }

    [Test]
    public async Task A_third_quiet_pass_stays_converged()
    {
        var root = NewRepo();
        var corpus = WriteCorpus(root, CorpusSize);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);

        await ingestor.IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        await ingestor.IngestAsync(RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        var passagesBefore = provider.CapturedTexts.Count;

        // The reported symptom is a count that stays flat pass after pass rather
        // than decaying, so one converged pass is not enough to disprove it: the
        // third pass is what distinguishes a fixed point from a slow oscillation.
        var third = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(third.FilesEmbedded, Is.Zero, "the back-fill must remain a fixed point");
            Assert.That(third.Converged, Is.True);
            Assert.That(
                provider.CapturedTexts.Count,
                Is.EqualTo(passagesBefore),
                "a converged pass must not send a single passage to the embedder");
        });
    }
}
