using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins what a file-arm pass reports when SOME of its embedding batches fail at the
/// embedding call itself - the one loss path between selection and batching that
/// produces no warning of any kind (issue #2271).
/// <para>
/// The arm flattens every selected source's passages into one unit list and embeds
/// it in fixed-size batches. A batch whose embedding call returns unsuccessful, or
/// returns the wrong vector count, is skipped with a <c>continue</c>. That skip
/// increments neither the batch-failure counter nor the consecutive-failure counter,
/// so it never trips the saturation break and never sets the arm's
/// <c>Saturated</c> flag; and it is logged at Information, not Warning. Every source
/// carrying a unit in that batch therefore never completes, never has its membership
/// recorded, and is re-selected by the always-on gap sweep on every later pass.
/// </para>
/// <para>
/// The ALL-fail case is not the dangerous one: it lands nothing, so the arm's
/// "no embedding batch succeeded" line fires and the failure is visible. The PARTIAL
/// case is the silent one, and it is the case these tests cover - some batches land,
/// so that line is suppressed, the pass returns a healthy-looking outcome with a
/// non-zero embedded count and coverage established, and the only trace of the
/// stranded sources is that a LATER pass selects them again. Nothing in the pass
/// that lost them says so.
/// </para>
/// <para>
/// The accounting hole is wider than the log level. <c>Saturated</c> is computed by
/// the shared embed helper but the file arm reads only <c>Landed.Count</c> from the
/// result and discards it - it is consumed solely by the symbol arm. So even a
/// record-failure saturation, which IS counted, cannot reach the file arm's caller:
/// <see cref="RepoFileVectorIngestOutcome"/> carries no field that distinguishes a
/// pass which deferred work from one which had none to do.
/// </para>
/// <para>
/// These tests assert the behaviour as it stands, so they are characterisation, not
/// a specification. If a later change gives a skipped batch a retirement path or
/// surfaces the signal on the outcome, update them to assert the new contract rather
/// than deleting them - the distinction they draw is what separates this mechanism
/// from vector-plane contention, which produces the same never-converging shape.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and reads file content off a temp repo, so
/// it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorPartialBatchStrandingTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// The arm's fixed embedding batch size. Mirrored here rather than referenced so
    /// the fixture states the boundary it is deliberately straddling; if the constant
    /// moves, the arithmetic below is what needs revisiting.
    /// </summary>
    private const int EmbedBatchSize = 32;

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
        var root = Path.Combine(Path.GetTempPath(), "rc-partial-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    /// <summary>
    /// Writes a file short enough to chunk to exactly one passage, so the unit count
    /// equals the file count and the batch boundary falls at a known file index.
    /// </summary>
    private static RepoFileEntry WriteSinglePassageFile(string root, int index)
    {
        var relativePath = $"src/F{index:D3}.cs";
        var content = $"namespace Acme;\npublic sealed class F{index}\n{{\n    public int V => {index};\n}}\n";
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
    public async Task A_failed_embedding_batch_strands_exactly_its_own_sources_while_the_pass_reports_success()
    {
        const int fileCount = 40;
        var root = NewRepo();
        var corpus = new RepoFileEntry[fileCount];
        for (var i = 0; i < fileCount; i++)
        {
            corpus[i] = WriteSinglePassageFile(root, i);
        }

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        // Fail only the FIRST batch. 40 single-passage files fill one full batch of 32
        // and a partial batch of 8, so the second batch lands and suppresses the
        // arm's "no embedding batch succeeded" line - which is what makes the loss
        // silent rather than merely unfixed.
        var provider = new FakeEmbeddingProvider();
        provider.FailEmbedCallOrdinals.Add(1);
        var ingestor = Ingestor(harness, provider);

        var cold = await ingestor.IngestAsync(
            RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.EmbedCallCount,
                Is.EqualTo(2),
                "the arm issued two batches; this is measured, so the split below is not inferred from the passage total");
            Assert.That(
                cold.FilesEmbedded,
                Is.EqualTo(fileCount - EmbedBatchSize),
                "only the surviving batch's sources landed");
            Assert.That(
                cold.CoverageEstablished,
                Is.True,
                "the pass presents as healthy: coverage established and a non-zero embedded count, "
                + "with nothing on the outcome recording that 32 sources were dropped");
        });

        // The stranded sources are invisible until a LATER pass re-selects them. That
        // delay is the whole diagnostic problem: the pass that loses the work reports
        // nothing, and the pass that reports a gap did not cause it.
        var quiet = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.That(
            quiet.GapsSelected,
            Is.EqualTo(EmbedBatchSize),
            "exactly the failed batch's sources are re-selected, so the residue is attributable to the "
            + "batch failure and not to a corpus-wide problem");
    }

    [Test]
    public async Task Clearing_the_batch_failure_converges_the_residue_it_created()
    {
        const int fileCount = 40;
        var root = NewRepo();
        var corpus = new RepoFileEntry[fileCount];
        for (var i = 0; i < fileCount; i++)
        {
            corpus[i] = WriteSinglePassageFile(root, i);
        }

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        provider.FailEmbedCallOrdinals.Add(1);
        var ingestor = Ingestor(harness, provider);

        await ingestor.IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        // With the failure cleared the residue drains, which is what distinguishes
        // this mechanism from an unreadable file: the sources are perfectly
        // embeddable and were lost only because a batch was dropped. A test that
        // stopped at the previous assertion could not tell the two apart.
        provider.FailEmbedCallOrdinals.Clear();
        var healing = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);
        var settled = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                healing.FilesEmbedded,
                Is.EqualTo(EmbedBatchSize),
                "the previously stranded sources embed once the batch succeeds");
            Assert.That(
                settled.GapsSelected,
                Is.Zero,
                "and the repository then converges, so the residue was caused by the dropped batch alone");
            Assert.That(settled.Converged, Is.True);
        });
    }
}
