using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins the file arm's cross-pass gap-back-fill backoff (issue #2208), which is the
/// file-arm twin of the symbol arm's remedy for issues #2071 and #2078.
/// <para>
/// The deployed symptom was a zero-change pass that kept embedding the same closed
/// pool of files on every reconcile, flat across 179 consecutive passes: the rolling
/// union of gap selections stayed flat while files kept entering and leaving it, so
/// the same set was being re-selected rather than fresh vectors being lost. The
/// symbol arm was given a backoff for exactly that loop and converged; the file arm
/// was left without one and did not.
/// </para>
/// <para>
/// These tests also carry the load-bearing half of the change, which is not the
/// backoff itself but what it does to the READING of a quiet pass. Once this arm can
/// skip its gap scan, <c>GapsSelected == 0</c> stops discriminating: it is satisfied
/// both by "scanned and genuinely found nothing" and by "never scanned, because the
/// backoff was engaged". A regression could then be fully masked with the existing
/// convergence tests still green. So the skip is surfaced on the outcome as
/// <see cref="RepoFileVectorIngestOutcome.GapScanSkipped"/> and excluded from
/// <see cref="RepoFileVectorIngestOutcome.Converged"/>, and the first test below
/// EXHIBITS the ambiguous state rather than arguing about it: a pass that selects
/// zero gaps over a corpus with no coverage at all.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and reads file content off a temp repo, so
/// it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorFileGapBackoffTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// The arm's fixed embedding batch size, mirrored so the fixture states the
    /// boundary its arithmetic depends on.
    /// </summary>
    private const int EmbedBatchSize = 32;

    /// <summary>
    /// Four full batches of single-passage files. Three consecutive failures trip
    /// <see cref="EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures"/>
    /// with a fourth batch left to defer, which is what makes the pass saturated
    /// rather than merely unlucky.
    /// </summary>
    private const int CorpusSize = EmbedBatchSize * 4;

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
        var root = Path.Combine(Path.GetTempPath(), "rc-backoff-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    /// <summary>
    /// Writes a corpus of files short enough to chunk to exactly one passage each, so
    /// the batch count is the file count divided by the batch size.
    /// </summary>
    private RepoFileEntry[] WriteCorpus(string root, int count)
    {
        var entries = new RepoFileEntry[count];
        for (var i = 0; i < count; i++)
        {
            var relativePath = $"src/F{i:D3}.cs";
            var content = $"namespace Acme;\npublic sealed class F{i}\n{{\n    public int V => {i};\n}}\n";
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

    /// <summary>
    /// The core test, and the one that carries the non-vacuity argument. A saturated
    /// pass stands the back-fill down; the pass that skips then selects zero gaps
    /// over a corpus in which NOTHING is covered, which is precisely the reading
    /// <c>GapsSelected == 0</c> can no longer be trusted to make. The third pass is
    /// the denominator: it runs the scan and selects the whole corpus, proving the
    /// zero was the skip and not convergence.
    /// </summary>
    [Test]
    public async Task A_pass_that_skips_the_back_fill_selects_no_gaps_and_must_not_read_as_converged()
    {
        var root = NewRepo();
        var corpus = WriteCorpus(root, CorpusSize);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var provider = new FakeEmbeddingProvider { FailEmbeds = true };
        var ingestor = Ingestor(harness, provider);

        var cold = await ingestor.IngestAsync(
            RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                cold.Deferred,
                Is.True,
                "the cold pass must actually saturate, or the backoff under test is never engaged and every "
                + "assertion below passes for the wrong reason. If this fails, fix the saturation setup rather "
                + "than weakening the assertion");
            Assert.That(cold.FilesEmbedded, Is.Zero, "no batch succeeded, so the whole corpus is uncovered");
            Assert.That(
                ingestor.FileGapScanBackoffRemaining(RepoId),
                Is.EqualTo(1),
                "one saturated pass grants a one-pass skip budget");
        });

        // The failures are cleared, so nothing about the embedder explains what the
        // next pass does. Every file is unchanged and uncovered: a scanning pass would
        // select all of them.
        provider.FailEmbeds = false;

        var skipped = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                skipped.GapScanSkipped,
                Is.True,
                "the granted budget must be consumed by this pass");
            Assert.That(
                skipped.GapsSelected,
                Is.Zero,
                "this is the ambiguous state itself: zero gaps selected over a corpus with no coverage at all");
            Assert.That(skipped.FilesEmbedded, Is.Zero);
            Assert.That(
                skipped.CoverageEstablished,
                Is.True,
                "the probe still ran, so the zero above cannot be blamed on a failed probe either");
            Assert.That(
                skipped.Converged,
                Is.False,
                "a pass that never asked has proved nothing. This is the assertion that keeps GapsSelected == 0 "
                + "from silently coming to mean 'did not look' (issue #2208)");
        });

        // The denominator. With the budget spent, this pass runs the scan over the
        // identical input, and selecting the whole corpus is what proves the previous
        // pass's zero was the skip rather than convergence.
        var scanning = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                scanning.GapScanSkipped,
                Is.False,
                "the budget was one pass, so this pass must scan");
            Assert.That(
                scanning.GapsSelected,
                Is.EqualTo(CorpusSize),
                "every file was uncovered the whole time. If this is zero the corpus was never a gap and the "
                + "test above proved nothing; fix the setup rather than lowering this floor");
            Assert.That(scanning.FilesEmbedded, Is.EqualTo(CorpusSize));
        });
    }

    /// <summary>
    /// A skipped pass must not clear the budget it was granted by. Without the guard
    /// one skip fakes recovery and the backoff disarms itself, leaving the loop
    /// exactly as it was - the failure mode being that the fix appears to work while
    /// doing nothing.
    /// </summary>
    [Test]
    public async Task A_skipped_pass_does_not_clear_the_budget_it_was_granted_by()
    {
        var root = NewRepo();
        var corpus = WriteCorpus(root, CorpusSize);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var provider = new FakeEmbeddingProvider { FailEmbeds = true };
        var ingestor = Ingestor(harness, provider);

        // Two consecutive saturated passes, so the budget reaches two and a cleared
        // budget is distinguishable from a decremented one. The second pass offers the
        // corpus as changed, because a skipped pass still embeds changed files and can
        // therefore still saturate.
        await ingestor.IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        await ingestor.IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        Assert.That(
            ingestor.FileGapScanBackoffRemaining(RepoId),
            Is.EqualTo(2),
            "two consecutive saturated passes double the budget. If this is not two the sequence below cannot "
            + "discriminate a cleared budget from a decremented one, so fix the setup rather than the assertion");

        // A clean pass that SKIPPED. It is not evidence the plane recovered, because
        // it never touched the membership tree hard enough to find out.
        provider.FailEmbeds = false;
        var skipped = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(skipped.GapScanSkipped, Is.True);
            Assert.That(
                ingestor.FileGapScanBackoffRemaining(RepoId),
                Is.EqualTo(1),
                "the budget is decremented by the skip and NOT cleared by it. A cleared budget reads as zero "
                + "here, which is the bug this guard exists to prevent");
        });
    }

    /// <summary>
    /// The backoff must be a pause, not an off switch. Once a pass actually runs the
    /// back-fill without saturating, the budget is cleared outright and the arm
    /// returns to reporting real convergence.
    /// </summary>
    [Test]
    public async Task A_completed_back_fill_clears_the_backoff_and_the_arm_converges_again()
    {
        var root = NewRepo();
        var corpus = WriteCorpus(root, CorpusSize);

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        var provider = new FakeEmbeddingProvider { FailEmbeds = true };
        var ingestor = Ingestor(harness, provider);

        var cold = await ingestor.IngestAsync(
            RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        Assert.That(cold.Deferred, Is.True, "the backoff must have been engaged for its clearing to mean anything");

        provider.FailEmbeds = false;

        // Pass 2 spends the one-pass budget, pass 3 runs the back-fill and heals the
        // corpus, pass 4 is the quiet pass that must now read as genuinely converged.
        await ingestor.IngestAsync(RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);
        var healing = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);
        var settled = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), corpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                healing.FilesEmbedded,
                Is.EqualTo(CorpusSize),
                "the pass that resumed the back-fill embedded the whole uncovered corpus");
            Assert.That(
                ingestor.FileGapScanBackoffRemaining(RepoId),
                Is.Zero,
                "a full back-fill without saturation clears the budget outright, so the backoff is a pause "
                + "rather than an off switch");
            Assert.That(settled.GapScanSkipped, Is.False);
            Assert.That(settled.GapsSelected, Is.Zero);
            Assert.That(settled.FilesEmbedded, Is.Zero);
            Assert.That(
                settled.Converged,
                Is.True,
                "and a quiet pass that actually scanned reports the convergence it earned");
        });
    }
}
