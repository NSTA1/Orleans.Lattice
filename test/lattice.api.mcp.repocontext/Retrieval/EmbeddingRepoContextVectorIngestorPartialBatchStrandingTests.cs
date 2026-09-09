using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins what a file-arm pass reports when SOME of its embedding batches fail at the
/// embedding call itself (issues #2271, #2272).
/// <para>
/// The arm flattens every selected source's passages into one unit list and embeds
/// it in fixed-size batches. A batch whose embedding call returns unsuccessful, or
/// returns the wrong vector count, is skipped: every source carrying a unit in that
/// batch never completes, never has its membership recorded, and is re-selected by
/// the always-on gap sweep on every later pass.
/// </para>
/// <para>
/// That skip used to increment neither the batch-failure counter nor the
/// consecutive-failure counter, so it could not trip the saturation break however
/// many times it fired, and it was logged at Information. It is now accounted
/// exactly as a record failure is: Warning, both counters, the failed batch's source
/// keys named, and eligible to trip the break. The arm's <c>Saturated</c> flag also
/// reaches the caller now, as <see cref="RepoFileVectorIngestOutcome.Deferred"/>,
/// which <see cref="RepoFileVectorIngestOutcome.Converged"/> excludes - previously
/// the file arm computed that flag and discarded it, so a pass that deferred work
/// was indistinguishable from one that had none to do.
/// </para>
/// <para>
/// The ALL-fail case was never the dangerous one: it lands nothing, so the arm's
/// "no embedding batch succeeded" line fires and the failure is visible. The PARTIAL
/// case was the silent one, and it is the case these tests centre on - some batches
/// land, so that line is suppressed and the pass returns a healthy-looking outcome
/// with a non-zero embedded count and coverage established.
/// </para>
/// <para>
/// A single failed batch still reports a non-deferred pass, and that is deliberate
/// rather than an unfixed remnant: one batch failing and being retried on the next
/// reconcile is the arm's documented contract, and treating it as saturation would
/// stand the back-fill down for an ordinary blip. What changed is that the loss is
/// now counted and warned about at the moment it happens, instead of surfacing only
/// as a gap some later pass reports without being its cause.
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
        RepoContextMcpHarness harness,
        IEmbeddingProvider provider,
        ILogger<EmbeddingRepoContextVectorIngestor>? logger = null)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            logger ?? NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
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
        // arm's "no embedding batch succeeded" line - which is what used to make the
        // loss silent. It is now warned about as it happens; what this test pins is
        // that a lone failure is still reported as a non-deferred pass.
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
                "the pass presents as healthy: coverage established and a non-zero embedded count");
            Assert.That(
                cold.Deferred,
                Is.False,
                "one failed batch is not saturation; the arm retries it on the next reconcile rather than "
                + "standing the back-fill down, so a lone blip must not present as a deferred pass");
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

    /// <summary>
    /// The load-shedding half of the fix. Counting the failure is only worth doing if
    /// it can actually reach the saturation break, so this asserts the arm STOPS
    /// rather than merely complaining: the fourth batch is never attempted.
    /// </summary>
    [Test]
    public async Task Consecutive_embed_failures_trip_the_saturation_break_and_defer_the_remaining_batches()
    {
        // Four full batches, of which the first three fail. The fourth exists purely
        // so there is something left to defer; without it a break and a natural loop
        // exit would be indistinguishable.
        const int fileCount = EmbedBatchSize * 4;
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
        provider.FailEmbedCallOrdinals.Add(2);
        provider.FailEmbedCallOrdinals.Add(3);
        var ingestor = Ingestor(harness, provider);

        var cold = await ingestor.IngestAsync(
            RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                provider.EmbedCallCount,
                Is.EqualTo(3),
                "3 of the 4 batches were attempted: the break stopped the arm at the third consecutive "
                + "failure instead of driving the remaining batch into a store that is already failing. "
                + "Before the fix this was 4 of 4, because the skip incremented no counter");
            Assert.That(
                cold.Deferred,
                Is.True,
                "the arm's saturation signal now reaches the file arm's caller instead of being computed "
                + "and discarded");
            Assert.That(
                cold.Converged,
                Is.False,
                "a pass that gave up before looking at its remaining batches has not proved coverage, "
                + "however few gaps it happened to select");
            Assert.That(cold.FilesEmbedded, Is.Zero, "no batch succeeded, so nothing landed");
        });
    }

    /// <summary>
    /// The visibility half. A dropped batch is now reported at Warning as it happens,
    /// naming its sources, rather than at Information with wording that presented the
    /// loss as a fallback.
    /// </summary>
    [Test]
    public async Task A_failed_embedding_batch_warns_as_it_happens_and_names_its_sources()
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
        var logs = new RecordingLogger();
        var ingestor = Ingestor(harness, provider, logs);

        await ingestor.IngestAsync(RepoId, root, corpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var lines = logs.Lines;
        var embedFailures = lines
            .Where(l => l.Message.Contains("could not embed a batch", StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                embedFailures,
                Has.Length.EqualTo(1),
                $"exactly one batch was failed, so exactly one line reports it (of {lines.Count} captured)");
            Assert.That(
                embedFailures[0].Level,
                Is.EqualTo(LogLevel.Warning),
                "Information was the level that let a partial failure pass unnoticed");
            Assert.That(
                embedFailures[0].Message,
                Does.Contain("spanning 32 source(s)"),
                "the failed batch's sources are named, which is what separates the same sources failing "
                + "every pass from a different set each pass (issue #2208)");
            Assert.That(
                lines.Where(l => l.Message.Contains("fall back to keyword recall", StringComparison.Ordinal)),
                Is.Empty,
                "the old wording described the loss as a fallback, which read as a sanctioned outcome "
                + "rather than a fault; leaving it would keep a written justification for the behaviour "
                + "this change removed");
        });
    }

    private sealed record RecordedLine(LogLevel Level, string Message);

    private sealed class RecordingLogger : ILogger<EmbeddingRepoContextVectorIngestor>
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<RecordedLine> _lines = new();

        public IReadOnlyList<RecordedLine> Lines => _lines.ToArray();

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            _lines.Enqueue(new RecordedLine(logLevel, formatter(state, exception)));
        }
    }
}
