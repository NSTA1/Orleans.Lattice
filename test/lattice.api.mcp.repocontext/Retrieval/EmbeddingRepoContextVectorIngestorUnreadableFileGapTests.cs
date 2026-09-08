using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins what a file that is selected for embedding but cannot be READ does to the
/// gap sweep, which is the one mechanism that produces a never-converging back-fill
/// (issue #2208) with no contention anywhere in the picture.
/// <para>
/// The file arm reads each selected file before embedding it. A read that fails
/// yields no content and the file is skipped without a coverage marker,
/// deliberately, so a later pass retries it once the file is readable. Whether that
/// retry ever terminates depends entirely on WHY the read failed, and the two cases
/// have opposite futures.
/// </para>
/// <para>
/// A file that is still PRESENT but unreadable - held under an exclusive lock, on a
/// bad sector, or behind a permission this process does not hold - is enumerated by
/// every subsequent walk, so it is offered to the gap sweep on every pass and the
/// retry never ends. That is the shape this fixture models, using a real exclusive
/// lock rather than a stand-in.
/// </para>
/// <para>
/// A file DELETED between the walk and the read fails identically at the read, and
/// because <see cref="FileNotFoundException"/> and
/// <see cref="DirectoryNotFoundException"/> both derive from
/// <see cref="IOException"/> it was counted as the same fault until issue #2269. It
/// is not the same: the next walk does not enumerate it, the plan classifies it
/// removed, and it is never offered to the gap sweep again. It self-heals within one
/// pass and is now reported separately from the faults that do not.
/// </para>
/// <para>
/// That distinction is load-bearing because an earlier revision of this fixture
/// modelled the persistent case with a file that was merely absent, described it in
/// prose as the deleted case, and then handed the same corpus to every pass -
/// re-offering a file the real walk would have dropped. The assertions were right
/// and the story attached to them was wrong, and the story is what a later reader
/// reasons from.
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
/// The persistent case is asserted as NON-converging on purpose. Retiring such a
/// file with a coverage marker would be worse than the gap it closes: a permission
/// fault is fixable by an operator and does not change the file's digest, so the
/// file would return to the unchanged set, be excluded by its own marker, and never
/// be embedded - a silent permanent coverage hole in place of a loud
/// non-convergence. If a later change gives it a retirement path, update this
/// fixture to assert the new contract rather than deleting it; the distinction it
/// draws is what makes the three mechanisms separable.
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

    private readonly List<FileStream> _locks = new();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [TearDown]
    public void TearDown()
    {
        // Release the exclusive locks first: a locked file cannot be deleted, so
        // reversing this order would leave temp trees behind on every run.
        foreach (var handle in _locks)
        {
            handle.Dispose();
        }

        _locks.Clear();

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
    /// Writes a file and holds it open with <see cref="FileShare.None"/>, so every
    /// read of it throws <see cref="IOException"/> for as long as the fixture runs.
    /// This is the PERSISTENT case: the file is on disk, so every walk enumerates it
    /// and the gap sweep is offered it on every pass. The lock is released in
    /// <see cref="TearDown"/>.
    /// </summary>
    private RepoFileEntry LockedFile(string root, string relativePath)
    {
        var entry = WriteFile(root, relativePath);
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        _locks.Add(new FileStream(full, FileMode.Open, FileAccess.Read, FileShare.None));
        return entry;
    }

    /// <summary>
    /// Describes a file the walk enumerated which is no longer on disk, so the read
    /// fails with <see cref="FileNotFoundException"/>. This is the DELETED case, and
    /// the corpus a later pass is given has to drop it to stay faithful to the real
    /// walk, which cannot enumerate a file that is not there.
    /// </summary>
    private static RepoFileEntry DeletedFile(string relativePath)
        => new(relativePath, "digest-" + relativePath, 128, "csharp");

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
    public async Task A_present_but_unreadable_file_is_re_selected_on_every_pass_and_never_converges()
    {
        var root = NewRepo();
        var readable = new[] { WriteFile(root, "src/A.cs"), WriteFile(root, "src/B.cs") };
        var corpus = new[] { readable[0], readable[1], LockedFile(root, "src/Locked.cs") };

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
                "only the two readable files can be embedded; the locked one cannot be read and is skipped");

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
        var locked = new[] { LockedFile(root, "src/L1.cs"), LockedFile(root, "src/L2.cs") };
        var corpus = new[] { readable[0], readable[1], locked[0], locked[1] };

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
                Is.EqualTo(locked.Length),
                "the residual gap set is exactly the files that cannot be read");
            Assert.That(quiet.FilesEmbedded, Is.Zero);
            Assert.That(
                provider.CapturedTexts.Count,
                Is.EqualTo(passagesBefore),
                "an unreadable file never reaches the embedder, so it costs no model call");
        });
    }

    [Test]
    public async Task A_file_deleted_between_the_walk_and_the_read_converges_once_the_walk_stops_offering_it()
    {
        var root = NewRepo();
        var readable = new[] { WriteFile(root, "src/A.cs"), WriteFile(root, "src/B.cs") };
        var racingCorpus = new[] { readable[0], readable[1], DeletedFile("src/Gone.cs") };

        // What the NEXT walk produces. It enumerates the live tree, which no longer
        // holds the deleted file, so the entry is simply absent and the plan
        // classifies it removed. Handing the racing corpus to a later pass, as an
        // earlier revision of this fixture did, models a walk that cannot happen and
        // makes a self-healing race look like a permanent fault.
        var laterCorpus = new[] { readable[0], readable[1] };

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var provider = new FakeEmbeddingProvider();
        var ingestor = Ingestor(harness, provider);

        var cold = await ingestor.IngestAsync(
            RepoId, root, racingCorpus, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        var second = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), laterCorpus, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                cold.FilesEmbedded,
                Is.EqualTo(2),
                "the two files that survived the race are embedded; the deleted one cannot be");

            Assert.That(
                second.GapsSelected,
                Is.Zero,
                "the deleted file is not offered again, so nothing is left to select");
            Assert.That(
                second.CoverageEstablished,
                Is.True,
                "the probe completed, so the verdict is meaningful and not a probe failure");
            Assert.That(
                second.Converged,
                Is.True,
                "a deletion race self-heals within one pass and needs no retirement path, "
                + "which is exactly what separates it from the present-but-unreadable case");
        });
    }

    [Test]
    public async Task A_deleted_file_is_reported_separately_from_a_file_that_is_present_but_unreadable()
    {
        var root = NewRepo();
        var readable = WriteFile(root, "src/A.cs");
        var locked = LockedFile(root, "src/Locked.cs");
        var deleted = DeletedFile("src/Gone.cs");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var logger = new RecordingLogger();
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider(), logger);

        await ingestor.IngestAsync(
            RepoId, root, new[] { readable, locked, deleted }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        RecordedLine? fault = null;
        RecordedLine? raced = null;
        foreach (var line in logger.Lines)
        {
            if (line.Level == LogLevel.Warning && line.Message.Contains("could not be read", StringComparison.Ordinal))
            {
                fault = line;
            }
            else if (line.Level == LogLevel.Information
                && line.Message.Contains("were deleted between", StringComparison.Ordinal))
            {
                raced = line;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.Not.Null, "a present-but-unreadable file is still reported as a fault");
            Assert.That(fault!.Message, Does.Contain("src/Locked.cs"));
            Assert.That(
                fault.Message,
                Does.Not.Contain("src/Gone.cs"),
                "a deletion race must not inflate the count whose REPETITION across passes is the "
                + "signal that a permanent gap set exists");

            Assert.That(raced, Is.Not.Null, "the deleted file is still reported, just not as a fault");
            Assert.That(raced!.Message, Does.Contain("src/Gone.cs"));
            Assert.That(
                raced.Message,
                Does.Not.Contain("src/Locked.cs"),
                "the two populations have opposite futures, so one line must never carry both");
        });
    }

    private sealed record RecordedLine(LogLevel Level, string Message);

    private sealed class RecordingLogger : ILogger<EmbeddingRepoContextVectorIngestor>
    {
        private readonly List<RecordedLine> _lines = new();

        public IReadOnlyList<RecordedLine> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToArray();
                }
            }
        }

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
            lock (_lines)
            {
                _lines.Add(new RecordedLine(logLevel, formatter(state, exception)));
            }
        }
    }
}
