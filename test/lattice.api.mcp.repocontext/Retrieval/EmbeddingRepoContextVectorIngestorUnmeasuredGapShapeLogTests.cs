using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Regression tests for issue #3483: the ingestor's "gap set shape not measured"
/// line must not call a pass convergence when that pass was offered no
/// content-unchanged file.
/// <para>
/// The coordinator withholds the unchanged set whenever its gap scan is not due, so
/// a between-scans pass hands the ingestor only the files that changed. With nothing
/// unchanged to probe the selection is empty by construction, and the line used to
/// fall through to "This IS convergence for the file arm" - read in a deployed log as
/// a verdict over a corpus the pass never looked at. The second arm is the control:
/// a pass that WAS offered an unchanged, already-covered file genuinely measured the
/// corpus and must keep saying so, so a fix that merely deleted the claim cannot
/// pass.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorUnmeasuredGapShapeLogTests
{
    private const string RepoId = "acme";

    private const string ShapeLineMarker = "back-fill gap set shape not measured";

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

    [Test]
    public async Task A_pass_offered_no_unchanged_file_does_not_claim_file_arm_convergence()
    {
        var root = NewRepo();
        var changed = WriteFile(root, "src/Changed.cs");

        await using var harness = await RepoContextMcpHarness.StartAsync(Writer(), Ct);
        var log = new RecordingLogger();
        var ingestor = Ingestor(harness, log);

        // First pass: the only file is new, and it is the whole of what the pass is
        // given. It lands, so the file is now covered.
        await ingestor.IngestAsync(
            RepoId, root, new[] { changed }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        // Second pass, the #3483 shape: the file changed again and the gap scan is not
        // due, so the unchanged set is empty. The selection is empty only because
        // nothing was offered to select from.
        var shapeLinesBefore = ShapeLines(log).Length;
        var outcome = await ingestor.IngestAsync(
            RepoId, root, new[] { changed with { Digest = "digest-v2" } }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var line = ShapeLines(log).Skip(shapeLinesBefore).LastOrDefault();
        Assert.Multiple(() =>
        {
            Assert.That(outcome.GapsSelected, Is.Zero, "precondition: the pass selected no gap");
            Assert.That(line, Is.Not.Null, "precondition: the pass logged that it measured no gap shape");
            Assert.That(
                line!.Message,
                Does.Not.Contain("IS convergence"),
                "a pass offered no unchanged file measured nothing about the unchanged corpus");
            Assert.That(line.Message, Does.Contain("This is NOT a convergence verdict"));
            Assert.That(line.Message, Does.Contain("unchanged=0"));
        });
    }

    [Test]
    public async Task A_pass_offered_an_already_covered_unchanged_file_still_reports_file_arm_convergence()
    {
        var root = NewRepo();
        var file = WriteFile(root, "src/Covered.cs");

        await using var harness = await RepoContextMcpHarness.StartAsync(Writer(), Ct);
        var log = new RecordingLogger();
        var ingestor = Ingestor(harness, log);

        await ingestor.IngestAsync(
            RepoId, root, new[] { file }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        // The file is now covered and is offered as unchanged: the probe genuinely
        // looked at it and found nothing missing. That IS convergence.
        var shapeLinesBefore = ShapeLines(log).Length;
        var outcome = await ingestor.IngestAsync(
            RepoId, root, Array.Empty<RepoFileEntry>(), new[] { file }, onProgress: null, Ct);

        var line = ShapeLines(log).Skip(shapeLinesBefore).LastOrDefault();
        Assert.Multiple(() =>
        {
            Assert.That(outcome.GapsSelected, Is.Zero, "precondition: the covered file is not a gap");
            Assert.That(outcome.CoverageEstablished, Is.True, "precondition: the probe measured coverage");
            Assert.That(line, Is.Not.Null);
            Assert.That(line!.Message, Does.Contain("This IS convergence for the file arm"));
            Assert.That(line.Message, Does.Not.Contain("NOT a convergence verdict"));
        });
    }

    private string NewRepo()
    {
        var root = Path.Combine(Path.GetTempPath(), "rc-gapshape-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private static RepoFileEntry WriteFile(string root, string relativePath)
    {
        var content = "namespace Acme;\npublic sealed class C\n{\n    public int V => 1;\n}\n";
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        return new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
    }

    private static RepoContextMcpHarnessOptions Writer() =>
        new() { Posture = RepoContextMcpAuthPosture.Writer };

    private static EmbeddingRepoContextVectorIngestor Ingestor(RepoContextMcpHarness harness, RecordingLogger logger)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            logger,
            new FakeEmbeddingProvider());

    private static RecordedLine[] ShapeLines(RecordingLogger logger)
        => logger.Lines
            .Where(line => line.Message.Contains(ShapeLineMarker, StringComparison.Ordinal))
            .ToArray();

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
