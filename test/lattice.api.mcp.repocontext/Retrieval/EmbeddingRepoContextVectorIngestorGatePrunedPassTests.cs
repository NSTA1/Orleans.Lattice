using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins what an ingest pass does when the read-path access gate prunes its coverage
/// probe, so the pass cannot tell an uncovered source from one it was not permitted
/// to see.
/// <para>
/// The ingestor already handles this: a probe whose absence is not conclusive sets a
/// gate-pruned flag, the gap sweep stands down for the pass, and the outcome reports
/// coverage unestablished. That branch shipped with issue #2434 and, until this
/// fixture, <b>no test in the repository entered it</b> - the flag was introduced,
/// read in three places, and never executed under test. It was found by a whole-suite
/// mutation sweep of this bucket: inverting the branch condition reddened nothing,
/// and a follow-up reachability probe showed why - the line never ran at all. That is
/// the more expensive of the two zero-red states, because the remedy is not a missing
/// assertion but a missing test.
/// </para>
/// <para>
/// Why it matters beyond coverage bookkeeping: the gate-pruned path and the
/// probe-failed path converge on the same stand-down, but they are reached for
/// opposite reasons. A probe that FAULTED is an availability problem and resolves
/// itself. A probe that was PRUNED is a configuration decision, so it persists
/// silently for as long as the gate is configured that way, and the visible symptom
/// is a repository whose back-fill never runs while every pass reports success. If
/// the sweep were allowed to proceed on a pruned probe it would read the pruned keys
/// as absent coverage and re-embed the entire corpus on every pass, which is the
/// expensive failure this branch exists to prevent.
/// </para>
/// <para>
/// Every assertion here is paired with an admitted-probe arm in which the correct
/// answer DIFFERS from the degenerate one - two files embedded rather than one,
/// coverage established rather than not, warning absent rather than present. A
/// scenario whose correct answer equals the value a broken implementation would
/// produce cannot detect that implementation, so the contrast is the test.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorGatePrunedPassTests
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
        var root = Path.Combine(Path.GetTempPath(), "rc-gatepruned-" + Guid.NewGuid().ToString("N"));
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

    private static RepoContextMcpHarnessOptions WithGate(ReadPruningGate gate) =>
        new()
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
                // Last-wins override of the null gate AddLattice registered above.
                silo.Services.AddSingleton<ILatticeAccessGate>(gate),
        };

    private static RepoContextVectorWriter Writer(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextVectorWriter>();

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness,
        IEmbeddingProvider provider,
        ILogger<EmbeddingRepoContextVectorIngestor>? logger = null)
        => new(
            Writer(harness),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            logger ?? NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            provider);

    /// <summary>
    /// The gate-pruned pass embeds what it was explicitly given, stands the gap sweep
    /// down, and reports coverage unestablished. The admitted arm is the control: the
    /// same corpus, the same call, and a different answer on all three counts.
    /// </summary>
    [Test]
    public async Task A_gate_pruned_coverage_probe_stands_the_gap_sweep_down_and_reports_coverage_unestablished()
    {
        var root = NewRepo();
        var changed = WriteFile(root, "src/Changed.cs");
        var unchanged = WriteFile(root, "src/Unchanged.cs");

        var gate = new ReadPruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        // Arm the prune before the pass, so the coverage probe the pass runs is the
        // read that gets pruned. Writes stay admitted: a read-path gate does not stop
        // the pass recording what it embedded, and pruning them too would model a
        // different fault and confound the outcome.
        gate.PruneMembershipReads = true;

        var pruned = await ingestor.IngestAsync(
            RepoId, root, new[] { changed }, new[] { unchanged }, onProgress: null, Ct);

        gate.PruneMembershipReads = false;

        var admittedRoot = NewRepo();
        var admittedChanged = WriteFile(admittedRoot, "src/Changed.cs");
        var admittedUnchanged = WriteFile(admittedRoot, "src/Unchanged.cs");

        var openGate = new ReadPruningGate();
        await using var admittedHarness = await RepoContextMcpHarness.StartAsync(WithGate(openGate), Ct);
        var admittedIngestor = Ingestor(admittedHarness, new FakeEmbeddingProvider());

        var admitted = await admittedIngestor.IngestAsync(
            RepoId, admittedRoot, new[] { admittedChanged }, new[] { admittedUnchanged }, onProgress: null, Ct);

        Assert.Multiple(() =>
        {
            // The changed file is embedded either way: standing the sweep down must
            // not cost the pass the work it was explicitly asked to do.
            Assert.That(
                pruned.FilesEmbedded,
                Is.EqualTo(1),
                "The pruned pass must still embed the file it was given.");
            Assert.That(
                pruned.GapsSelected,
                Is.Zero,
                "A pruned probe cannot distinguish uncovered from unseen, so the sweep must not run.");
            Assert.That(
                pruned.CoverageEstablished,
                Is.False,
                "A pass that could not read coverage must not report that it established it.");

            // The control. An admitted probe sees the unchanged file is uncovered and
            // back-fills it, so the correct answer differs from the degenerate one on
            // every count above.
            Assert.That(
                admitted.FilesEmbedded,
                Is.EqualTo(2),
                "An admitted probe must back-fill the uncovered unchanged file.");
            Assert.That(
                admitted.GapsSelected,
                Is.EqualTo(1),
                "The unchanged file is a real gap and must be selected when coverage is readable.");
            Assert.That(
                admitted.CoverageEstablished,
                Is.True,
                "An admitted probe establishes coverage.");
        });
    }

    /// <summary>
    /// The pass says WHY it stood down, naming the gate rather than reporting a
    /// generic failure. The two stand-down causes are remediated by different people
    /// - a pruned probe is a configuration decision and a faulted one is an
    /// availability problem - so a line that does not distinguish them sends the
    /// operator to the wrong place.
    /// </summary>
    [Test]
    public async Task The_pass_reports_the_access_gate_as_the_reason_it_stood_the_sweep_down()
    {
        var root = NewRepo();
        var changed = WriteFile(root, "src/Changed.cs");
        var unchanged = WriteFile(root, "src/Unchanged.cs");

        var gate = new ReadPruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        var prunedLog = new RecordingLogger();
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider(), prunedLog);

        gate.PruneMembershipReads = true;
        await ingestor.IngestAsync(
            RepoId, root, new[] { changed }, new[] { unchanged }, onProgress: null, Ct);
        gate.PruneMembershipReads = false;

        var admittedRoot = NewRepo();
        var admittedChanged = WriteFile(admittedRoot, "src/Changed.cs");
        var admittedUnchanged = WriteFile(admittedRoot, "src/Unchanged.cs");

        var openGate = new ReadPruningGate();
        await using var admittedHarness = await RepoContextMcpHarness.StartAsync(WithGate(openGate), Ct);
        var admittedLog = new RecordingLogger();
        var admittedIngestor = Ingestor(admittedHarness, new FakeEmbeddingProvider(), admittedLog);

        await admittedIngestor.IngestAsync(
            RepoId, admittedRoot, new[] { admittedChanged }, new[] { admittedUnchanged }, onProgress: null, Ct);

        var prunedLines = prunedLog.Lines
            .Where(line => line.Message.Contains("access gate", StringComparison.OrdinalIgnoreCase))
            .ToArray();
        var admittedLines = admittedLog.Lines
            .Where(line => line.Message.Contains("access gate", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                prunedLines,
                Is.Not.Empty,
                "A pass that stood down because of the gate must say so.");
            Assert.That(
                prunedLines.Any(line => line.Level == LogLevel.Warning),
                Is.True,
                "The stand-down is operator-actionable and must not be logged below warning.");

            // The control: the same phrase must be absent when nothing was pruned, so
            // the assertion above is detecting the gate rather than matching a line
            // this pass always emits.
            Assert.That(
                admittedLines,
                Is.Empty,
                "An admitted pass must not report a gate prune.");
        });
    }

    /// <summary>
    /// The pass records which of the two coverage sources answered it. Both are
    /// legitimate and they have very different costs - the digest is a fixed number of
    /// page reads and the probe is linear in the corpus - so a repository that has
    /// quietly stopped using its digest is only visible here.
    /// <para>
    /// The flag behind this line shipped with issue #2434 and was asserted by nothing:
    /// the mutation sweep flipped it to its opposite value and the whole suite stayed
    /// green. Unlike the gate-pruned branch above, the line does run under test - it
    /// was executed and simply unasserted, which is the cheaper of the two states and
    /// is fixed by asserting it.
    /// </para>
    /// <para>
    /// The two arms are a gate-pruned pass and an admitted one, because that is the
    /// contrast the ingestor actually makes: the digest declines to build from a
    /// pruned membership scan (issue #2675), so the pass falls through to the probe
    /// and says so. Before that fix both arms reported the digest, because the digest
    /// built itself from the pruned read and claimed to be authoritative - which is
    /// why this assertion is also a regression guard on the seed path, not only on the
    /// log line.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_pass_records_which_source_answered_its_coverage_question()
    {
        var root = NewRepo();
        var first = WriteFile(root, "src/First.cs");

        var gate = new ReadPruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        var probeLog = new RecordingLogger();
        var probePass = Ingestor(harness, new FakeEmbeddingProvider(), probeLog);

        gate.PruneMembershipReads = true;
        await probePass.IngestAsync(
            RepoId, root, new[] { first }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);
        gate.PruneMembershipReads = false;

        var admittedRoot = NewRepo();
        var admittedFirst = WriteFile(admittedRoot, "src/First.cs");

        var openGate = new ReadPruningGate();
        await using var admittedHarness = await RepoContextMcpHarness.StartAsync(WithGate(openGate), Ct);
        var digestLog = new RecordingLogger();
        var digestPass = Ingestor(admittedHarness, new FakeEmbeddingProvider(), digestLog);

        await digestPass.IngestAsync(
            RepoId, admittedRoot, new[] { admittedFirst }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var probeSource = SourceLine(probeLog);
        var digestSource = SourceLine(digestLog);

        Assert.Multiple(() =>
        {
            Assert.That(
                probeSource,
                Is.Not.Null,
                "The pass must report which source answered its coverage question.");
            Assert.That(
                probeSource!.Message,
                Does.Contain("a per-source membership probe"),
                "A pruned membership scan cannot seed the digest, so the probe is what answered.");

            Assert.That(
                digestSource,
                Is.Not.Null,
                "The digest pass must report its source too.");
            Assert.That(
                digestSource!.Message,
                Does.Contain("the per-page coverage digest"),
                "An admitted scan seeds the digest, and reporting the probe would hide that it is in use.");
        });
    }

    private static RecordedLine? SourceLine(RecordingLogger logger)
        => logger.Lines.LastOrDefault(
            line => line.Message.Contains("resolved from", StringComparison.Ordinal));

    /// <summary>
    /// A gate that prunes membership-tree READS once armed, and admits everything
    /// else. Narrowing to reads is deliberate: the ingest under test writes membership
    /// for what it embeds, and a gate that pruned those writes as well would model a
    /// different fault entirely and make the outcome unattributable to the read.
    /// </summary>
    private sealed class ReadPruningGate : ILatticeAccessGate
    {
        private const LatticeOperation Reads = LatticeOperation.Read | LatticeOperation.RangeRead;

        /// <summary>Whether membership reads are currently pruned to nothing.</summary>
        public bool PruneMembershipReads { get; set; }

        /// <inheritdoc />
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var prune = PruneMembershipReads
                && (request.Operation & Reads) != LatticeOperation.None
                && string.Equals(request.TreeId, RepoContextTrees.VectorMembership, StringComparison.Ordinal);
            return new ValueTask<LatticeAccessDecision>(
                prune
                    ? LatticeAccessDecision.Filtered(static _ => false)
                    : LatticeAccessDecision.Allow());
        }
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
