using System.Diagnostics.Metrics;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Drives the store's read-path access gate against the three ingestion arms that
/// stand down when it prunes their coverage probe, and pins what the coverage-probe
/// instrument reports in each case (issue #2964).
/// <para>
/// The stand-downs themselves are already tested elsewhere. What is new here is that
/// they are now <b>observable</b>. A gate-pruned stand-down is silent by design: the
/// probe answered, the gate did its job, the pass continued, and there is no error,
/// no fault, and no warning an operator has any reason to expect. On the scrape,
/// "did less work because there was less to do" and "did less work because it was not
/// permitted to see the work" were the same reading.
/// </para>
/// <para>
/// Every assertion is paired with an admitted-gate control in which the correct answer
/// DIFFERS - the conclusive arm advances and the gate-pruned arm does not. A scenario
/// whose correct answer equals the value a broken implementation would produce cannot
/// detect that implementation.
/// </para>
/// <para>
/// The sweep arm assertions are also a reachability proof. Before this fixture, no
/// test in the repository entered the gap scanner's gate-pruned branch at all, and
/// that branch is the strongest of the three sites: the other two fall silent and are
/// only later misread by a human, whereas this one returns a positive
/// <c>GapFound: false</c> that the self-heal grain consumes as a control decision with
/// no reader involved.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextCoverageProbeStandDownTests
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
        var root = Path.Combine(Path.GetTempPath(), "rc-standdown-" + Guid.NewGuid().ToString("N"));
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

    /// <summary>
    /// Resolves the gap scanner from the container rather than constructing one, so
    /// the assertions below also prove the reporter is actually registered and
    /// injected. That matters more than it looks: the scanner takes the reporter as an
    /// OPTIONAL parameter, and the container supplies the declared default for a
    /// parameter it cannot resolve rather than failing, so an unregistered reporter is
    /// a silent null and an instrument that exists on no host.
    /// </summary>
    private static RepoContextEmbeddingGapScanner Scanner(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextEmbeddingGapScanner>();

    private static RepoContextCoverageProbeReporter Reporter(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextCoverageProbeReporter>();

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness,
        RepoContextCoverageProbeReporter reporter,
        ILogger<EmbeddingRepoContextVectorIngestor>? logger = null)
        => new(
            Writer(harness),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Orleans.Serialization.Serializer>(),
            logger ?? NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider(),
            reporter);

    /// <summary>
    /// Captures the ingestor's warnings so a test can attribute a stand-down to the
    /// gate-pruning branch specifically.
    /// <para>
    /// <see cref="RepoContextIngestPassCensus.CoverageEstablished"/> is too weak to serve
    /// as that precondition: it is computed as "neither probe-failed nor gate-pruned", so
    /// a probe failure satisfies it just as well. A test that used it alone could pass
    /// while exercising a different stand-down than the one it names, which is the
    /// wrong-observable failure this fixture exists to avoid.
    /// </para>
    /// </summary>
    private sealed class WarningRecorder : ILogger<EmbeddingRepoContextVectorIngestor>
    {
        private readonly List<string> _warnings = new();

        public IReadOnlyList<string> Warnings
        {
            get
            {
                lock (_warnings)
                {
                    return _warnings.ToArray();
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
            if (logLevel != LogLevel.Warning)
            {
                return;
            }

            ArgumentNullException.ThrowIfNull(formatter);
            lock (_warnings)
            {
                _warnings.Add(formatter(state, exception));
            }
        }
    }

    private async Task SeedFileAsync(RepoContextMcpHarness harness, string relativePath)
    {
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await tree.SetAsync(RepoContextKeys.File(RepoId, relativePath), new byte[] { 1 }, Ct);
    }

    /// <summary>
    /// The gap sweep's gate-pruned stand-down scores the sweep arm as gate-pruned, and
    /// the admitted control scores it conclusive instead.
    /// <para>
    /// This is the site where the absent signal was most dangerous. The scanner does
    /// not merely fall silent: it returns <c>GapFound: false</c>, which the self-heal
    /// grain consumes as "this repository has no gaps". A permanently gate-pruned
    /// deployment therefore reports a converged bootstrap forever, produced entirely
    /// by a correctly functioning safety gate.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_sweep_arm_records_a_gate_pruned_stand_down_and_a_conclusive_scan_apart()
    {
        var gate = new ReadPruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await Writer(harness).AddMembersAsync(
            RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        var scanner = Scanner(harness);
        var reporter = Reporter(harness);

        // The admitted control first, so the pruned arm below cannot be satisfied by a
        // value that was already there.
        var admittedBefore = reporter.Snapshot();
        var admittedPage = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);
        var admittedAfter = reporter.Snapshot();

        gate.PruneMembershipReads = true;
        var prunedBefore = reporter.Snapshot();
        var prunedPage = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);
        var prunedAfter = reporter.Snapshot();
        gate.PruneMembershipReads = false;

        Assert.Multiple(() =>
        {
            // Preconditions: both scans genuinely went where this test claims.
            Assert.That(
                admittedPage.CoverageUnavailable,
                Is.False,
                "Precondition: the admitted scan resolved coverage.");
            Assert.That(
                prunedPage.CoverageUnavailable,
                Is.True,
                "Precondition: the pruned scan took the stand-down branch. Without this the arm "
                + "assertions below would be asserting about a path that was never entered.");
            Assert.That(
                prunedPage.GapFound,
                Is.False,
                "and it reported no gap, which is exactly the reading this instrument exists to "
                + "qualify - it is not evidence the repository is converged.");

            Assert.That(
                Delta(admittedBefore, admittedAfter, RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.Conclusive),
                Is.EqualTo(1),
                "An admitted scan scores the sweep arm conclusive,");
            Assert.That(
                Delta(admittedBefore, admittedAfter, RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.GatePruned),
                Is.Zero,
                "and not gate-pruned.");

            Assert.That(
                Delta(prunedBefore, prunedAfter, RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.GatePruned),
                Is.EqualTo(1),
                "A pruned scan scores the sweep arm gate-pruned,");
            Assert.That(
                Delta(prunedBefore, prunedAfter, RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.Conclusive),
                Is.Zero,
                "and NOT conclusive - a stand-down that also scored conclusive would leave the "
                + "converged reading exactly as unfalsifiable as it was before.");
            Assert.That(
                Delta(prunedBefore, prunedAfter, RepoContextCoverageProbeArm.Sweep, RepoContextCoverageProbeOutcome.ProbeFailed),
                Is.Zero,
                "and not probe-failed: the probe answered, which is what makes this stand-down "
                + "silent rather than faulty.");
        });
    }

    /// <summary>
    /// On the pruned early-return path, the arms that did NOT occur are still present
    /// on the scrape at zero.
    /// <para>
    /// This is the regression guard the issue asks for, and the ordinary
    /// after-the-fact counter assertions above cannot serve as one. If the priming
    /// were moved down onto the paths that charge the arms - the natural-looking
    /// refactor, because that is where the tags already are - then this scan, which
    /// returns early, would leave <c>conclusive</c> and <c>probe_failed</c> with no
    /// series at all. Their absence would then be indistinguishable from a build that
    /// shipped without the instrument, which is the precise ambiguity this epic exists
    /// to remove. Every assertion above would still pass.
    /// </para>
    /// <para>
    /// The listener is started before the harness because the reporter is constructed
    /// during container resolution, and the priming happens in its constructor.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_scan_that_returns_early_still_leaves_every_unused_arm_present_at_zero()
    {
        var sink = new List<(long Value, string? Arm, string? Outcome)>();
        using var listener = ListenForProbes(sink);

        var gate = new ReadPruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);

        await SeedFileAsync(harness, "src/A.cs");
        await Writer(harness).AddMembersAsync(
            RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        var scanner = Scanner(harness);

        gate.PruneMembershipReads = true;
        var page = await scanner.ScanFilePageAsync(RepoId, resumeKeyInclusive: null, pageSize: 100, Ct);
        gate.PruneMembershipReads = false;

        List<(long Value, string? Arm, string? Outcome)> measurements;
        lock (sink)
        {
            measurements = [.. sink];
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                page.CoverageUnavailable,
                Is.True,
                "Precondition: this scan returned early down the stand-down branch.");

            foreach (var outcome in new[]
                     {
                         RepoContextCoverageProbeReporter.OutcomeConclusiveTag,
                         RepoContextCoverageProbeReporter.OutcomeProbeFailedTag,
                     })
            {
                Assert.That(
                    measurements.Any(m =>
                        m.Value == 0
                        && string.Equals(m.Arm, RepoContextCoverageProbeReporter.ArmSweepTag, StringComparison.Ordinal)
                        && string.Equals(m.Outcome, outcome, StringComparison.Ordinal)),
                    Is.True,
                    $"arm=sweep outcome={outcome} did not occur on this pass, and must therefore be "
                    + "present at zero rather than absent. An absent series would mean the build lacks "
                    + "the instrument, which is a different fact entirely.");
            }

            // The arms of the two ingestor sites are equally present, even though this
            // pass ran neither of them. That is what lets an operator read "the file
            // arm never resolved coverage at all" off the scrape.
            foreach (var arm in new[]
                     {
                         RepoContextCoverageProbeReporter.ArmFileTag,
                         RepoContextCoverageProbeReporter.ArmSymbolTag,
                     })
            {
                Assert.That(
                    measurements.Any(m =>
                        m.Value == 0
                        && string.Equals(m.Arm, arm, StringComparison.Ordinal)),
                    Is.True,
                    $"arm={arm} was not exercised by this pass and must still be present at zero.");
            }
        });
    }

    /// <summary>
    /// The file arm of an ingest pass scores gate-pruned when the gate prunes its
    /// coverage probe, and conclusive when it does not. The pass itself is unchanged -
    /// it still embeds what it was given and still stands its sweep down - so the arm
    /// is the only thing that distinguishes the two runs on the scrape.
    /// </summary>
    [Test]
    public async Task The_file_arm_records_a_gate_pruned_pass_and_an_admitted_pass_apart()
    {
        var root = NewRepo();
        var changed = WriteFile(root, "src/Changed.cs");
        var unchanged = WriteFile(root, "src/Unchanged.cs");

        var gate = new ReadPruningGate();
        await using var harness = await RepoContextMcpHarness.StartAsync(WithGate(gate), Ct);
        using var reporter = new RepoContextCoverageProbeReporter();
        var prunedLog = new WarningRecorder();
        var ingestor = Ingestor(harness, reporter, prunedLog);

        gate.PruneMembershipReads = true;
        var prunedBefore = reporter.Snapshot();
        var pruned = await ingestor.IngestAsync(
            RepoId, root, new[] { changed }, new[] { unchanged }, onProgress: null, Ct);
        var prunedAfter = reporter.Snapshot();
        gate.PruneMembershipReads = false;

        var admittedRoot = NewRepo();
        var admittedChanged = WriteFile(admittedRoot, "src/Changed.cs");
        var admittedUnchanged = WriteFile(admittedRoot, "src/Unchanged.cs");

        var openGate = new ReadPruningGate();
        await using var admittedHarness = await RepoContextMcpHarness.StartAsync(WithGate(openGate), Ct);
        using var admittedReporter = new RepoContextCoverageProbeReporter();
        var admittedLog = new WarningRecorder();
        var admittedIngestor = Ingestor(admittedHarness, admittedReporter, admittedLog);

        var admittedBefore = admittedReporter.Snapshot();
        var admitted = await admittedIngestor.IngestAsync(
            RepoId, admittedRoot, new[] { admittedChanged }, new[] { admittedUnchanged }, onProgress: null, Ct);
        var admittedAfter = admittedReporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(
                prunedLog.Warnings.Any(w => w.Contains(
                    "removed by the store's read-path access gate",
                    StringComparison.Ordinal)),
                Is.True,
                "Precondition: the pruned pass took the GATE-PRUNING branch specifically. "
                + "CoverageEstablished is false for a probe failure too, so it cannot "
                + "attribute the stand-down on its own.");
            Assert.That(
                admittedLog.Warnings.Any(w => w.Contains(
                    "removed by the store's read-path access gate",
                    StringComparison.Ordinal)),
                Is.False,
                "Paired control: the admitted pass did not take that branch.");
            Assert.That(
                pruned.CoverageEstablished,
                Is.False,
                "Precondition: the pruned pass stood its sweep down.");
            Assert.That(
                admitted.CoverageEstablished,
                Is.True,
                "Precondition: the admitted pass established coverage.");

            Assert.That(
                Delta(prunedBefore, prunedAfter, RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.GatePruned),
                Is.EqualTo(1),
                "The pruned pass scores the file arm gate-pruned,");
            Assert.That(
                Delta(prunedBefore, prunedAfter, RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.Conclusive),
                Is.Zero,
                "and not conclusive.");

            Assert.That(
                Delta(admittedBefore, admittedAfter, RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.Conclusive),
                Is.EqualTo(1),
                "The admitted pass scores the file arm conclusive,");
            Assert.That(
                Delta(admittedBefore, admittedAfter, RepoContextCoverageProbeArm.File, RepoContextCoverageProbeOutcome.GatePruned),
                Is.Zero,
                "and not gate-pruned.");
        });
    }

    private static long Delta(
        RepoContextCoverageProbeSnapshot before,
        RepoContextCoverageProbeSnapshot after,
        RepoContextCoverageProbeArm arm,
        RepoContextCoverageProbeOutcome outcome)
        => after.Count(arm, outcome) - before.Count(arm, outcome);

    private static MeterListener ListenForProbes(List<(long Value, string? Arm, string? Outcome)> sink)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(instrument.Name, RepoContextCoverageProbeReporter.ProbeInstrumentName, StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? arm = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, RepoContextCoverageProbeReporter.ArmTagKey, StringComparison.Ordinal))
                {
                    arm = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextCoverageProbeReporter.OutcomeTagKey, StringComparison.Ordinal))
                {
                    outcome = tag.Value?.ToString();
                }
            }

            lock (sink)
            {
                sink.Add((value, arm, outcome));
            }
        });

        listener.Start();
        return listener;
    }

    /// <summary>
    /// A gate that prunes membership-tree READS once armed, and admits everything
    /// else. Narrowing to reads is deliberate: the passes under test write membership
    /// for what they embed, and a gate that pruned those writes as well would model a
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
}
