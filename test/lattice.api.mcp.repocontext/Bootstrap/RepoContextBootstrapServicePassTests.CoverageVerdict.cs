namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for the coverage verdict a pass reaches and publishes - the decision that
/// drives the embedding gap scan's cadence, and the one instrument that says why it
/// came out the way it did.
/// <para>
/// Issue #3340: a refused coverage probe and a measured coverage gap were collapsed
/// into one boolean, so a probe the store refused under saturation read as evidence
/// of an incomplete corpus. That escalated the gap scan from its periodic cadence to
/// every pass, and the escalated whole-corpus sweep was itself the load that refused
/// the next probe. The loop was self-sustaining and its only external symptom was
/// unbounded write-ahead-log growth, because the re-sweep wrote a full re-walk every
/// pass while embedding almost nothing.
/// </para>
/// <para>
/// The behaviour these tests pin has two halves, and neither is sufficient alone.
/// An unmeasurable verdict must stop escalating the scan, AND it must remain
/// distinguishable from a converged one on the scrape - otherwise the cure is a
/// silent latch (issues #3320 and #2656) in which ingest stops with nothing to
/// trigger on. A measured gap and an arm failure still escalate, unchanged.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    /// <summary>
    /// A coverage outcome the pass could not measure at all: the membership probe
    /// did not establish coverage, so nothing it says about gaps is admissible.
    /// This is the live failure mode - a probe refused by a saturated store.
    /// </summary>
    private static RepoFileVectorIngestOutcome Unmeasurable => new(0, 0, false);

    /// <summary>
    /// Options whose gap-scan cadence spans exactly two passes, so the periodic
    /// re-arm is observable inside a test. <c>BackOffOptions</c> pins a cadence no
    /// test can reach, which proves the back-off engages but cannot prove it ever
    /// lifts; this one proves it does.
    /// </summary>
    private static RepoContextIndexingOptions TwoPassCadenceOptions() =>
        new() { EmbeddingGapScanInterval = TimeSpan.FromMinutes(40) };

    [Test]
    public async Task An_unmeasurable_probe_backs_the_gap_scan_off_to_the_periodic_cadence()
    {
        // The regression test for issue #3340. The cold pass could not establish
        // coverage, which is an UNKNOWN and not a finding: it observed no gap, so
        // scanning the whole corpus again next pass buys no detection, and on the
        // saturated store that refused the probe it is the load that refuses the
        // next one. Before the fix this offered every indexed source on every pass,
        // forever.
        using var harness = await ConvergedHarnessAsync(BackOffOptions(), Unmeasurable);

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Empty,
            "a pass that could not measure coverage has not observed a gap, so it must not escalate the scan");
    }

    [Test]
    public async Task An_unmeasurable_probe_backs_off_to_the_cadence_rather_than_latching_the_scan_off()
    {
        // The other half of the fix, and the reason the back-off is a back-off and
        // not an off switch. Standing the scan down on an unmeasured verdict would
        // be a latch if the verdict never got re-examined: the store recovers, the
        // corpus still has holes, and nothing ever looks again. The periodic cadence
        // is what lifts it, so it is pinned here rather than assumed.
        var options = TwoPassCadenceOptions();
        using var harness = await ConvergedHarnessAsync(options, Unmeasurable);

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);
        var backedOff = harness.UnchangedOfferedToIngestor;

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.Multiple(() =>
        {
            Assert.That(
                options.PassesPerEmbeddingGapScan,
                Is.EqualTo(2),
                "precondition: the cadence must span two passes for the re-arm to be observable");
            Assert.That(backedOff, Is.Empty, "the pass immediately after an unmeasurable verdict stands down");
            Assert.That(
                harness.UnchangedOfferedToIngestor,
                Is.Not.Empty,
                "the periodic cadence must re-examine an unmeasurable repository rather than abandon it");
        });
    }

    [Test]
    public async Task A_measured_gap_keeps_the_gap_scan_armed_under_a_wide_cadence()
    {
        // The perturbation guard on the test above: the back-off must be scoped to
        // the unmeasurable verdict alone. A pass that DID measure coverage and DID
        // see a hole has a finding, and a finding is exactly what the every-pass
        // escalation exists to act on. Widening the back-off to cover this case
        // would silence real gap detection, so this is pinned under the same wide
        // cadence that makes the previous test's Is.Empty meaningful.
        using var harness = await ConvergedHarnessAsync(BackOffOptions(), new RepoFileVectorIngestOutcome(0, 1, true));

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Not.Empty,
            "a measured gap is a finding, not an unknown, and must keep the scan armed on every pass");
    }

    [Test]
    public async Task A_deferred_embed_is_unmeasurable_even_when_coverage_was_established()
    {
        // Saturation is the live cause, and it presents on this arm too: the probe
        // answered, but the embed work was deferred, so the gap count reflects what
        // the pass got to rather than what is missing. Treating a deferred pass as a
        // measurement is the same category error as treating a refused probe as one.
        using var harness = await ConvergedHarnessAsync(
            BackOffOptions(), new RepoFileVectorIngestOutcome(0, 0, true, Deferred: true));

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(harness.UnchangedOfferedToIngestor, Is.Empty);
    }

    [Test]
    public async Task A_converged_pass_records_the_converged_verdict()
    {
        using var harness = await ConvergedHarnessAsync(BackOffOptions());

        Assert.That(
            harness.CoverageVerdictReporter.Snapshot().Count(RepoContextCoverageVerdict.Converged),
            Is.EqualTo(1),
            "a pass that measured complete coverage must say so, not merely stay quiet");
    }

    [Test]
    public async Task An_unmeasurable_pass_records_a_distinct_verdict_from_a_converged_one()
    {
        // The anti-latch requirement stated as an assertion. After the fix an
        // unmeasurable pass behaves like a converged one - it stops escalating the
        // scan - so if the two were not separable on the scrape, "we stopped
        // escalating because we cannot measure" would be indistinguishable from "we
        // stopped escalating because we are covered". That is the defect class of
        // issues #3320 and #2656, and it is what this partition exists to prevent.
        using var harness = await ConvergedHarnessAsync(BackOffOptions(), Unmeasurable);

        var snapshot = harness.CoverageVerdictReporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.ProbeUnmeasurable), Is.EqualTo(1));
            Assert.That(
                snapshot.Count(RepoContextCoverageVerdict.Converged),
                Is.Zero,
                "an unmeasured pass must never be counted as a converged one");
        });
    }

    [Test]
    public async Task A_measured_gap_records_the_gap_found_verdict()
    {
        using var harness = await ConvergedHarnessAsync(
            BackOffOptions(), new RepoFileVectorIngestOutcome(0, 1, true));

        var snapshot = harness.CoverageVerdictReporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count(RepoContextCoverageVerdict.GapFound), Is.EqualTo(1));
            Assert.That(
                snapshot.Count(RepoContextCoverageVerdict.ProbeUnmeasurable),
                Is.Zero,
                "a measured gap is not an unknown and must not be filed as one");
        });
    }

    [Test]
    public void An_arm_failure_records_the_arm_failure_verdict()
    {
        using var harness = new BootstrapHarness(options: BackOffOptions());
        harness.WriteFile("src/a.cs", "class A { }");
        harness.OnIngest = (_, _) => throw new InvalidOperationException("embedder down");

        // The file arm's fault is banked and re-thrown once every other arm has had
        // its turn, so the verdict seam is still reached on the way out.
        Assert.That(
            async () => await harness.Service.RunAsync(GapScanRequest(harness), progress: null),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(
            harness.CoverageVerdictReporter.Snapshot().Count(RepoContextCoverageVerdict.ArmFailure),
            Is.EqualTo(1),
            "an arm that threw makes this pass's coverage facts inadmissible, which is its own reason");
    }

    [Test]
    public async Task A_pass_that_did_not_scan_records_no_verdict()
    {
        // The verdict is only ever published by a pass that actually looked. A pass
        // that backed off learned nothing, and recording a verdict for it would
        // manufacture a reading out of an absence of work.
        using var harness = await ConvergedHarnessAsync(BackOffOptions());
        var afterColdPass = harness.CoverageVerdictReporter.Snapshot().Total;

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(harness.CoverageVerdictReporter.Snapshot().Total, Is.EqualTo(afterColdPass));
    }
}
