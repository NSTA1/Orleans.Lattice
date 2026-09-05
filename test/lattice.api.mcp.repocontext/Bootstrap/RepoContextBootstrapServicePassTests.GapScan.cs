namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for the cadence that gates the whole-repository embedding-gap probe.
/// <para>
/// Offering every unchanged file to the vector ingestor makes the ingestor probe
/// membership for each one, which on a structurally converged repository is pure
/// waste and dominates the pass. The probe is therefore skipped once coverage has
/// been observed complete, and re-armed by three independent conditions: the
/// periodic cadence coming due, prune consent being withheld (a deliberate full
/// sweep), and the self-index grain's out-of-band paged sweep forcing it after
/// finding a real gap. That last one is what keeps a converged repository healing
/// promptly rather than waiting out the cadence.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    /// <summary>
    /// Runs one pass over a harness seeded with a single already-indexed file, so
    /// the pass has an unchanged file it could offer for a gap scan and nothing
    /// else to do.
    /// </summary>
    private static async Task<BootstrapHarness> ConvergedHarnessAsync(
        RepoContextIndexingOptions? options = null,
        RepoFileVectorIngestOutcome? coldOutcome = null)
    {
        var harness = new BootstrapHarness(options: options);
        harness.WriteFile("src/a.cs", "class A { }");
        if (coldOutcome is { } outcome)
        {
            harness.IngestOutcome = outcome;
        }

        // The cold pass has no prior snapshot, so it always scans; it is what
        // publishes the "coverage is complete" verdict later passes read.
        await harness.Service.RunAsync(
            new RepoContextBootstrapRequest { RepoRoot = harness.RepoRoot, RepoId = RepoId, AllowPrune = true },
            progress: null);

        return harness;
    }

    private static RepoContextBootstrapRequest GapScanRequest(
        BootstrapHarness harness, bool allowPrune = true, bool force = false) =>
        new()
        {
            RepoRoot = harness.RepoRoot,
            RepoId = RepoId,
            AllowPrune = allowPrune,
            ForceEmbeddingGapScan = force,
        };

    [Test]
    public async Task A_converged_repository_stops_offering_unchanged_files_for_a_gap_scan()
    {
        using var harness = await ConvergedHarnessAsync();

        // The cold pass reported coverage established with no gaps, so the next
        // consented pass has nothing to re-probe: it offers an empty unchanged set
        // and the whole-corpus membership probe never runs.
        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Empty,
            "a pass over a converged repository must not re-probe every indexed source");
    }

    [Test]
    public async Task A_repository_with_an_outstanding_gap_is_re_scanned_every_pass()
    {
        // The cold pass scans and finds a gap. A probe that found a gap is not
        // convergence, so the verdict never flips and the next pass scans again
        // rather than backing off over a known hole.
        using var harness = await ConvergedHarnessAsync(
            coldOutcome: new RepoFileVectorIngestOutcome(0, 1, true));

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Not.Empty,
            "an unhealed gap must keep the scan armed on every pass");
    }

    [Test]
    public async Task A_probe_that_never_established_coverage_keeps_the_scan_armed()
    {
        // A failed or absent coverage probe is not evidence of convergence. Backing
        // off on it would turn a transient store fault into a permanently unhealed
        // corpus, so silence must re-arm rather than settle.
        using var harness = await ConvergedHarnessAsync(
            coldOutcome: new RepoFileVectorIngestOutcome(0, 0, false));

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(harness.UnchangedOfferedToIngestor, Is.Not.Empty);
    }

    [Test]
    public async Task A_verdict_is_only_taken_from_a_pass_that_actually_scanned()
    {
        // A pass that was offered nothing learned nothing, so its outcome must not
        // be allowed to overwrite the standing verdict in either direction. Without
        // this the skipped pass's empty result would immediately re-arm the scan and
        // the back-off would be worth nothing.
        using var harness = await ConvergedHarnessAsync();

        harness.IngestOutcome = RepoFileVectorIngestOutcome.None;
        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);
        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Empty,
            "the skipped pass reported nothing, which is not evidence that coverage regressed");
    }

    [Test]
    public async Task A_forced_gap_scan_re_arms_a_converged_repository()
    {
        using var harness = await ConvergedHarnessAsync();

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);
        Assert.That(harness.UnchangedOfferedToIngestor, Is.Empty, "precondition: the repository has settled");

        // The self-index grain's paged sweep found a real gap out of band. Without
        // this override the re-drive would heal nothing until the cadence came due,
        // so the sweep would re-trigger every cooldown forever: a livelock, not a fix.
        await harness.Service.RunAsync(GapScanRequest(harness, force: true), progress: null);

        Assert.That(
            harness.UnchangedOfferedToIngestor,
            Is.Not.Empty,
            "an out-of-band gap report must re-arm the in-pass scan immediately");
    }

    [Test]
    public async Task A_pass_without_prune_consent_re_arms_the_gap_scan()
    {
        using var harness = await ConvergedHarnessAsync();

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);
        Assert.That(harness.UnchangedOfferedToIngestor, Is.Empty, "precondition: the repository has settled");

        // Withholding prune consent is a caller asking for a complete sweep. The
        // embedding arm honours that the same way the walk does.
        await harness.Service.RunAsync(GapScanRequest(harness, allowPrune: false), progress: null);

        Assert.That(harness.UnchangedOfferedToIngestor, Is.Not.Empty);
    }

    [Test]
    public async Task The_periodic_cadence_re_arms_the_gap_scan_when_it_comes_due()
    {
        // One reconcile spacing per gap scan means the cadence is due on every pass,
        // which is exactly the pre-cadence behaviour and proves the gate is driven by
        // the configured interval rather than hard-wired off.
        var options = new RepoContextIndexingOptions { EmbeddingGapScanInterval = TimeSpan.Zero };
        using var harness = await ConvergedHarnessAsync(options);

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.Multiple(() =>
        {
            Assert.That(options.PassesPerEmbeddingGapScan, Is.EqualTo(1));
            Assert.That(
                harness.UnchangedOfferedToIngestor,
                Is.Not.Empty,
                "a cadence of one pass scans every pass, as it did before the cadence existed");
        });
    }
}
