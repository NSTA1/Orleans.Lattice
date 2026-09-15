namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for the cadence that gates the whole-repository embedding-gap scan.
/// <para>
/// Offering every unchanged file to the vector ingestor used to make the ingestor
/// probe membership for each one, which on a structurally converged repository was
/// pure waste and dominated the pass. That is what the back-off below exists to
/// avoid, and it still applies whenever the coverage digest is unbuilt and the
/// ingestor falls through to the old probe. The scan is skipped once coverage has
/// been observed complete, and re-armed by three independent conditions: the
/// periodic cadence coming due, prune consent being withheld (a deliberate full
/// sweep), and the self-index grain's out-of-band paged sweep forcing it after
/// finding a real gap.
/// </para>
/// <para>
/// With the digest built, detection reads a fixed number of rows on a tree of its
/// own rather than 2N rows on the membership tree, so the shipped cadence is now a
/// single reconcile spacing and the back-off does not engage at the defaults. The
/// tests that exercise the back-off therefore pin a wide cadence explicitly via
/// <c>BackOffOptions</c>, and
/// <see cref="The_shipped_default_cadence_re_checks_a_converged_repository_every_pass"/>
/// pins the default behaviour that replaced it.
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

    /// <summary>
    /// Options whose gap-scan cadence is far wider than any test here runs, so the
    /// periodic re-arm cannot fire and the back-off mechanism is observed in
    /// isolation.
    /// <para>
    /// The shipped default is deliberately one reconcile spacing (see
    /// <see cref="RepoContextIndexingOptions.EmbeddingGapScanInterval"/>), which
    /// re-arms on every pass and would therefore mask the back-off entirely. That
    /// default is not an accident to be worked around: detection now reads a
    /// fixed-size coverage digest rather than probing membership per source, so
    /// re-checking every pass is affordable. These tests pin a wide cadence because
    /// they are about the back-off, not about the constant.
    /// </para>
    /// </summary>
    private static RepoContextIndexingOptions BackOffOptions() =>
        new() { EmbeddingGapScanInterval = TimeSpan.FromHours(4) };

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
        using var harness = await ConvergedHarnessAsync(BackOffOptions());

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
        using var harness = await ConvergedHarnessAsync(BackOffOptions());

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
        using var harness = await ConvergedHarnessAsync(BackOffOptions());

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
        using var harness = await ConvergedHarnessAsync(BackOffOptions());

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

    [Test]
    public async Task The_shipped_default_cadence_re_checks_a_converged_repository_every_pass()
    {
        // This is the behavioural point of the coverage digest, pinned so it cannot
        // be quietly reverted by widening the constant. Detection no longer costs two
        // membership reads per indexed source; it reads a fixed-size digest whose cost
        // does not move with the corpus. Re-checking every pass is therefore affordable,
        // and it is what collapses the worst-case detection window from the former four
        // hours to a single reconcile.
        //
        // Note this is the same observable behaviour that
        // A_converged_repository_stops_offering_unchanged_files_for_a_gap_scan denies -
        // which is precisely why that test now pins a wide cadence explicitly. The
        // back-off still exists and still matters (an unbuilt digest falls through to
        // the old probe), it simply no longer engages at the shipped defaults.
        var options = new RepoContextIndexingOptions();
        using var harness = await ConvergedHarnessAsync(options);

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        Assert.Multiple(() =>
        {
            Assert.That(
                options.PassesPerEmbeddingGapScan,
                Is.EqualTo(1),
                "the shipped gap-scan interval must be the shortest window the scheduler can express");
            Assert.That(
                harness.UnchangedOfferedToIngestor,
                Is.Not.Empty,
                "at the shipped defaults a converged repository is re-checked on every pass");
        });
    }
}
