using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Regression tests for issue #3483: the line a pass logs when it skips the
/// embedding-gap scan must say WHICH of the two back-off conditions held.
/// <para>
/// The gate skips the scan when coverage was last observed complete OR when the last
/// scan could not measure coverage at all (issue #3340). Those mean opposite things,
/// but the skip line claimed "coverage was last observed complete" for both, so a
/// repository with thousands of unscanned gaps - backed off because its last scan
/// ended saturated - read in a deployed log as a converged one. That misreading is
/// what #3483 was filed on.
/// </para>
/// <para>
/// Every test here pins <c>BackOffOptions</c>, so the periodic gap-scan term evaluates
/// false and the skip branch is actually entered. At the shipped default cadence the
/// scan is due on every pass and no skip line is ever written, which would make an
/// assertion on it vacuous.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    private const string SkipLineMarker = "skipping the embedding-gap scan";

    [Test]
    public async Task A_skip_after_an_unmeasurable_scan_does_not_claim_coverage_was_observed_complete()
    {
        // The last scan deferred under saturation, so it measured nothing. The gate
        // backs off to the cadence (#3340), and the line must say that rather than
        // assert a completeness nobody observed.
        using var harness = await ConvergedHarnessAsync(
            BackOffOptions(), new RepoFileVectorIngestOutcome(0, 0, true, Deferred: true));

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        var skip = LastSkipLine(harness);
        Assert.Multiple(() =>
        {
            Assert.That(harness.UnchangedOfferedToIngestor, Is.Empty, "precondition: the scan was skipped");
            Assert.That(skip, Is.Not.Null, "a skipped scan must log why it skipped");
            Assert.That(
                skip,
                Does.Not.Contain("last observed complete"),
                "an unmeasured scan observed nothing, so the skip must not claim coverage was complete");
            Assert.That(
                skip,
                Does.Contain("could NOT measure coverage"),
                "the skip must name the unmeasurable back-off as the condition that held");
        });
    }

    [Test]
    public async Task A_skip_after_a_converged_scan_still_reports_coverage_as_observed_complete()
    {
        // The control arm. A scan that genuinely measured and found no gap is the one
        // case the original wording was true of, and it must keep saying so: a fix
        // that merely deleted the claim would pass the test above and leave an
        // operator unable to tell the two back-offs apart in the other direction.
        using var harness = await ConvergedHarnessAsync(BackOffOptions());

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        var skip = LastSkipLine(harness);
        Assert.Multiple(() =>
        {
            Assert.That(harness.UnchangedOfferedToIngestor, Is.Empty, "precondition: the scan was skipped");
            Assert.That(skip, Is.Not.Null);
            Assert.That(skip, Does.Contain("coverage was last observed complete"));
            Assert.That(skip, Does.Not.Contain("could NOT measure coverage"));
        });
    }

    [Test]
    public async Task A_pass_that_offered_no_unchanged_file_cannot_turn_an_unmeasured_verdict_into_convergence()
    {
        // The mechanism #3483 suspected: a between-scans pass offers the ingestor only
        // the changed files, the ingestor finds nothing missing among them, and that
        // clean-looking outcome is carried into CoverageConverged and postpones the
        // scan. Pinned here so it cannot become true: the pass below is handed exactly
        // that outcome - coverage established, zero gaps, nothing deferred, the shape
        // Classify reads as Converged - while it offered no unchanged file.
        using var harness = await ConvergedHarnessAsync(
            BackOffOptions(), new RepoFileVectorIngestOutcome(0, 0, true, Deferred: true));

        harness.WriteFile("src/b.cs", "class B { }");
        harness.IngestOutcome = new RepoFileVectorIngestOutcome(1, 0, true);
        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);
        var offeredWhileNotDue = harness.UnchangedOfferedToIngestor;
        var changedWhileNotDue = harness.ChangedOfferedToIngestor;

        await harness.Service.RunAsync(GapScanRequest(harness), progress: null);

        var skip = LastSkipLine(harness);
        Assert.Multiple(() =>
        {
            Assert.That(offeredWhileNotDue, Is.Empty, "precondition: the pass offered no unchanged file");
            Assert.That(
                changedWhileNotDue.Select(f => f.RelativePath),
                Is.EqualTo(new[] { "src/b.cs" }),
                "precondition: the pass walked a changed file, the #3483 shape");
            Assert.That(skip, Is.Not.Null, "precondition: the following pass also skipped");
            Assert.That(
                skip,
                Does.Contain("could NOT measure coverage"),
                "the carried verdict must still be the unmeasured one: a pass that looked at no unchanged "
                + "file measured nothing about the unchanged corpus and cannot assert convergence over it");
        });
    }

    private static string? LastSkipLine(BootstrapHarness harness) =>
        harness.LogEntries
            .Where(e => e.Level == LogLevel.Information
                && e.Message.Contains(SkipLineMarker, StringComparison.Ordinal))
            .Select(e => e.Message)
            .LastOrDefault();
}
