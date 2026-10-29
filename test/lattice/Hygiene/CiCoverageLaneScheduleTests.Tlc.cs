using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The nightly lane's exhaustive TLC legs (#5153): <c>spec/README.md</c> and
/// <c>tier-scope.py</c> say nightly coverage runs the exhaustive TLC shards, and these
/// assertions keep that statement true. The legs run every TLC shard without coverlet,
/// never restore a verdict receipt, are bounded by a planned per-leg timeout, and only the
/// tracking job may write issues. A job of its own writes a receipt for every module the
/// night verified (#5165), so main's scope stays warm for later pull requests.
/// </summary>
public sealed partial class CiCoverageLaneScheduleTests
{
    /// <summary>
    /// The TLC legs follow the nightly skip, run through the CI leg runner from the plan's
    /// own TLC matrix, carry the planner's per-leg timeout, and are not instrumented.
    /// </summary>
    [Test]
    public void The_nightly_lane_runs_every_tlc_shard_uninstrumented_under_a_planned_timeout()
    {
        var yaml = Read();
        var tlc = Job(yaml, "tlc");
        var plan = Job(yaml, "plan");

        Assert.Multiple(() =>
        {
            Assert.That(plan, Does.Contain("--output-tlc-matrix"),
                "the plan job must plan the TLC legs, or nothing in the nightly lane runs TLC and "
                + "spec/README.md's statement that nightly coverage runs the exhaustive shards is false");

            Assert.That(tlc, Does.Contain("fromJson(needs.plan.outputs.tlc_legs)"),
                "the TLC job must fan out over the planned TLC matrix");

            Assert.That(tlc, Does.Contain("needs.measured.outputs.measure != 'false'").And.Contain("!cancelled()"),
                "the TLC job must follow the same fail-open nightly skip as the coverage legs");

            Assert.That(tlc, Does.Match(@"(?m)^    timeout-minutes:\s*\$\{\{\s*matrix\.leg\.timeout\s*\}\}\s*$"),
                "a TLC-only item has no inactivity collector (#5012), so the job timeout is its only "
                + "hang bound and must come from the planner's per-leg estimate, not a constant a "
                + "rebalance silently outgrows");

            Assert.That(tlc, Does.Contain("python3 .github/workflows/run-test-leg.py"),
                "the TLC legs must run through run-test-leg.py, which bounds declared TLC items by "
                + "the job timeout exactly as ci.yml's legs are bounded");

            Assert.That(tlc, Does.Not.Contain("XPlat Code Coverage").And.Not.Contain("--collect"),
                "the TLC legs run uninstrumented: coverlet buys no coverage of the harness worth "
                + "having and slows the JVM-bound cases");
        });
    }

    /// <summary>
    /// Nothing in the lane may restore a TLC verdict receipt: the nightly run is the backstop
    /// for drift no fingerprint can see, so every case must run every night.
    /// </summary>
    [Test]
    public void The_nightly_tlc_legs_never_read_a_verdict_receipt()
    {
        var yaml = Read();

        Assert.Multiple(() =>
        {
            Assert.That(yaml, Does.Not.Contain("tlc-verdict"),
                "coverage.yml must never name a TLC verdict receipt cache key (contract C5). A "
                + "receipt would let the nightly run skip a module, which turns the drift backstop "
                + "for the JDK patch level and the runner image into a check of nothing.");

            Assert.That(Job(yaml, "tlc"), Does.Not.Contain("actions/cache"),
                "the TLC legs must not restore any cache; see the receipt rule above");

            Assert.That(yaml, Does.Not.Contain("tlc-receipts.py candidates")
                    .And.Not.Contain("tlc-receipts.py plan")
                    .And.Not.Contain("operation: lookup"),
                "nothing in coverage.yml may list, restore or plan from receipts: the lane writes "
                + "them (#5165) and never reads one");
        });
    }

    /// <summary>
    /// The lane writes a receipt for every module whose every census case passed tonight,
    /// from this attempt's evidence alone, in a job of its own (#5165). On main that is the
    /// scope every pull request reads, so a module the night verified is not rerun.
    /// </summary>
    [Test]
    public void The_nightly_lane_saves_a_receipt_for_every_module_it_verifies()
    {
        var receipts = Job(Read(), "tlc-receipts");

        Assert.Multiple(() =>
        {
            Assert.That(receipts, Does.Match(@"(?m)^    needs:\s*\[\s*measured,\s*plan,\s*tlc\s*\]\s*$"),
                "receipts must be written only after the TLC legs");

            Assert.That(receipts, Does.Contain("!cancelled()").And.Contain("needs.measured.outputs.measure != 'false'"),
                "the receipt job must run after a partly red night, so the modules that passed keep "
                + "their receipts, and skip with the legs on a quiet night");

            Assert.That(receipts, Does.Contain("pattern: tlc-results-${{ github.run_attempt }}-*"),
                "a receipt must come from one attempt's evidence, never a mix of attempts");

            Assert.That(receipts, Does.Contain("tlc-receipts.py assemble").And.Not.Contain("--receipt-plan"),
                "the night runs every module, so assemble must count every module as run rather than "
                + "read a plan of cached ones");

            Assert.That(receipts, Does.Contain("uses: ./.github/actions/tlc-receipts")
                    .And.Contain("operation: save"),
                "the receipts must be saved through the shared C5 action");

            Assert.That(receipts, Does.Not.Contain("tlc-tracking").And.Not.Match(@"(?m)^\s+issues:"),
                "a receipt-writing fault must not read as a TLC failure in the tracking issue");
        });
    }

    /// <summary>
    /// Only the tracking job may write issues, it runs only for a run that tracks, and the
    /// workflow-level token stays read-only.
    /// </summary>
    [Test]
    public void Only_the_tlc_tracking_job_may_write_issues()
    {
        var yaml = Read();
        var tracking = Job(yaml, "tlc-tracking");

        Assert.Multiple(() =>
        {
            Assert.That(Regex.Matches(yaml, @"(?m)^\s+issues:\s*write\s*$").Count, Is.EqualTo(1),
                "exactly one job in coverage.yml may hold 'issues: write'");

            Assert.That(tracking, Does.Match(@"(?m)^      issues:\s*write\s*$"),
                "the tlc-tracking job must hold the only 'issues: write' grant");

            Assert.That(TopLevelBlock(yaml, "permissions"), Does.Not.Contain("write"),
                "the workflow-level token must stay read-only; write access is per job");

            Assert.That(tracking, Does.Contain("needs.plan.outputs.track == 'true'"),
                "a branch run must not touch main's tracking issue unless it asks to");

            Assert.That(tracking, Does.Contain("Nightly exhaustive TLC failing on main"),
                "the tracking issue's title is its identity; a renamed title opens a duplicate");

            Assert.That(tracking, Does.Contain("gh issue close").And.Contain("gh issue comment")
                .And.Contain("gh issue create"),
                "the tracking job must open the issue on a red night, update it on each later one "
                + "and close it on the first green one");
        });
    }
}
