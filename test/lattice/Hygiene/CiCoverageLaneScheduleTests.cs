using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The coverage lane's trigger contract: it measures main once a night rather than on
/// every push, nothing in it cancels a run, a scheduled run skips only a commit the
/// last successful run already measured, and no single run can hold the lane for
/// GitHub's six-hour default.
/// <para>
/// <b>What went wrong.</b> <c>coverage.yml</c> ran on every push to main with
/// <c>cancel-in-progress: true</c> - the idiom of every other lane here, and the right
/// one for a lane that gates a pull request. This lane gates nothing and takes about an
/// hour, and main takes merges faster than that, so most of its runs were cancelled part
/// way: of the 35 runs from #3327 to #3473, 22 were cancelled. GitHub rolls a cancelled
/// check run up as a failure on its commit, so main showed a red cross on most of its
/// commits although nothing had failed, and the two runs that genuinely failed were
/// buried among them. Replaying the 207 pushes from 4 to 24 September put every
/// per-push design, queued or cancelled, at 8,900-10,000 runner-minutes against about
/// 870 for one run a night.
/// </para>
/// <para>
/// <b>Why this is a guard and not just a fix.</b> A push trigger with
/// <c>cancel-in-progress: true</c> is exactly what a well-meaning edit would restore to
/// make this lane "consistent" with the others, and nothing would fail when it did,
/// because a cancelled run is not a failed test - the red would simply come back. The
/// nightly skip is the other half: it is correct only while it can skip nothing but a
/// commit already measured, since a skipped job reports as skipped rather than failed
/// and a skip that fired every night would freeze the coverage report with nothing red
/// anywhere.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiCoverageLaneScheduleTests
{
    private const string CoverageWorkflow = ".github/workflows/coverage.yml";

    /// <summary>
    /// The lane must be triggered by a schedule and by hand, and by nothing that fires
    /// on every change.
    /// </summary>
    [Test]
    public void The_coverage_lane_runs_nightly_not_on_every_push()
    {
        var triggers = TopLevelBlock(Read(), "on");

        Assert.Multiple(() =>
        {
            Assert.That(triggers, Does.Match(@"(?m)^  schedule:[ \t]*\r?$"),
                $"{CoverageWorkflow} must be triggered by a 'schedule:'. It is the only trigger "
                + "under which an hour-long lane neither cancels itself red on a busy main nor "
                + "competes with pull-request CI for runners through the working day.");

            Assert.That(triggers, Does.Match(@"(?m)^\s+- cron:\s*'[^']+'"),
                "the schedule declares no cron expression, so this fixture found the trigger but "
                + "not its contents and the lane would never run");

            Assert.That(triggers, Does.Match(@"(?m)^  workflow_dispatch:"),
                $"{CoverageWorkflow} must keep 'workflow_dispatch:', the only way to re-run the lane "
                + "after a transient failure or to validate a change to it before it merges");

            Assert.That(triggers, Does.Not.Match(@"(?m)^  (push|pull_request|pull_request_target):"),
                $"{CoverageWorkflow} must not run on every change. On every push to main it could "
                + "not keep up: 22 of 35 runs were cancelled part way and rolled up as failures on "
                + "main, and even queued instead of cancelled it cost more than ten times the runner "
                + "time of one run a night. build-and-test in ci.yml is the per-change gate.");
        });
    }

    /// <summary>
    /// Nothing in the lane may cancel an in-progress run, at the workflow level or at a
    /// job level, because a cancelled check run rolls up as a failure on its commit.
    /// </summary>
    [Test]
    public void Nothing_in_the_coverage_lane_cancels_a_run()
    {
        var yaml = Read();

        Assert.That(TopLevelBlock(yaml, "concurrency"), Does.Match(@"(?m)^\s+group:\s*\S"),
            $"expected a concurrency group in {CoverageWorkflow}, so a dispatch that arrives during "
            + "the nightly run waits for it instead of measuring alongside it");

        Assert.That(
            Regex.IsMatch(yaml, @"^\s*cancel-in-progress:(?!\s*false\s*$)", RegexOptions.Multiline),
            Is.False,
            $"{CoverageWorkflow} must not cancel in-progress runs anywhere. GitHub rolls a cancelled "
                + "check run up as a failure on its commit, which is how main came to show a red "
                + "cross on most of its commits although nothing had failed.");
    }

    /// <summary>
    /// A scheduled run may skip the measurement only when its commit is the one the last
    /// successful run measured, and the measurement job may skip only on that explicit
    /// verdict.
    /// </summary>
    [Test]
    public void A_scheduled_run_skips_only_a_commit_already_measured()
    {
        var yaml = Read();
        var check = Job(yaml, "measured");
        var coverage = Job(yaml, "coverage");

        Assert.Multiple(() =>
        {
            Assert.That(check, Does.Contain("-f status=success"),
                "the skip must compare against the last SUCCESSFUL run, or a failed run would stop "
                + "the next night from measuring the same commit again");

            Assert.That(check, Does.Contain("[ \"$GITHUB_EVENT_NAME\" = schedule ]"),
                "only a scheduled run may skip; a dispatched run is a request to measure");

            Assert.That(check, Does.Contain("[ \"$last\" = \"$GITHUB_SHA\" ]"),
                "the skip must require this run's commit to be exactly the one already measured - "
                + "the only condition under which skipping cannot lose a measurement");

            Assert.That(check, Does.Match(@"(?m)^\s+actions:\s*read\s*$"),
                "the check job must grant 'actions: read' to read the lane's run history; without it "
                + "every lookup fails and every night measures");

            Assert.That(coverage, Does.Match(@"(?m)^    needs:.*\bmeasured\b"),
                $"the coverage job in {CoverageWorkflow} must need the check job, or it starts before "
                + "any verdict exists");

            var condition = Regex.Match(coverage, @"^    if:\s*(?<expr>.+?)\s*$", RegexOptions.Multiline);
            Assert.That(condition.Success, Is.True,
                "the coverage job has no job-level 'if:', so the check decides nothing");

            Assert.That(condition.Groups["expr"].Value, Does.Contain("needs.measured.outputs.measure != 'false'"),
                "the coverage job must skip only on an explicit 'false'. Comparing against 'true' "
                + "would turn a missing verdict into a skip, and a skipped job reports as skipped, not "
                + "failed - the coverage report would stop moving with nothing red anywhere.");

            Assert.That(condition.Groups["expr"].Value, Does.Contain("!cancelled()"),
                "the coverage job's condition must carry '!cancelled()', so a failed check job cannot "
                + "skip the measurement: without a status function the job is skipped whenever a job "
                + "it needs fails");
        });
    }

    /// <summary>
    /// One hung run must not be able to hold the lane, and any dispatch waiting behind
    /// it, for GitHub's six-hour default.
    /// </summary>
    [Test]
    public void The_measurement_job_is_bounded_by_a_timeout()
    {
        var coverage = Job(Read(), "coverage");

        var timeout = Regex.Match(coverage, @"^    timeout-minutes:\s*(?<minutes>\d+)\s*$", RegexOptions.Multiline);

        Assert.That(timeout.Success, Is.True,
            $"the coverage job in {CoverageWorkflow} must declare a job-level 'timeout-minutes', or a "
            + "hung run holds the lane for the six-hour default");

        Assert.That(int.Parse(timeout.Groups["minutes"].Value), Is.InRange(1, 359),
            "the coverage job's timeout must be shorter than GitHub's 360-minute default, which is "
            + "the bound it exists to undercut");
    }

    /// <summary>
    /// Reads a top-level block (<c>on:</c>, <c>concurrency:</c>) up to the next line that
    /// starts in the first column. Fails rather than returning an empty string, because
    /// every assertion above would pass against one.
    /// </summary>
    private static string TopLevelBlock(string yaml, string key)
    {
        var start = Regex.Match(yaml, $@"^{Regex.Escape(key)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a top-level '{key}:' block in {CoverageWorkflow}; if it moved, this fixture "
            + "stopped guarding it and must be updated with it");

        var rest = yaml[(start.Index + start.Length)..];
        var end = Regex.Match(rest, @"^\S", RegexOptions.Multiline);
        var block = end.Success ? rest[..end.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the '{key}:' block in {CoverageWorkflow} is empty, so nothing asserted about it means anything");

        return block;
    }

    /// <summary>
    /// Reads one job's block by its id, up to the next job at the same indentation or the
    /// end of the file. Fails rather than returning an empty string, for the same reason.
    /// </summary>
    private static string Job(string yaml, string jobId)
    {
        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a job with id '{jobId}' in {CoverageWorkflow}. If it was renamed, this fixture "
            + "stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_-]+:[ \t]*\r?$|^\S", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the job '{jobId}' in {CoverageWorkflow} has an empty body, so nothing asserted about it "
            + "means anything");

        return block;
    }

    private static string Read() =>
        File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            CoverageWorkflow.Replace('/', Path.DirectorySeparatorChar)));
}
