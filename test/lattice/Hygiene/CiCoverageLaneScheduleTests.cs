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
/// <para>
/// <b>Why a catch-up lane.</b> GitHub's <c>schedule</c> trigger is best-effort, and
/// after the Actions incidents of 2026-08-26 the 03:17 UTC slot was created more than
/// five hours late on each of 25, 26 and 27 September, the last at 09:16 UTC.
/// <c>coverage-catch-up.yml</c> dispatches the lane on the first push to
/// main once the slot is past its grace period with no run, and a nightly run skips
/// when an earlier run already covered the night, so the night is measured once
/// whichever arrives first. The three copies of the slot - the cron and the two
/// <c>slot=</c> constants - must agree, or the catch-up would look for the wrong night.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiCoverageLaneScheduleTests
{
    private const string CoverageWorkflow = ".github/workflows/coverage.yml";

    private const string CatchUpWorkflow = ".github/workflows/coverage-catch-up.yml";

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
        var plan = Job(yaml, "plan");

        Assert.Multiple(() =>
        {
            Assert.That(check, Does.Contain("-f status=success"),
                "the skip must compare against the last SUCCESSFUL run, or a failed run would stop "
                + "the next night from measuring the same commit again");

            Assert.That(check, Does.Contain("[ \"$GITHUB_EVENT_NAME\" = schedule ]"),
                "only a nightly run - the schedule, or a catch-up standing in for it - may skip; a "
                + "manual dispatch is a request to measure");

            Assert.That(check, Does.Contain("[ \"$last\" = \"$GITHUB_SHA\" ]"),
                "the skip must require this run's commit to be exactly the one already measured - "
                + "the only condition under which skipping cannot lose a measurement");

            Assert.That(check, Does.Match(@"(?m)^\s+actions:\s*read\s*$"),
                "the check job must grant 'actions: read' to read the lane's run history; without it "
                + "every lookup fails and every night measures");

            Assert.That(coverage, Does.Match(@"(?m)^    needs:.*\bmeasured\b"),
                $"the coverage job in {CoverageWorkflow} must need the check job, or it starts before "
                + "any verdict exists");

            Assert.That(plan, Does.Contain("needs.measured.outputs.measure != 'false'")
                .And.Contain("!cancelled()"),
                "planning must preserve the nightly skip and fail-open measurement verdict");

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
    /// A nightly run may also skip when an earlier run of the lane already covered the
    /// night, and only then: "earlier" must be by run id, so the first of any set of
    /// nightly runs always measures, and a cancelled run must not count as covering.
    /// </summary>
    [Test]
    public void A_nightly_run_skips_a_night_only_an_earlier_run_already_covered()
    {
        var yaml = Read();
        var check = Job(yaml, "measured");

        Assert.Multiple(() =>
        {
            Assert.That(check, Does.Match(@"(?m)^\s+CATCH_UP:\s*\$\{\{\s*inputs\.catch_up\s*\}\}\s*$"),
                "the check job must read the 'catch_up' dispatch input, or a catch-up dispatch is "
                + "treated as a manual one and measures the night a second time");

            Assert.That(check, Does.Contain("[ \"$CATCH_UP\" = true ]"),
                "a catch-up dispatch must be treated as a nightly run, so a late schedule and the "
                + "catch-up standing in for it measure the night once between them");

            Assert.That(check, Does.Contain("select(.id < ${GITHUB_RUN_ID}"),
                "the night-covered skip must count only runs created before this one. Counting every "
                + "run since the slot would let two nightly runs each see the other and both skip, "
                + "so the night would not be measured at all, with nothing red anywhere.");

            Assert.That(check, Does.Contain(".conclusion != \\\"cancelled\\\""),
                "a cancelled run measured nothing, so it must not count as covering the night");

            Assert.That(check, Does.Contain("-f created=\">=${since}\""),
                "the night-covered skip must look only at runs since tonight's slot opened, or last "
                + "night's run would stop tonight's");
        });
    }

    /// <summary>
    /// The coverage lane must accept the catch-up dispatch input, and it must default to
    /// false so a manual dispatch still always measures.
    /// </summary>
    [Test]
    public void The_coverage_lane_accepts_a_catch_up_dispatch()
    {
        var triggers = TopLevelBlock(Read(), "on");

        Assert.That(triggers, Does.Match(
                @"(?ms)^  workflow_dispatch:\s*\r?\n    inputs:\s*\r?\n      catch_up:.*?^        type:\s*boolean\s*\r?$.*?^        default:\s*false\s*\r?$"),
            $"{CoverageWorkflow} must declare a boolean 'catch_up' dispatch input defaulting to false: "
            + $"{CatchUpWorkflow} sets it, and a manual dispatch that leaves it unset must still measure");
    }

    /// <summary>
    /// The catch-up lane must run on every push to main and nothing else, dispatch the
    /// coverage lane as a catch-up, count any non-cancelled run as covering the night, and
    /// never cancel anything.
    /// </summary>
    [Test]
    public void The_catch_up_lane_dispatches_the_coverage_lane_when_the_schedule_misses()
    {
        var yaml = Read(CatchUpWorkflow);
        var triggers = TopLevelBlock(yaml, "on", CatchUpWorkflow);
        var job = Job(yaml, "catch-up", CatchUpWorkflow);

        Assert.Multiple(() =>
        {
            Assert.That(triggers, Does.Match(@"(?m)^  push:\s*\r?\n    branches:\s*\[\s*main\s*\]\s*\r?$"),
                $"{CatchUpWorkflow} must run on every push to main: a push is the only reliable event "
                + "that follows a missed schedule, and main is the only branch coverage is measured on");

            Assert.That(triggers, Does.Not.Match(@"(?m)^  (schedule|pull_request|pull_request_target):"),
                $"{CatchUpWorkflow} must not run on a schedule, which is the unreliable event it "
                + "exists to cover, nor on pull requests, which never measure coverage");

            Assert.That(job, Does.Contain("gh workflow run coverage.yml")
                    .And.Contain("-f catch_up=true"),
                "the catch-up must dispatch coverage.yml with catch_up=true, so a late schedule that "
                + "arrives after it skips instead of measuring the night again");

            Assert.That(job, Does.Match(@"(?m)^\s+actions:\s*write\s*$"),
                "the catch-up job must grant 'actions: write'; without it the dispatch is refused and "
                + "a missed schedule is never caught up");

            Assert.That(job, Does.Contain("select(.conclusion != \"cancelled\")"),
                "the catch-up must count an in-progress, failed or skipped run as covering the night. "
                + "Counting only successes would re-dispatch an hour-long failing run on every merge.");

            Assert.That(job, Does.Match(@"(?m)^    timeout-minutes:\s*\d+\s*$"),
                "the catch-up job must declare a timeout; it takes seconds and must not hold a runner");

            Assert.That(yaml, Does.Not.Contain("cancel-in-progress"),
                $"{CatchUpWorkflow} must not cancel runs. It runs on every push to main, where a "
                + "cancelled check run rolls up as a failure on the commit.");
        });
    }

    /// <summary>
    /// The cron and the two <c>slot=</c> constants that compute tonight's slot must name the
    /// same time, or the catch-up and the skip would look for the wrong night.
    /// </summary>
    [Test]
    public void The_nightly_slot_agrees_across_the_cron_and_both_lanes()
    {
        var cron = Regex.Match(TopLevelBlock(Read(), "on"), @"(?m)^\s+- cron:\s*'(?<minute>\d+) (?<hour>\d+) \* \* \*'");

        Assert.That(cron.Success, Is.True,
            $"expected a daily '<minute> <hour> * * *' cron in {CoverageWorkflow}; if the schedule "
            + "changed shape, the slot computation in both lanes must change with it");

        var expected = $"slot=\"{int.Parse(cron.Groups["hour"].Value):00}:{int.Parse(cron.Groups["minute"].Value):00}\"";

        Assert.Multiple(() =>
        {
            Assert.That(Job(Read(), "measured"), Does.Contain(expected),
                $"the measured job in {CoverageWorkflow} must compute tonight's slot as {expected}, "
                + "matching the cron");

            Assert.That(Job(Read(CatchUpWorkflow), "catch-up", CatchUpWorkflow), Does.Contain(expected),
                $"{CatchUpWorkflow} must compute tonight's slot as {expected}, matching the cron in "
                + CoverageWorkflow);
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
    private static string TopLevelBlock(string yaml, string key, string file = CoverageWorkflow)
    {
        var start = Regex.Match(yaml, $@"^{Regex.Escape(key)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a top-level '{key}:' block in {file}; if it moved, this fixture "
            + "stopped guarding it and must be updated with it");

        var rest = yaml[(start.Index + start.Length)..];
        var end = Regex.Match(rest, @"^\S", RegexOptions.Multiline);
        var block = end.Success ? rest[..end.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the '{key}:' block in {file} is empty, so nothing asserted about it means anything");

        return block;
    }

    /// <summary>
    /// Reads one job's block by its id, up to the next job at the same indentation or the
    /// end of the file. Fails rather than returning an empty string, for the same reason.
    /// </summary>
    private static string Job(string yaml, string jobId, string file = CoverageWorkflow)
    {
        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a job with id '{jobId}' in {file}. If it was renamed, this fixture "
            + "stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_-]+:[ \t]*\r?$|^\S", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the job '{jobId}' in {file} has an empty body, so nothing asserted about it "
            + "means anything");

        return block;
    }

    private static string Read(string file = CoverageWorkflow) =>
        File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            file.Replace('/', Path.DirectorySeparatorChar)));
}
