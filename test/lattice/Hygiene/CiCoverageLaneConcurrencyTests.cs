using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The coverage lane's concurrency contract: runs on main queue behind one another
/// and are never cancelled, a queued run that has been overtaken defers to the newer
/// run instead of measuring a superseded commit, and no single run can hold the queue
/// indefinitely.
/// <para>
/// <b>What went wrong.</b> <c>coverage.yml</c> set <c>cancel-in-progress: true</c>,
/// the setting every other lane here uses and the right one for a lane that gates a
/// pull request. This lane gates nothing, takes about an hour, and main routinely
/// takes merges faster than that, so most of its runs were cancelled part way: of the
/// 35 runs from #3327 to #3473, 22 were cancelled, spending 615 runner-minutes
/// measuring nothing against 708 for the 11 that completed. GitHub rolls a cancelled
/// check run up as a failure on its commit, so main showed a red cross on most of its
/// commits although nothing had failed, and the two runs that genuinely failed were
/// buried among them.
/// </para>
/// <para>
/// <b>Why this is a guard and not just a fix.</b> <c>cancel-in-progress: true</c> is
/// the idiom of this repository's workflows, so it is exactly what a well-meaning edit
/// would restore to make this lane "consistent" - and nothing would fail when it did,
/// because a cancelled run is not a failed test. The queue alone is not enough either:
/// without the supersession gate it measures every commit serially and falls hours
/// behind main during a burst of merges, and without a timeout one hung run holds every
/// queued run for the six-hour default. Each of those is pinned here.
/// </para>
/// <para>
/// The gate's predicate itself is exercised by
/// <c>.github/workflows/coverage-supersession-selftest.py</c>, which this fixture
/// requires CI to run: the gate executes only on main after a merge, so no pull
/// request would otherwise ever drive it.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiCoverageLaneConcurrencyTests
{
    private const string CoverageWorkflow = ".github/workflows/coverage.yml";
    private const string CiWorkflow = ".github/workflows/ci.yml";
    private const string GateScript = ".github/workflows/coverage-supersession.py";
    private const string GateSelfTest = ".github/workflows/coverage-supersession-selftest.py";

    /// <summary>
    /// The lane's concurrency group must queue runs, and nothing in the workflow may
    /// cancel an in-progress run - neither the workflow-level group nor a job-level one.
    /// </summary>
    [Test]
    public void Coverage_runs_queue_behind_each_other_instead_of_cancelling()
    {
        var yaml = Read(CoverageWorkflow);

        var start = Regex.Match(yaml, @"^concurrency:[ \t]*\r?$", RegexOptions.Multiline);
        Assert.That(start.Success, Is.True,
            $"expected a top-level 'concurrency:' block in {CoverageWorkflow}. Without one, "
            + "runs neither queue nor cancel: every merge starts its own hour-long run in "
            + "parallel with the others.");

        var rest = yaml[(start.Index + start.Length)..];
        var end = Regex.Match(rest, @"^\S", RegexOptions.Multiline);
        var block = end.Success ? rest[..end.Index] : rest;

        Assert.That(block, Does.Match(@"(?m)^\s+group:\s*\S"),
            $"the concurrency block in {CoverageWorkflow} names no group, so this fixture "
            + "found the block but not its contents and the assertions below would be vacuous");

        Assert.That(block, Does.Match(@"(?m)^\s+queue:\s*max\s*$"),
            $"the concurrency block in {CoverageWorkflow} must set 'queue: max'. The default "
            + "queue holds ONE pending run and cancels it when the next arrives, so every "
            + "merge in a burst but the last would still roll up as a failure on main.");

        Assert.That(
            Regex.IsMatch(yaml, @"^\s*cancel-in-progress:(?!\s*false\s*$)", RegexOptions.Multiline),
            Is.False,
            $"{CoverageWorkflow} must not cancel in-progress runs anywhere. A run takes about "
                + "an hour and main takes merges faster than that, so cancellation left most "
                + "runs unfinished and most commits on main showing a red cross although "
                + "nothing had failed.");
    }

    /// <summary>
    /// The measurement job must be gated on the supersession verdict, and gated so that
    /// only an explicit decision to skip can skip it.
    /// </summary>
    [Test]
    public void A_queued_run_defers_to_a_newer_one_through_the_supersession_gate()
    {
        var yaml = Read(CoverageWorkflow);
        var gate = Job(yaml, "supersession");
        var coverage = Job(yaml, "coverage");

        Assert.Multiple(() =>
        {
            Assert.That(gate, Does.Contain("coverage-supersession.py --run-id"),
                $"the supersession job in {CoverageWorkflow} no longer runs {GateScript}, "
                + "so nothing decides whether a queued run has been overtaken");

            Assert.That(gate, Does.Contain("GITHUB_RUN_ID"),
                "the gate identifies this run in the listing by its id; without it the "
                + "listing cannot be checked to be about this run at all");

            Assert.That(gate, Does.Match(@"(?m)^\s+actions:\s*read\s*$"),
                "the supersession job must grant 'actions: read': listing this workflow's "
                + "runs is the whole of what the gate reads, and without the scope every "
                + "call fails and the gate measures every run");

            Assert.That(gate, Does.Match(@"(?m)^\s+measure:\s*\$\{\{\s*steps\.\S+\.outputs\.measure\s*\}\}"),
                "the supersession job must publish its verdict as the 'measure' output");

            Assert.That(coverage, Does.Match(@"(?m)^    needs:.*\bsupersession\b"),
                $"the coverage job in {CoverageWorkflow} must need the supersession job, or "
                + "it starts before any verdict exists");

            var condition = Regex.Match(coverage, @"^    if:\s*(?<expr>.+?)\s*$", RegexOptions.Multiline);
            Assert.That(condition.Success, Is.True,
                "the coverage job has no job-level 'if:', so it measures every queued run "
                + "serially and the queue falls hours behind main during a burst of merges");

            Assert.That(condition.Groups["expr"].Value,
                Does.Contain("needs.supersession.outputs.measure != 'false'"),
                "the coverage job must skip only on an explicit 'false'. Comparing against "
                + "'true' instead would turn a missing verdict into a skip, and a skipped job "
                + "reports as skipped, not failed - the coverage report would stop moving "
                + "with nothing red anywhere.");

            Assert.That(condition.Groups["expr"].Value, Does.Contain("!cancelled()"),
                "the coverage job's condition must carry '!cancelled()', so a failed gate job "
                + "cannot silently skip the measurement: without a status function the job is "
                + "skipped whenever a job it needs fails");
        });
    }

    /// <summary>
    /// A queue that never cancels needs a bound on each run, or one hung run holds every
    /// run behind it until GitHub's six-hour default expires.
    /// </summary>
    [Test]
    public void The_measurement_job_is_bounded_so_a_hung_run_cannot_hold_the_queue()
    {
        var coverage = Job(Read(CoverageWorkflow), "coverage");

        var timeout = Regex.Match(coverage, @"^    timeout-minutes:\s*(?<minutes>\d+)\s*$", RegexOptions.Multiline);

        Assert.That(timeout.Success, Is.True,
            $"the coverage job in {CoverageWorkflow} must declare a job-level 'timeout-minutes'. "
            + "Runs no longer cancel one another, so a hung run would otherwise hold every "
            + "queued run for the six-hour default.");

        Assert.That(int.Parse(timeout.Groups["minutes"].Value), Is.InRange(1, 359),
            "the coverage job's timeout must be shorter than GitHub's 360-minute default, "
            + "which is the bound it exists to undercut");
    }

    /// <summary>
    /// The gate must trigger the lane when it changes, and its self-test must run in CI.
    /// </summary>
    [Test]
    public void The_gate_triggers_the_lane_and_its_self_test_runs_in_ci()
    {
        var root = HygieneRepository.FindRepoRoot();
        var coverage = Read(CoverageWorkflow);
        var ci = Read(CiWorkflow);

        Assert.Multiple(() =>
        {
            Assert.That(File.Exists(Path.Combine(root, Native(GateScript))), Is.True,
                $"{GateScript} is missing, so the supersession job has nothing to run");

            Assert.That(File.Exists(Path.Combine(root, Native(GateSelfTest))), Is.True,
                $"{GateSelfTest} is missing, so nothing exercises the gate's predicate");

            Assert.That(coverage, Does.Contain($"- '{GateScript}'"),
                $"{GateScript} must be in the push paths of {CoverageWorkflow}, so a change to "
                + "the gate runs the lane on main and is validated by running it");

            Assert.That(ci, Does.Contain($"python3 {GateSelfTest}"),
                $"{CiWorkflow} must run {GateSelfTest}. The gate runs only on main after a "
                + "merge, so no pull request would otherwise execute it, and a gate that "
                + "skipped every run would look exactly like one working correctly.");
        });
    }

    /// <summary>
    /// Reads one job's block by its id, up to the next job at the same indentation or
    /// the end of the file. Fails rather than returning an empty string, because every
    /// assertion above would pass against one.
    /// </summary>
    private static string Job(string yaml, string jobId)
    {
        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a job with id '{jobId}' in {CoverageWorkflow}. If it was renamed, this "
            + "fixture stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_-]+:[ \t]*\r?$|^\S", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the job '{jobId}' in {CoverageWorkflow} has an empty body, so nothing asserted "
            + "about it means anything");

        return block;
    }

    private static string Read(string relativePath) =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), Native(relativePath)));

    private static string Native(string relativePath) =>
        relativePath.Replace('/', Path.DirectorySeparatorChar);
}
