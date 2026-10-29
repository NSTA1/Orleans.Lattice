using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The weekly production-perturbation lane's contract (#5163, epic #5148):
/// <c>.github/workflows/formal-perturbations.yml</c> runs on a weekly schedule and by
/// hand only, writes nothing but the tracking issue, runs once per ref without
/// cancelling itself, and says why it needs no TLA+ toolchain.
/// <para>
/// <b>Why a guard.</b> Each perturbation rebuilds a test project, so the lane costs
/// minutes per perturbation. A <c>pull_request</c> or <c>push</c> trigger added to make
/// it "consistent" with the other lanes would multiply that cost by every change and
/// nothing would fail. The tracking issue is the lane's only output that reaches a
/// person, so the permission to write issues is confined to the one job that maintains
/// it; a broader grant would hand every step that runs repository code a token that can
/// write. A cancelled run rolls up as a failure on its commit and says nothing about any
/// perturbation, so nothing in the lane cancels.
/// </para>
/// <para>
/// The lane's bookkeeping lives in <c>.github/workflows/perturbation-lane.py</c>, whose
/// self-test this fixture runs, because no other lane does and an untested summariser
/// can report green for a run that checked nothing.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiPerturbationLaneTests
{
    private const string LaneWorkflow = ".github/workflows/formal-perturbations.yml";

    private const string LaneHelper = ".github/workflows/perturbation-lane.py";

    private const string TrackingJob = "tracking";

    private static readonly TimeSpan ScriptTimeout = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The lane runs on a weekly schedule and by hand, and on nothing that fires per change.
    /// </summary>
    [Test]
    public void The_perturbation_lane_runs_weekly_and_by_hand_only()
    {
        var triggers = TopLevelBlock(Read(LaneWorkflow), "on");

        Assert.Multiple(() =>
        {
            Assert.That(triggers, Does.Match(@"(?m)^  schedule:[ \t]*\r?$"),
                $"{LaneWorkflow} must be triggered by a 'schedule:'; it is the weekly lane");

            var crons = Regex.Matches(triggers, @"(?m)^\s+- cron:\s*'(?<cron>[^']+)'");
            Assert.That(crons, Has.Count.EqualTo(1), "the schedule must declare exactly one cron expression");
            Assert.That(
                crons.Select(match => match.Groups["cron"].Value),
                Has.All.Match(@"^\d{1,2} \d{1,2} \* \* [0-6]$"),
                "the cron must fire once a week: a fixed minute, hour and day of the week. A more frequent "
                    + "schedule multiplies a lane that rebuilds a test project per perturbation.");

            Assert.That(triggers, Does.Match(@"(?m)^  workflow_dispatch:"),
                $"{LaneWorkflow} must keep 'workflow_dispatch:', the only way to validate a change to the "
                    + "lane on a branch or to re-run it after a transient failure");

            Assert.That(
                triggers,
                Does.Not.Match(@"(?m)^  (push|pull_request|pull_request_target|workflow_run|merge_group):"),
                $"{LaneWorkflow} must not run per change. RefinementPerturbationTests is the per-pull-request "
                    + "gate; this lane is the expensive half and runs weekly.");
        });
    }

    /// <summary>
    /// The workflow reads source; only the tracking job writes, and it writes only issues.
    /// </summary>
    [Test]
    public void Only_the_tracking_job_may_write_and_it_may_write_only_issues()
    {
        var yaml = Read(LaneWorkflow);
        var topLevel = Uncommented(TopLevelBlock(yaml, "permissions"));
        var tracking = Job(yaml, TrackingJob);
        var writes = Regex.Matches(yaml, @"(?m)^[ \t]+[a-z-]+:[ \t]*write[ \t]*\r?$");

        Assert.Multiple(() =>
        {
            Assert.That(topLevel, Is.EqualTo("contents: read"),
                $"the workflow-level permissions of {LaneWorkflow} must be exactly 'contents: read'");

            Assert.That(yaml, Does.Not.Contain("write-all"), "no job may hold every write scope");

            Assert.That(tracking, Does.Match(@"(?m)^    permissions:[ \t]*\r?\n      issues: write[ \t]*\r?$"),
                $"the '{TrackingJob}' job must declare 'issues: write' as its only permission");

            Assert.That(writes.Select(match => match.Value.Trim()), Is.EqualTo(new[] { "issues: write" }),
                $"exactly one write grant may appear in {LaneWorkflow}, 'issues: write' on the '{TrackingJob}' "
                    + "job. Every other job runs repository code and must not hold a token that writes.");

            Assert.That(Regex.Matches(yaml, @"(?m)^[ \t]+issues:[ \t]*write").Single().Index,
                Is.GreaterThan(yaml.IndexOf("  " + TrackingJob + ":", StringComparison.Ordinal)),
                $"the issues grant must sit inside the '{TrackingJob}' job");
        });
    }

    /// <summary>
    /// One run per ref at a time, and nothing in the lane cancels a run.
    /// </summary>
    [Test]
    public void One_run_per_ref_and_nothing_cancels_a_run()
    {
        var yaml = Read(LaneWorkflow);
        var concurrency = TopLevelBlock(yaml, "concurrency");

        Assert.Multiple(() =>
        {
            Assert.That(concurrency, Does.Match(@"(?m)^  group: formal-perturbations-\$\{\{ github\.ref \}\}[ \t]*\r?$"),
                $"{LaneWorkflow} must queue runs per ref in the 'formal-perturbations-${{{{ github.ref }}}}' group");

            Assert.That(concurrency, Does.Match(@"(?m)^  cancel-in-progress: false[ \t]*\r?$"),
                "a dispatch arriving during a run must wait for it, not cancel it");

            Assert.That(Regex.IsMatch(yaml, @"^\s*cancel-in-progress:(?!\s*false\s*$)", RegexOptions.Multiline),
                Is.False,
                "nothing in the lane may cancel a run: a cancelled check run rolls up as a failure on its "
                    + "commit and says nothing about any perturbation");
        });
    }

    /// <summary>
    /// The lane never selects the <c>Tlc</c> category and says so with the opt-out marker
    /// <c>CiTlaToolchainProvisioningTests</c> recognises.
    /// </summary>
    [Test]
    public void The_lane_runs_the_perturbation_runner_and_records_why_it_needs_no_tla_toolchain()
    {
        var yaml = Read(LaneWorkflow);

        Assert.Multiple(() =>
        {
            Assert.That(yaml, Does.Match(@"(?m)#\s*tla-toolchain:\s*not-required\s*-\s*\S.*$"),
                $"{LaneWorkflow} must carry '# tla-toolchain: not-required - <reason>'. The runner filters "
                    + "to the named detector methods, so the lane cannot select the Tlc category.");

            Assert.That(yaml, Does.Contain("python3 tools/perturbations/run-perturbations.py"),
                "the lane must run the perturbation runner; a lane that runs something else checks nothing");

            Assert.That(yaml, Does.Not.Contain("TestCategory").And.Not.Contain("tla2tools").And.Not.Contain("setup-java"),
                "the lane must neither select test categories nor provision TLC, or the marker is false");

            Assert.That(yaml, Does.Contain("perturbation-shard-${{ github.run_attempt }}-${{ matrix.shard }}")
                    .And.Contain("pattern: perturbation-shard-${{ github.run_attempt }}-*"),
                "shard artifacts must be scoped to the run attempt, or a re-run would summarise the "
                    + "previous attempt's results");
        });
    }

    /// <summary>
    /// The lane and its helper are TLC-neutral, so changing them never costs exhaustive TLC.
    /// </summary>
    [Test]
    public void The_lane_is_classified_tlc_neutral()
    {
        using var inputs = JsonDocument.Parse(Read(".github/workflows/tlc-inputs.json"));
        var neutral = inputs.RootElement.GetProperty("neutral").EnumerateArray().Select(item => item.GetString()).ToArray();

        Assert.That(neutral, Is.SupersetOf(new[] { LaneWorkflow, LaneHelper }),
            "the lane selects and runs no TLC case, so it is listed as neutral in tlc-inputs.json");
    }

    /// <summary>
    /// The helper's own self-test covers selection, sharding, the tracking decision and every
    /// red verdict the summariser must report.
    /// </summary>
    [Test]
    public void The_lane_helper_self_test_passes()
    {
        var (exitCode, stdout, stderr) = RunPython([Path.Combine(RepoRoot, LaneHelper), "--self-test"]);

        Assert.That(exitCode, Is.Zero, "perturbation-lane.py --self-test failed:\n" + stdout + stderr);
        Assert.That(stdout, Does.Match(@"self-test: passed \([1-9]\d* checks, 0 failure"),
            "the self-test must run checks, not pass vacuously");
    }

    private static string RepoRoot => HygieneRepository.FindRepoRoot();

    private static string Read(string file) =>
        File.ReadAllText(Path.Combine(RepoRoot, file.Replace('/', Path.DirectorySeparatorChar)));

    private static string Uncommented(string block) =>
        string.Join('\n', block.Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith('#')));

    private static string TopLevelBlock(string yaml, string key)
    {
        var start = Regex.Match(yaml, $@"^{Regex.Escape(key)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a top-level '{key}:' block in {LaneWorkflow}; if it moved, this fixture "
            + "stopped guarding it and must be updated with it");

        var rest = yaml[(start.Index + start.Length)..];
        var end = Regex.Match(rest, @"^\S", RegexOptions.Multiline);
        var block = end.Success ? rest[..end.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty, $"the '{key}:' block in {LaneWorkflow} is empty");

        return block;
    }

    private static string Job(string yaml, string jobId)
    {
        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a job with id '{jobId}' in {LaneWorkflow}. If it was renamed, this fixture "
            + "stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_-]+:[ \t]*\r?$|^\S", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty, $"the job '{jobId}' in {LaneWorkflow} has an empty body");

        return block;
    }

    private static (int ExitCode, string StandardOutput, string StandardError) RunPython(IEnumerable<string> arguments)
    {
        var interpreter = FindPython();

        if (interpreter is null)
        {
            if (string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Fail("no Python interpreter was found on the CI runner, so the lane helper cannot be checked");
            }

            Assert.Ignore("no Python interpreter is available on this host");
        }

        var start = new ProcessStartInfo(interpreter!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = RepoRoot,
        };

        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        return Run(start);
    }

    private static string? FindPython()
    {
        // `python3` on Windows is often a Store alias stub that prints an error,
        // so each candidate is probed rather than assumed.
        string[] candidates = OperatingSystem.IsWindows() ? ["python", "py", "python3"] : ["python3", "python"];

        foreach (var candidate in candidates)
        {
            try
            {
                var probe = new ProcessStartInfo(candidate)
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
                probe.ArgumentList.Add("--version");

                var (exitCode, stdout, stderr) = Run(probe);

                if (exitCode == 0 && (stdout + stderr).Contains("Python 3", StringComparison.Ordinal))
                {
                    return candidate;
                }
            }
            catch (System.ComponentModel.Win32Exception)
            {
                // Not on PATH.
            }
        }

        return null;
    }

    private static (int ExitCode, string StandardOutput, string StandardError) Run(ProcessStartInfo start)
    {
        using var process = Process.Start(start)!;
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit((int)ScriptTimeout.TotalMilliseconds))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
                // Already exited.
            }

            Assert.Fail(
                $"'{start.FileName} {string.Join(' ', start.ArgumentList)}' did not exit within "
                + $"{ScriptTimeout.TotalSeconds:0}s.");
        }

        return (process.ExitCode, stdoutTask.GetAwaiter().GetResult(), stderrTask.GetAwaiter().GetResult());
    }
}
