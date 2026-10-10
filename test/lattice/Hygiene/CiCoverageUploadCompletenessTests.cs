using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The coverage lane must never upload a partial report to Codecov: a run in which any
/// suite's test host was aborted, or any suite produced no report, withholds the upload.
/// <para>
/// <b>What went wrong.</b> On 29 September a hang in <c>Orleans.Lattice.Tests</c> tripped
/// <c>--blame-hang</c>, which killed the test host. Coverlet still wrote a
/// <c>coverage.cobertura.xml</c> for the suite, but with the hit counts the dead process
/// never flushed, so the core library read as almost entirely uncovered. The upload step
/// ran on <c>!cancelled()</c> alone, uploaded it, and main's coverage - and the README
/// badge - fell from 93.5% to 78.7% with no code change. The next nightly run then
/// skipped, because a failed earlier run counts as covering the night, so the wrong
/// figure stood for a day.
/// </para>
/// <para>
/// <b>Why a file-exists check is not enough.</b> The aborted suite <i>did</i> produce a
/// report. The guard must detect the aborted host itself, and it must fail closed: the
/// upload runs only on an explicit <c>complete == 'true'</c>, so a test step that dies
/// before deciding uploads nothing. A suite that failed a named test still ran to
/// completion and its report is whole, so that case still uploads - the reason the
/// upload ran on failure in the first place.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiCoverageUploadCompletenessTests
{
    private const string CoverageWorkflow = ".github/workflows/coverage.yml";

    /// <summary>
    /// The upload step must require the test step's explicit completeness verdict, and
    /// still tolerate a failed test step.
    /// </summary>
    [Test]
    public void The_upload_runs_only_on_an_explicit_complete_verdict()
    {
        var upload = Step("Upload coverage to Codecov");
        var condition = Regex.Match(upload, @"^\s+if:\s*(?<expr>.+?)\s*$", RegexOptions.Multiline);

        Assert.That(condition.Success, Is.True,
            "the Codecov upload step has no 'if:', so it either never runs after a failure or "
            + "runs regardless of whether the report is complete");

        Assert.Multiple(() =>
        {
            Assert.That(condition.Groups["expr"].Value, Does.Contain("steps.test.outputs.complete == 'true'"),
                "the upload must require the test step's 'complete' output to be exactly 'true'. "
                + "Anything weaker - '!= 'false'', or no check at all - uploads when the test step "
                + "died before deciding, and a truncated report replaces main's coverage with a "
                + "figure that is wrong, as it did on 29 September (93.5% to 78.7%).");

            Assert.That(condition.Groups["expr"].Value, Does.Contain("!cancelled()"),
                "the upload must carry '!cancelled()': without a status function it is skipped "
                + "whenever the test step fails, so one named test failure would discard a "
                + "complete report and freeze the badge");
        });
    }

    /// <summary>
    /// Every matrix leg must publish its complete report before the aggregate upload.
    /// </summary>
    [Test]
    public void The_upload_requires_a_complete_artifact_from_every_planned_leg()
    {
        var verify = Step("Verify all coverage legs are complete");
        var artifact = Step("Upload coverage leg");
        var marker = Step("Save complete coverage leg");

        Assert.Multiple(() =>
        {
            Assert.That(artifact, Does.Contain("steps.test.outputs.complete == 'true'")
                .And.Contain("actions/upload-artifact@")
                .And.Contain("github.run_attempt")
                .And.Contain("matrix.leg.id"));
            Assert.That(marker, Does.Contain("steps.test.outputs.complete == 'true'"));
            Assert.That(verify, Does.Contain("needs.plan.outputs.legs")
                .And.Contain(".[].id")
                .And.Contain("complete/${leg}")
                .And.Contain("exit 1")
                .And.Contain("complete=true"));
        });
    }

    /// <summary>
    /// The test step must be addressable as <c>test</c>, detect an aborted test host from
    /// the vstest output, and write its verdict before it can exit on a failed suite.
    /// </summary>
    [Test]
    public void The_test_step_detects_an_aborted_host_and_decides_before_exiting()
    {
        var step = Step("Test with coverage");

        Assert.Multiple(() =>
        {
            Assert.That(step, Does.Match(@"(?m)^\s+id:\s*test\s*$"),
                "the test step must carry 'id: test', or 'steps.test.outputs.complete' is always "
                + "empty and the upload never runs");

            Assert.That(step, Does.Contain("| tee \"$out\"").And.Contain("rc=${PIPESTATUS[0]}"),
                "each suite's vstest output must be captured to a log so an aborted host can be "
                + "detected, and the suite's own exit code - not tee's - must be returned");

            Assert.That(step, Does.Contain("grep -qiE 'test run (was )?aborted|test host process crashed'"),
                "the test step must detect an aborted test host from the vstest output. The "
                + "aborted suite still writes a coverage.cobertura.xml, so a file-exists check "
                + "alone passes the truncated report straight through.");

            Assert.That(step, Does.Contain("incomplete+=("),
                "an aborted or report-less suite must be recorded as incomplete");
        });

        var verdictTrue = step.IndexOf("echo \"complete=true\" >> \"$GITHUB_OUTPUT\"", StringComparison.Ordinal);
        var verdictFalse = step.IndexOf("echo \"complete=false\" >> \"$GITHUB_OUTPUT\"", StringComparison.Ordinal);
        var failedExit = step.IndexOf("if [ ${#failed[@]} -gt 0 ]; then", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(verdictTrue, Is.GreaterThanOrEqualTo(0),
                "the test step never writes 'complete=true', so no run could ever upload");
            Assert.That(verdictFalse, Is.GreaterThanOrEqualTo(0),
                "the test step never writes 'complete=false' for an incomplete run");
            Assert.That(failedExit, Is.GreaterThanOrEqualTo(0),
                "expected the failed-suites block that ends the step; if it moved, this fixture "
                + "stopped guarding the ordering and must be updated with it");
            Assert.That(verdictTrue, Is.LessThan(failedExit),
                "the completeness verdict must be written before the step exits on a failed suite, "
                + "or a complete report with one named failure would never upload");
            Assert.That(verdictFalse, Is.LessThan(failedExit),
                "the completeness verdict must be written before the step exits on a failed suite");
        });
    }

    /// <summary>
    /// The abort-detection pattern must match every line vstest prints for a killed host,
    /// and must not match a normal run's summary.
    /// </summary>
    [TestCase("  The active test run was aborted. Reason: Test host process crashed", true)]
    [TestCase("Test Run Aborted.", true)]
    [TestCase("The Test Run was aborted because the host process exited unexpectedly.", true)]
    [TestCase("Passed!  - Failed:     0, Passed:   748, Skipped:     0, Total:   748", false)]
    [TestCase("Failed!  - Failed:     2, Passed:   746, Skipped:     0, Total:   748", false)]
    public void The_abort_pattern_matches_vstest_abort_output(string line, bool aborted)
    {
        var pattern = Regex.Match(Step("Test with coverage"), @"grep -qiE '(?<pattern>[^']+)'");

        Assert.That(pattern.Success, Is.True, "expected the abort-detection grep in the test step");
        Assert.That(Regex.IsMatch(line, pattern.Groups["pattern"].Value, RegexOptions.IgnoreCase), Is.EqualTo(aborted),
            aborted
                ? $"the abort pattern must match '{line}', which vstest prints for a killed test host"
                : $"the abort pattern must not match '{line}', a normal run's summary");
    }

    /// <summary>
    /// Reads one step's block by its name, up to the next step or the end of the file.
    /// Fails rather than returning an empty string, because every assertion above would
    /// pass or fail meaninglessly against one.
    /// </summary>
    private static string Step(string name)
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            CoverageWorkflow.Replace('/', Path.DirectorySeparatorChar)));

        var start = Regex.Match(yaml, $@"^      - name:\s*{Regex.Escape(name)}\s*\r?$", RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a step named '{name}' in {CoverageWorkflow}. If it was renamed, this fixture "
            + "stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^      - |^  [A-Za-z0-9_-]+:[ \t]*\r?$|^\S", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the step '{name}' in {CoverageWorkflow} has an empty body");

        return block;
    }
}
