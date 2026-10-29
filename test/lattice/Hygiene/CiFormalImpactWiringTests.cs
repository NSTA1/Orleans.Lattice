using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The formal-impact wiring gate (#5148 items 7 and 14, #5155 and #5162): the
/// <c>content-gates</c> job must write the refinement index, publish it, and
/// feed it to <c>formal-impact.py</c>, and the script's self-test must run.
/// <para>
/// Every link in that chain fails quietly. The index is written only when
/// <c>LATTICE_REFINEMENT_INDEX</c> names a file, so a renamed or dropped
/// variable leaves <c>RefinementIndexTests</c> green and writes nothing. A
/// report step pointed at a different path, or one that never runs, prints no
/// summary - which is also what a pull request touching no modelled code
/// prints. And a self-test nothing invokes reports the same silence as a
/// passing one. This fixture pins each link where the workflow declares it.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiFormalImpactWiringTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";
    private const string ScriptPath = ".github/workflows/formal-impact.py";
    private const string ContentGateJob = "content-gates";
    private const string LocateStep = "Locate the refinement index";
    private const string GatesStep = "Run content gates";
    private const string UploadStep = "Upload refinement index";
    private const string ReportStep = "Formal impact report";
    private const string SelfTestStep = "Formal impact self-test";

    [Test]
    public void The_content_gate_names_an_absolute_refinement_index_before_the_gates_run()
    {
        var job = JobBlock(ContentGateJob);
        var locate = Step(job, LocateStep);

        Assert.That(
            locate,
            Does.Match(@"echo """ + RefinementIndex.EnvironmentVariable + @"=\$\{RUNNER_TEMP\}/[^""\s]+\.json"" >> ""\$GITHUB_ENV"""),
            $"'{LocateStep}' must export {RefinementIndex.EnvironmentVariable} as an absolute path under "
            + "RUNNER_TEMP through GITHUB_ENV. RefinementIndexTests writes the index only when that exact "
            + "variable is set, and a relative path would resolve against the test host's working "
            + "directory rather than a place the later steps read.");

        Assert.That(
            job.IndexOf("- name: " + LocateStep, StringComparison.Ordinal),
            Is.LessThan(job.IndexOf("- name: " + GatesStep, StringComparison.Ordinal)),
            $"'{LocateStep}' must run before '{GatesStep}': GITHUB_ENV reaches only later steps.");

        Assert.That(
            Step(job, GatesStep),
            Does.Contain("id: gates").And.Contain("$CONTENT_GATE_FILTER"),
            $"'{GatesStep}' must carry 'id: gates', which the upload and report steps read.");
    }

    [Test]
    public void The_content_gate_publishes_the_refinement_index_artifact()
    {
        var upload = Step(JobBlock(ContentGateJob), UploadStep);

        Assert.That(upload, Does.Contain("uses: actions/upload-artifact@"));
        Assert.That(upload, Does.Match(@"(?m)^\s+name: refinement-index\s*$"),
            "the artifact name 'refinement-index' is what consumers download; renaming it orphans them.");
        Assert.That(upload, Does.Contain("path: ${{ env." + RefinementIndex.EnvironmentVariable + " }}"),
            "the upload must read the same variable the exporter writes to.");
        Assert.That(upload, Does.Contain("steps.gates.outcome != 'skipped'").And.Contain("!cancelled()"),
            "the upload must run whenever the gates ran, including when a gate failed, and only then.");
    }

    [Test]
    public void The_formal_impact_report_reads_the_index_the_gates_wrote()
    {
        var report = Step(JobBlock(ContentGateJob), ReportStep);

        Assert.Multiple(() =>
        {
            Assert.That(report, Does.Contain(ScriptPath));
            Assert.That(report, Does.Contain("--index \"$" + RefinementIndex.EnvironmentVariable + "\""),
                "the report must read the index the gates wrote, not a second path.");
            Assert.That(report, Does.Contain("--base \"$BASE_SHA\"").And.Contain("--head \"$HEAD_SHA\""));
            Assert.That(report, Does.Contain("github.event.pull_request.base.sha")
                .And.Contain("github.event.pull_request.head.sha"));
            Assert.That(report, Does.Contain("--summary-file \"$GITHUB_STEP_SUMMARY\""),
                "the report is delivered through the job summary.");
            Assert.That(report, Does.Contain("github.event_name == 'pull_request'")
                .And.Contain("steps.gates.outcome != 'skipped'"),
                "the report must run on every pull request whose gates ran: that is every pull request "
                + "that changes src/, because such a change is never lightweight.");
            Assert.That(report, Does.Not.Contain("continue-on-error"),
                "a report that cannot read its index must fail, not vanish.");
        });
    }

    [Test]
    public void The_formal_impact_self_test_runs_on_every_content_gate_run()
    {
        var root = HygieneRepository.FindRepoRoot();
        Assert.That(File.Exists(Path.Combine(root, ScriptPath.Replace('/', Path.DirectorySeparatorChar))), Is.True,
            $"expected {ScriptPath}");

        var selfTest = Step(JobBlock(ContentGateJob), SelfTestStep);

        Assert.That(selfTest, Does.Contain("python3 " + ScriptPath + " --self-test"));
        Assert.That(Regex.IsMatch(selfTest, @"^\s+if:", RegexOptions.Multiline), Is.False,
            $"'{SelfTestStep}' must carry no condition: a pull request changing only {ScriptPath} is "
            + "lightweight, skips the report, and would otherwise run no check of the script at all.");
    }

    /// <summary>One step of a job, from its <c>- name:</c> line to the next step.</summary>
    private static string Step(string job, string name)
    {
        var start = Regex.Match(job, $@"^      - name: {Regex.Escape(name)}[ \t]*\r?$", RegexOptions.Multiline);
        Assert.That(start.Success, Is.True, $"expected a '{name}' step in the '{ContentGateJob}' job of {WorkflowPath}");

        var rest = job[start.Index..];
        var next = Regex.Match(rest[start.Length..], @"^      - ", RegexOptions.Multiline);
        return next.Success ? rest[..(start.Length + next.Index)] : rest;
    }

    /// <summary>The body of one top-level job, from its two-space key to the next one.</summary>
    private static string JobBlock(string jobId)
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        var start = Regex.Match(yaml, $@"^  {Regex.Escape(jobId)}:[ \t]*\r?$", RegexOptions.Multiline);
        Assert.That(start.Success, Is.True, $"expected a '{jobId}:' job in {WorkflowPath}");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^  [A-Za-z0-9_.-]+:[ \t]*\r?$", RegexOptions.Multiline);

        return next.Success ? rest[..next.Index] : rest;
    }
}
