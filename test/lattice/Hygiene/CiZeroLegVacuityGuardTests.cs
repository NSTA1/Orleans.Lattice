using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The zero-leg vacuity guard: the rig must not be able to report a clean
/// green having selected, and therefore executed, no package's tests at all.
/// <para>
/// <c>summarise-test-legs.py</c> holds the rig's only vacuity guard, and it is
/// correctly unitised - keyed on <c>(package, shard)</c> and summed across
/// every tier, so a shard that executed nothing anywhere fails
/// <c>build-and-test</c>. But the step that runs it is conditioned on
/// <c>leg_count != '0'</c>, so the configuration in which package selection is
/// most badly wrong - selecting NOTHING - is the exact configuration in which
/// the guard protecting against wrong selection does not run (issue #3002). A
/// selection defect whose limiting case selects zero packages emits zero legs,
/// the aggregate is skipped, and no vacuity check runs anywhere.
/// </para>
/// <para>
/// A zero-leg plan is a legitimate outcome for a docs-only change, so the
/// remedy is not "fail on zero legs". The two cases are told apart by a
/// property the <c>leg_count</c> condition does not look at: whether the change
/// touched a file that SEEDS a package. Both halves of that predicate live in
/// <c>zero-leg-guard.sh</c> - one implementation, one self-test - because two
/// matchers that must agree will drift, and the drift would appear as the plan
/// classifying a path the verdict does not.
/// </para>
/// <para>
/// This fixture asserts the WIRING, which is the part no behavioural test can
/// cover. The guard fires precisely when no test legs run, so the run it
/// protects is by definition a run in which nothing else executed; it cannot
/// be exercised as a side effect of ordinary work, and an unwired guard and a
/// clean rig produce the same silence.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiZeroLegVacuityGuardTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";
    private const string GuardPath = ".github/workflows/zero-leg-guard.sh";
    private const string SelfTestPath = ".github/workflows/zero-leg-guard-selftest.py";
    private const string ClassifyStep = "Classify package source changes";
    private const string VerdictStep = "Verdict";
    private const string AggregateStep = "Aggregate test legs";

    /// <summary>
    /// The predicate must exist as a script. Inline in the workflow it has no
    /// executable surface, which is how the aggregate guard's own blind spot
    /// went unnoticed.
    /// </summary>
    [Test]
    public void The_zero_leg_guard_exists_as_an_executable_script()
    {
        Assert.That(File.Exists(Absolute(GuardPath)), Is.True,
            $"expected {GuardPath} to exist. The guard has to be a script rather than "
            + "inline workflow shell so that one implementation serves both the plan-job "
            + "classifier and the build-and-test verdict, and so that a self-test can "
            + "drive it. Two matchers that must agree will drift.");

        var guard = File.ReadAllText(Absolute(GuardPath));

        Assert.That(guard, Does.Contain("classify").And.Contain("verdict"),
            $"{GuardPath} must expose both halves of the predicate. The plan job "
            + "classifies the changed-path set; build-and-test applies the verdict. "
            + "Splitting them across two files reintroduces the drift this design avoids.");
    }

    /// <summary>
    /// The aggregate guard is still conditioned on a non-zero leg count, so
    /// its blind spot is real and this guard is the only thing covering it.
    /// If that condition is ever removed, this guard becomes redundant and
    /// this fixture should be revisited rather than silently left in place.
    /// </summary>
    [Test]
    public void The_aggregate_vacuity_guard_is_still_skipped_on_a_zero_leg_plan()
    {
        var step = Step(AggregateStep);

        Assert.That(step, Does.Contain("leg_count != '0'"),
            $"the '{AggregateStep}' step in {WorkflowPath} no longer skips on a zero-leg "
            + "plan. That was the entire premise of the zero-leg guard (#3002): the "
            + "aggregate vacuity check does not run in the one configuration where "
            + "selection is most badly wrong. If the aggregate now covers zero legs "
            + "itself, the separate guard is redundant and should be retired "
            + "deliberately - not left running as unexamined ceremony.");
    }

    /// <summary>
    /// The plan job must classify the changed-path set, from the merge base,
    /// and only on a pull request.
    /// </summary>
    [Test]
    public void The_plan_job_classifies_the_changed_path_set_for_the_guard()
    {
        var step = Step(ClassifyStep);

        Assert.That(step, Does.Contain(GuardPath),
            $"the '{ClassifyStep}' step in {WorkflowPath} must delegate to {GuardPath} "
            + "rather than reimplementing the path predicate inline, or the plan can "
            + "classify a path the verdict does not.");

        Assert.That(step, Does.Contain("BASE_SHA").And.Contain("HEAD_SHA"),
            $"the '{ClassifyStep}' step in {WorkflowPath} must address its diff with the "
            + "event's base and head shas. On a pull_request event HEAD is the synthetic "
            + "merge commit, so a diff anchored on HEAD would describe the wrong change.");

        Assert.That(step, Does.Contain("github.event_name == 'pull_request'"),
            $"the '{ClassifyStep}' step in {WorkflowPath} must be pull-request-only, like "
            + "every other change-detection step. A push lane has no merge base to diff "
            + "against, and the guard reads an EMPTY classification as 'nothing to "
            + "judge' precisely so that lane is never failed on a property nothing "
            + "measured.");
    }

    /// <summary>
    /// The classification must reach <c>build-and-test</c>. A job cannot read
    /// another job's step outputs, so an unexported classification would
    /// arrive as the empty string - which the guard reads as "nothing to
    /// judge", disabling it silently and on every run.
    /// </summary>
    [Test]
    public void The_classification_is_exported_as_a_plan_output()
    {
        var yaml = Workflow();

        Assert.That(yaml, Does.Match(@"(?m)^      package_source_changed: \$\{\{ steps\.pkgsrc\.outputs\.changed \}\}[ \t]*\r?$"),
            $"{WorkflowPath} must re-export the classification as the plan job output "
            + "`package_source_changed`. A job cannot read another job's step outputs, so "
            + "without this the verdict would receive the empty string on EVERY run - "
            + "which the guard treats as 'not a pull request, nothing to judge'. The "
            + "guard would then be disabled everywhere while still appearing wired.");
    }

    /// <summary>
    /// The verdict must be applied where it always runs, with all three of its
    /// inputs. Any missing input silently degrades the guard to one of its
    /// early returns.
    /// </summary>
    [Test]
    public void The_build_and_test_verdict_applies_the_zero_leg_guard()
    {
        var step = Step(VerdictStep);

        Assert.That(step, Does.Contain(GuardPath),
            $"the '{VerdictStep}' step in {WorkflowPath} must invoke {GuardPath}. This "
            + "step is the one thing in the rig that always runs and it already reads "
            + "LEG_COUNT, so it is the single narrowest seam for the check. Moving the "
            + "guard into any conditioned step reintroduces the blind spot it closes.");

        foreach (var input in new[] { "--leg-count", "--package-source-changed", "--version-only" })
        {
            Assert.That(step, Does.Contain(input),
                $"the '{VerdictStep}' step in {WorkflowPath} must pass {input} to the "
                + "guard. Each omitted input degrades the guard to one of its early "
                + "returns, which reports a reason and passes - so an incompletely "
                + "wired guard looks exactly like a healthy run.");
        }

        Assert.That(step, Does.Contain("PACKAGE_SOURCE_CHANGED: ${{ needs.plan.outputs.package_source_changed }}"),
            $"the '{VerdictStep}' step in {WorkflowPath} must read the classification from "
            + "the plan job's output.");

        Assert.That(step, Does.Contain("VERSION_ONLY: ${{ needs.plan.outputs.version_only }}"),
            $"the '{VerdictStep}' step in {WorkflowPath} must read `version_only` from the "
            + "plan job. It is the guard's one carve-out: a coordinated release rewrites "
            + "the <Version> slot of every publishable csproj - files under src/** that "
            + "seed a package - and deliberately plans zero legs. Without this input "
            + "every release would fail build-and-test.");
    }

    /// <summary>
    /// The guard's self-test must be invoked by the workflow. An uninvoked
    /// detector and a healthy guard report the same nothing.
    /// </summary>
    [Test]
    public void The_zero_leg_guards_self_test_is_invoked_by_the_workflow()
    {
        Assert.That(File.Exists(Absolute(SelfTestPath)), Is.True,
            $"expected {SelfTestPath} to exist. It is the only executable exercise of the "
            + "guard: the guard fires only when no test legs run, so on every healthy "
            + "pull request it takes an early return, and on the one run where it "
            + "matters there is nothing else running to notice whether it was right.");

        Assert.That(Workflow(), Does.Contain(SelfTestPath),
            $"{WorkflowPath} must invoke {SelfTestPath}. A self-test that is never run "
            + "reports nothing whether the guard is sound or inverted, so leaving it "
            + "unwired would remove the guard's only coverage without removing any green.");
    }

    /// <summary>
    /// The self-test must drive the real script rather than a transcription of
    /// it, and must prove each of its mutations still applies. A mutation
    /// whose anchor has drifted out of the script tests nothing while
    /// continuing to report a pass.
    /// </summary>
    [Test]
    public void The_zero_leg_guards_self_test_drives_the_real_script()
    {
        var selfTest = File.ReadAllText(Absolute(SelfTestPath));

        Assert.That(selfTest, Does.Contain("zero-leg-guard.sh"),
            $"{SelfTestPath} must drive the real {GuardPath}. A self-test that "
            + "reimplements the predicate in Python asserts that the transcription "
            + "matches the cases, not that the shipped guard does.");

        Assert.That(selfTest, Does.Contain("MUTATIONS"),
            $"{SelfTestPath} must carry a mutation suite. Every case in it passes "
            + "against a guard that always returns success, so without mutations a "
            + "neutered guard would report a clean self-test.");

        Assert.That(selfTest, Does.Contain("occurrences != 1"),
            $"{SelfTestPath} must assert that each mutation anchor still appears exactly "
            + "once in the guard. An anchor that no longer matches applies no mutation, "
            + "so the suite would keep reporting a pass while silently testing nothing - "
            + "the same class of failure the guard itself exists to catch.");
    }

    private static string Workflow() => File.ReadAllText(Absolute(WorkflowPath));

    private static string Absolute(string repoRelative) => Path.Combine(
        HygieneRepository.FindRepoRoot(),
        repoRelative.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Reads one workflow step by its <c>name:</c>, up to the next step at the
    /// same indentation. Fails rather than returning an empty string, because
    /// every assertion above would pass against one.
    /// </summary>
    private static string Step(string stepName)
    {
        var yaml = Workflow();

        var start = Regex.Match(yaml, $@"^      - name: {Regex.Escape(stepName)}[ \t]*\r?$",
            RegexOptions.Multiline);

        Assert.That(start.Success, Is.True,
            $"expected a step named '{stepName}' in {WorkflowPath}. If it was renamed, this "
            + "fixture stopped guarding it and must be updated with it.");

        var rest = yaml[(start.Index + start.Length)..];
        var next = Regex.Match(rest, @"^      - (name|uses):", RegexOptions.Multiline);
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.That(block.Trim(), Is.Not.Empty,
            $"the step named '{stepName}' in {WorkflowPath} has an empty body, so nothing "
            + "asserted about it means anything");

        return block;
    }
}
