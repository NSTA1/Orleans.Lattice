using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The test gate must not report that a change is irrelevant to the tests when
/// the changed file IS the body of a test.
/// <para>
/// <b>What went wrong.</b> <c>ci.yml</c>'s <c>nonSample</c> path filter carries
/// <c>'!samples/**'</c>, so a samples-only pull request selected no package leg
/// and the gate took its <c>else</c> arm, which reported
/// <c>"no test-relevant files changed"</c>. That was sound while every sample
/// was self-contained, and stopped being sound the moment a sample became the
/// body of a test:
/// <c>samples/RepoContextContainer/scripts/Test-ContainerProvenance.ps1</c> is a
/// 92-assertion provenance suite whose only executor is a fixture in
/// <c>test/lattice.api.mcp.repocontext</c> that shells out to it. Editing the
/// suite ran neither the suite nor anything else, and <c>build-and-test</c>
/// reported success. See #2653.
/// </para>
/// <para>
/// <b>Why the reason string is guarded and not just the selection.</b> The
/// selection being wrong cost one un-run suite. The REASON being wrong is what
/// made that unreadable: the run's own output asserted there was nothing to
/// select, so the hole could not be seen from the artefact you would look at to
/// find it. A gate that declines to run tests is legitimate; one that
/// misdescribes why is a defect on its own terms, because every future reader
/// is told the question was asked and answered.
/// </para>
/// <para>
/// <b>Why this lives in C# rather than in the selector's own self-test.</b> The
/// fix has two halves. <c>select-test-packages.sh</c> learned to derive a
/// <c>test/{package}/</c> -&gt; <c>samples/{sample}/</c> dependency from the test
/// sources, and <c>select-test-packages-selftest.sh</c> guards that half - it
/// plants a reference and requires the scan to move in both directions. But the
/// selector is only consulted if <c>ci.yml</c> asks it: delete the
/// <c>sampledeps</c> step or the gate branch that reads it and the selector
/// keeps answering correctly, the self-test keeps passing, and the hole is
/// fully reopened with nothing red anywhere. This fixture guards the wiring
/// between the two halves, which neither half can guard for itself.
/// </para>
/// <para>
/// It is named <c>*HygieneTests</c> deliberately. The content gate selects
/// <c>FullyQualifiedName~Hygiene</c> and runs on EVERY pull request with no
/// path condition, so this fixture executes even on the samples-only diff shape
/// it exists to police - which a fixture selected by the package matrix could
/// not do, since the matrix not running is the failure being guarded.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiSampleSelectionHygieneTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";
    private const string SelectorPath = ".github/workflows/select-test-packages.sh";

    /// <summary>
    /// The exact reason string the gate used to emit for a samples-only change.
    /// It is spelled here as a fragment so that this file's own prose, which
    /// quotes it above, cannot be what the scan finds.
    /// </summary>
    private const string RetiredFalseReason = "reason=\"no test-relevant files changed\"";

    /// <summary>
    /// Matches the exclusion as a YAML list ITEM, not as text anywhere in the
    /// file. The distinction is load-bearing and was found by perturbation: the
    /// comment above the gate quotes <c>'!samples/**'</c> while explaining why
    /// it is there, so a plain substring check stays green when the real filter
    /// entry is deleted - satisfied by the prose that describes the thing it
    /// was meant to police.
    /// </summary>
    private static readonly Regex SamplesExclusionEntry = new(
        @"^\s*-\s*'!samples/\*\*'\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex SampleDepsStepId = new(
        @"^\s*id:\s*sampledeps\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex GateStepId = new(
        @"^\s*id:\s*gate\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex SampleDependentsImport = new(
        @"^\s*SAMPLE_DEPENDENTS:\s*\$\{\{\s*steps\.sampledeps\.outputs\.found\s*\}\}\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex SampleDependentsInvocation = new(
        @"^\s*--sample-dependents\s*\\?\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static string WorkflowText() =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), WorkflowPath));

    private static string SelectorText() =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), SelectorPath));

    [Test]
    public void The_scan_reaches_the_gate_it_polices()
    {
        // Non-vacuity. Every assertion below is a search over these two files,
        // so if either moved or were renamed the whole fixture would report
        // success having examined nothing - the same class of silent absence it
        // exists to catch.
        var workflow = WorkflowText();

        Assert.That(workflow, Is.Not.Empty, $"{WorkflowPath} must be readable");
        Assert.That(SelectorText(), Is.Not.Empty, $"{SelectorPath} must be readable");

        Assert.That(
            GateStepId.IsMatch(workflow),
            Is.True,
            $"{WorkflowPath} must still contain the test gate (a step with `id: gate`); this fixture asserts "
            + "over its branches and can assert nothing if the step was renamed.");

        Assert.That(
            workflow,
            Does.Contain("run_tests="),
            $"{WorkflowPath} must still contain the gate's run_tests decision.");
    }

    [Test]
    public void The_gate_consults_the_selector_about_changed_samples()
    {
        var workflow = WorkflowText();

        Assert.That(
            SampleDependentsInvocation.IsMatch(workflow),
            Is.True,
            $"{WorkflowPath} must invoke the selector's --sample-dependents mode. Without it nothing asks "
            + "whether a changed sample is read by a test project, and a samples-only change to the body of a "
            + "test selects no leg again (#2653).");

        Assert.That(
            SampleDepsStepId.IsMatch(workflow),
            Is.True,
            $"{WorkflowPath} must expose that query as a step the gate can read (a step with `id: sampledeps`).");

        Assert.That(
            SampleDependentsImport.IsMatch(workflow),
            Is.True,
            $"the gate step in {WorkflowPath} must import the sampledeps result into its environment; a step "
            + "whose output nothing reads is dead configuration and the gate would decide as if it had never "
            + "run.");

        Assert.That(
            Regex.IsMatch(workflow, @"\$SAMPLE_DEPENDENTS""?\s*=\s*""true"""),
            Is.True,
            $"the gate in {WorkflowPath} must branch on SAMPLE_DEPENDENTS. Importing it into the environment "
            + "without testing it would satisfy every other assertion here while changing no decision.");
    }

    [Test]
    public void The_selector_still_offers_the_mode_the_gate_asks_for()
    {
        // The two halves are in different languages and neither compiler nor
        // shell checks the seam between them: ci.yml passing an argument the
        // script does not recognise is not an error at author time, and the
        // script's own self-test cannot know which arguments ci.yml sends.
        Assert.That(
            SelectorText(),
            Does.Contain("--sample-dependents"),
            $"{SelectorPath} must still implement the --sample-dependents mode that {WorkflowPath} invokes.");
    }

    [Test]
    public void No_branch_of_the_gate_claims_no_test_relevant_files_changed()
    {
        var workflow = WorkflowText();

        Assert.That(
            workflow,
            Does.Not.Contain(RetiredFalseReason),
            $"{WorkflowPath} has reinstated the reason string retired by #2653. It is false for a samples-only "
            + "change whose changed file is the body of a test, and a wrong reason is worse than a wrong "
            + "selection: it tells every future reader that the question was asked and answered, so the hole "
            + "cannot be seen from the run output. State what was actually checked instead.");
    }

    [Test]
    public void The_gate_does_not_close_the_hole_by_running_everything()
    {
        var workflow = WorkflowText();

        // The cheap fix is to drop '!samples/**' from the nonSample filter, so
        // that ANY samples change runs the full 46-package matrix. That closes
        // the hole and replaces it with a permanent cost on every sample edit,
        // in a repository that bucket-branches specifically to avoid burning CI
        // cycles. Guarded because it is a one-line change that looks like a
        // simplification.
        Assert.That(
            SamplesExclusionEntry.IsMatch(workflow),
            Is.True,
            $"{WorkflowPath}'s nonSample filter must keep excluding samples/** as a list entry. Including them "
            + "makes every samples-only change run the full package matrix, which closes #2653 by making "
            + "everything run rather than by selecting the legs that read the changed sample.");
    }
}
