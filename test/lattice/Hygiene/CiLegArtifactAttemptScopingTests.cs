using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The leg-artifact collection gate: a re-run must not be able to feed the
/// aggregate report a superseded result.
/// <para>
/// Workflow artifacts are scoped to the RUN, not to the run attempt. Re-running
/// a failed leg therefore uploads a second artifact beside the first rather
/// than replacing it, and every leg writes its record under the same file name.
/// With the artifacts merged into one directory the two records collide on
/// extraction, the survivor is not determined, and the aggregate reports it
/// without any indication that it came from a superseded attempt (issue #2924).
/// </para>
/// <para>
/// The runtime half of the guard lives in <c>summarise-test-legs.py</c>, which
/// keeps the highest attempt per leg and refuses two results for one leg under
/// one attempt. That half cannot defend itself here: a collision that already
/// overwrote a file is invisible to the script, because only one record
/// survives to be read. The collection has to be wired so both records reach
/// it, and this fixture is what holds that wiring in place.
/// </para>
/// <para>
/// The third assertion is the one worth explaining, because the obvious fix is
/// the one it forbids. Narrowing the download to the CURRENT attempt looks like
/// the natural counterpart to an attempt-scoped upload name, and it is wrong:
/// "re-run failed jobs" does not re-run the legs that passed, so they upload
/// nothing under the new attempt and would simply vanish from the collection.
/// That converts a recoverable run into one that can never report - a worse
/// failure than the one being fixed, reached by applying a correct idea outside
/// the range it holds on.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiLegArtifactAttemptScopingTests
{
    private const string WorkflowPath = ".github/workflows/ci.yml";

    /// <summary>
    /// Each leg's result artifact must carry the run attempt in its name, so a
    /// re-run adds a distinctly named artifact instead of a second one sharing
    /// the first one's name.
    /// </summary>
    [Test]
    public void Leg_result_artifacts_are_named_with_the_run_attempt()
    {
        var step = Step("Upload leg results");

        Assert.That(step, Does.Contain("leg-results-"),
            $"expected the 'Upload leg results' step in {WorkflowPath} to name a "
            + "'leg-results-' artifact; this fixture found the step but not the name, so "
            + "the assertions below would have been vacuous");

        Assert.That(step, Does.Contain("github.run_attempt"),
            $"the 'Upload leg results' step in {WorkflowPath} must include "
            + "${{ github.run_attempt }} in its artifact name. Artifacts are scoped to the "
            + "run, not the attempt, so without it a re-run uploads a second artifact under "
            + "the same name and the aggregate reads whichever survived extraction.");
    }

    /// <summary>
    /// The per-leg test output artifacts have the same shape and the same
    /// collision, and are read by humans diagnosing a failure.
    /// </summary>
    [Test]
    public void Test_output_artifacts_are_named_with_the_run_attempt()
    {
        var step = Step("Upload test artifacts");

        Assert.That(step, Does.Contain("test-output-"),
            $"expected the 'Upload test artifacts' step in {WorkflowPath} to name a "
            + "'test-output-' artifact; this fixture found the step but not the name");

        Assert.That(step, Does.Contain("github.run_attempt"),
            $"the 'Upload test artifacts' step in {WorkflowPath} must include "
            + "${{ github.run_attempt }} in its artifact name, for the same reason as the "
            + "leg results: a re-run otherwise leaves two artifacts sharing one name and "
            + "the trx files inside them overwrite each other.");
    }

    /// <summary>
    /// The download must not merge the artifacts into a single directory. Every
    /// leg writes its record under the same file name, so merging makes the
    /// attempt-scoped artifact names achieve nothing.
    /// </summary>
    [Test]
    public void Leg_results_are_not_merged_into_one_directory()
    {
        var step = Step("Download leg results");

        Assert.That(step, Does.Contain("pattern:"),
            $"expected the 'Download leg results' step in {WorkflowPath} to declare a "
            + "'pattern:'; this fixture found the step but not the pattern, so the "
            + "assertion below would have been vacuous");

        Assert.That(step, Does.Not.Contain("merge-multiple: true"),
            $"the 'Download leg results' step in {WorkflowPath} must not set "
            + "'merge-multiple: true'. Every leg writes its record under the same file "
            + "name, so merging every artifact into one directory makes the two attempts "
            + "collide on extraction even when their artifact names differ, which is "
            + "precisely what the attempt in the name exists to prevent.");
    }

    /// <summary>
    /// The download must collect every attempt, not only the current one.
    /// Narrowing it would silently drop every leg that passed first time.
    /// </summary>
    [Test]
    public void Leg_results_are_collected_from_every_attempt()
    {
        var step = Step("Download leg results");

        var pattern = Regex.Match(step, @"^\s*pattern:\s*(?<value>\S+)\s*$", RegexOptions.Multiline);

        Assert.That(pattern.Success, Is.True,
            $"expected a 'pattern:' on the 'Download leg results' step in {WorkflowPath}; "
            + "without it this fixture asserts nothing");

        Assert.That(pattern.Groups["value"].Value, Does.Not.Contain("run_attempt"),
            $"the 'Download leg results' pattern in {WorkflowPath} must not be narrowed to "
            + "the current run attempt. Re-running failed jobs does not re-run the legs "
            + "that passed, so those legs upload nothing under the new attempt and a "
            + "narrowed pattern would collect only the re-run legs and report every other "
            + "leg as missing. The aggregate selects the highest attempt per leg instead.");
    }

    /// <summary>
    /// Reads one workflow step by its <c>name:</c>, up to the next step at the
    /// same indentation. Fails rather than returning an empty string, because
    /// every assertion above would pass against one.
    /// </summary>
    private static string Step(string stepName)
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

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
