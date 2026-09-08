using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The change-detection gate: every workflow step that decides what a pull
/// request changed must ask the right question, and the checkout it depends on
/// must make that question answerable.
/// <para>
/// The workflows under <c>.github/workflows/</c> select which packages, samples
/// and apps to build from a <c>git diff</c> between the pull request's base and
/// head SHAs. The TWO-dot form <c>git diff A B</c> reports "what turns A into
/// B", which is not the question: when the head branch is behind its base it
/// also reports the REVERSE of every commit merged into the base since the
/// branch was cut, and attributes those files to the pull request. The
/// THREE-dot form <c>git diff A...B</c> diffs against the merge base, which is
/// "what did this branch change".
/// </para>
/// <para>
/// This is guarded rather than reviewed by eye because both failure directions
/// are quiet. Over-selection merely runs packages the pull request never
/// touched, so it reads as a slow pipeline rather than a bug. Under-selection
/// is worse and is a FALSE GREEN: if the pull request sets a file to some
/// content and the base independently set the same file to byte-identical
/// content, the two-dot diff reports no difference for it and its package is
/// dropped from the matrix entirely. Neither shows up as a failing check.
/// </para>
/// <para>
/// The condition is not exotic on the branches it affects. An epic or bucket
/// integration branch deliberately carries no strict branch protection - that
/// is the entire point of the pattern - so a member pull request is behind its
/// base whenever any other member has merged since it was cut, which on an
/// active integration branch is the ordinary case.
/// </para>
/// <para>
/// Note the asymmetry that makes a blanket "always use three dots" rule wrong,
/// and why this fixture scans <c>git diff</c> only. For <c>git rev-list</c>,
/// the two-dot form is already correct - <c>A..B</c> is exactly the branch's
/// own commits - while <c>A...B</c> is the SYMMETRIC difference and would pull
/// the base's commits in. The trailer guard in <c>ci.yml</c> relies on that and
/// must keep its two dots.
/// </para>
/// <para>
/// The scan covers the <c>*.sh</c> scripts beside the workflows as well as the
/// <c>*.yml</c> files. Part of the package selector now lives in
/// <c>select-test-packages.sh</c>, and a scan globbing only <c>*.yml</c> would
/// quietly stop covering any <c>git diff</c> that moved into (or was added to)
/// a script - shrinking its own denominator without failing, which is the
/// failure mode the denominator assertion exists to prevent.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiChangeDetectionDiffFormTests
{
    private const string WorkflowDirectory = ".github/workflows";

    /// <summary>
    /// A <c>git diff</c> invocation that compares the pull request's base and
    /// head SHAs, in either the correct or the incorrect form.
    /// </summary>
    private static readonly Regex BaseHeadDiff = new(
        @"git\s+diff\b[^\r\n]*\$\{?BASE_SHA\}?[^\r\n]*",
        RegexOptions.Compiled);

    /// <summary>
    /// The correct three-dot spelling, tolerating either <c>$BASE_SHA</c> or
    /// <c>${BASE_SHA}</c> on both ends.
    /// </summary>
    private static readonly Regex ThreeDotForm = new(
        @"\$\{?BASE_SHA\}?\.\.\.\$\{?HEAD_SHA\}?",
        RegexOptions.Compiled);

    /// <summary>Any <c>fetch-depth:</c> setting on a checkout step.</summary>
    private static readonly Regex FetchDepth = new(
        @"^\s*fetch-depth:\s*(?<depth>\S+)\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    [Test]
    public void The_scan_reaches_the_workflows_that_detect_changes()
    {
        // Without this the assertions below would pass vacuously if the
        // workflow directory moved or the steps were renamed away.
        Assert.That(
            ScannedFiles(),
            Is.Not.Empty,
            $"the scan must reach {WorkflowDirectory}");

        Assert.That(
            ScannedFiles().Sum(file => BaseHeadDiff.Matches(file.Text).Count),
            Is.GreaterThanOrEqualTo(6),
            $"the scan must reach the change-detection steps in {WorkflowDirectory}; finding none (or far "
            + "fewer than the six that exist) means the pattern no longer matches how the diffs are spelled, "
            + "and this fixture is asserting nothing.");
    }

    [Test]
    public void Every_base_to_head_diff_uses_the_three_dot_form()
    {
        var offenders = ScannedFiles()
            .SelectMany(file => BaseHeadDiff
                .Matches(file.Text)
                .Where(match => !ThreeDotForm.IsMatch(match.Value))
                .Select(match => $"{file.Name}: {match.Value.Trim()}"))
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "these steps decide what the pull request changed from a two-dot diff, which reports the reverse "
            + "of everything merged into the base since the branch was cut (over-selection), and reports "
            + "nothing at all for a file the base independently set to byte-identical content "
            + "(under-selection, a false green). Use \"$BASE_SHA...$HEAD_SHA\"."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_workflow_that_diffs_against_the_merge_base_checks_out_full_history()
    {
        var files = ScannedFiles();

        // A workflow counts as diffing when it runs a base-to-head diff itself
        // OR invokes a script beside it that does. Without the second half this
        // check would go vacuous the moment a diff moved out of the YAML and
        // into a script - which is exactly what happened to the package
        // selector - leaving the shallow-clone hole unguarded.
        var diffing = files
            .Where(file => file.Name.EndsWith(".yml", StringComparison.Ordinal))
            .Where(workflow => BaseHeadDiff.IsMatch(workflow.Text)
                || files.Any(script =>
                    script.Name.EndsWith(".sh", StringComparison.Ordinal)
                    && BaseHeadDiff.IsMatch(script.Text)
                    && workflow.Text.Contains(Path.GetFileName(script.Name), StringComparison.Ordinal)))
            .ToArray();

        Assert.That(
            diffing,
            Is.Not.Empty,
            "expected at least one workflow to diff base against head, directly or through a script it runs");

        foreach (var workflow in diffing)
        {
            var depths = FetchDepth
                .Matches(workflow.Text)
                .Select(match => match.Groups["depth"].Value)
                .ToArray();

            Assert.That(
                depths,
                Is.Not.Empty,
                $"{workflow.Name} diffs against the merge base but its checkout declares no fetch-depth, so it "
                + "gets the default shallow clone. A three-dot diff resolves the merge base and needs the "
                + "common ancestry of both commits present locally; without it the step fails outright.");

            Assert.That(
                depths,
                Is.All.EqualTo("0"),
                $"{workflow.Name} diffs against the merge base, so every checkout in it must use "
                + "'fetch-depth: 0'. A shallow clone does not contain the merge base.");
        }
    }

    /// <summary>
    /// The workflow definitions and the shell scripts beside them. Both are
    /// scanned because the package selector's logic is split across the two.
    /// </summary>
    private static IReadOnlyList<ScannedFile> ScannedFiles()
    {
        var directory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowDirectory.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(Directory.Exists(directory), Is.True, $"expected {WorkflowDirectory}");

        return new[] { "*.yml", "*.sh" }
            .SelectMany(pattern => HygieneRepository.EnumerateFiles(directory, pattern))
            .OrderBy(path => path, StringComparer.Ordinal)
            .Select(path => new ScannedFile(
                $"{WorkflowDirectory}/{Path.GetFileName(path)}",
                File.ReadAllText(path)))
            .ToArray();
    }

    private sealed record ScannedFile(string Name, string Text);
}
