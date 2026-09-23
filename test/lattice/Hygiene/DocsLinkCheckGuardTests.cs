using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The documentation link check: a gate that reported a number it never read,
/// and reported it as passing.
/// <para>
/// <c>docs-site/build.ps1</c> counts docfx warnings and fails the build above a
/// ceiling, which <c>docs.yml</c> pins at zero so a newly broken link or anchor
/// cannot merge. It extracted that count with an anchored pattern applied to
/// the raw build output. docfx colours its summary line when it detects a
/// terminal, which CI provides, so on CI the line arrives as
/// <c>ESC[38;5;11m    2 warning(s)ESC[0m</c>; <c>^\s*</c> cannot match past the
/// escape, the match failed, and the count kept its <c>0</c> default. The
/// ceiling was therefore vacuous on every CI run, while two genuinely broken
/// anchors shipped in the published corpus.
/// </para>
/// <para>
/// The by-type breakdown immediately below it used an UNANCHORED pattern and so
/// was never affected, which is why every affected log printed the
/// contradiction in plain sight - "2 warning(s)", "2 InvalidBookmark", then
/// "Link check: 0 warning(s), ceiling 0". A gate can disagree with the output it
/// is reading and still be green, so agreement between the two is not something
/// a reader will notice; it has to be asserted.
/// </para>
/// <para>
/// The general form outlives this script. A false green is worse than a red
/// because it also asserts there is nothing to fix, and the shape here is the
/// most common one: a parse failure silently indistinguishable from a clean
/// result, because the failure path and the healthy path produce the same
/// value. The remedy is not a better pattern - it is refusing to report a count
/// that was never obtained, which is what the last test below holds in place.
/// </para>
/// </summary>
[TestFixture]
public sealed class DocsLinkCheckGuardTests
{
    private const string ScriptPath = "docs-site/build.ps1";
    private const string WorkflowPath = ".github/workflows/docs.yml";

    /// <summary>The docfx summary line as CI receives it, colour codes and all.</summary>
    private const string ColouredSummaryLine = "\u001b[38;5;11m    2 warning(s)\u001b[0m";

    /// <summary>
    /// The count must be read from ANSI-stripped text. This is the defect
    /// itself: without the strip, the anchored pattern below cannot match on
    /// any run that colours its output.
    /// </summary>
    [Test]
    public void The_link_check_strips_ansi_escapes_before_reading_the_warning_count()
    {
        var script = Script();

        Assert.That(script, Does.Contain("[char]27"),
            $"{ScriptPath} must strip ANSI escape sequences from the docfx output before "
            + "matching the warning summary. docfx colours that line whenever it detects a "
            + "terminal, which CI provides, so an anchored pattern run against the raw line "
            + "matches nothing and the count silently defaults.");

        Assert.That(SummaryMatchSubject(script), Is.Not.EqualTo("output"),
            $"{ScriptPath} must match the warning summary against the ANSI-STRIPPED copy of "
            + "the build output, not against the raw $output. Stripping escapes into a "
            + "variable that is then never used restores the original defect exactly while "
            + "looking like the fix.");
    }

    /// <summary>
    /// Behavioural: the script's own patterns, lifted out of the file and run
    /// against a real coloured summary line, must produce the true count.
    /// Asserting only that the strip appears would pass against a strip that
    /// does not cover the escape docfx actually emits.
    /// </summary>
    [Test]
    public void The_link_checks_own_patterns_read_a_colour_coded_summary_line()
    {
        var script = Script();
        var ansi = AnsiPattern(script);
        var summary = SummaryPattern(script);

        Assert.That(ansi.IsMatch(ColouredSummaryLine), Is.True,
            $"the ANSI pattern in {ScriptPath} does not match the escape sequence docfx "
            + "actually emits, so stripping it leaves the line unchanged and the count "
            + "unreadable.");

        var stripped = ansi.Replace(ColouredSummaryLine, string.Empty);
        var match = summary.Match(stripped);

        Assert.Multiple(() =>
        {
            Assert.That(match.Success, Is.True,
                $"the summary pattern in {ScriptPath} must match the stripped line "
                + $"'{stripped}'. If it does not, the ceiling is applied to a count that "
                + "was never read.");
            Assert.That(match.Groups[1].Value, Is.EqualTo("2"),
                $"the summary pattern in {ScriptPath} must capture the warning COUNT. "
                + "Capturing the wrong group yields a number unrelated to the build.");
        });

        Assert.That(summary.IsMatch(ColouredSummaryLine), Is.False,
            $"the summary pattern in {ScriptPath} is expected NOT to match the raw coloured "
            + "line - that is precisely why the strip is load-bearing. If this ever starts "
            + "matching, the pattern was unanchored and this fixture is no longer "
            + "demonstrating the defect it was written for.");
    }

    /// <summary>
    /// A count that could not be read must fail, not pass. Defaulting to zero
    /// makes an unparsed summary byte-identical in the result to a clean build.
    /// </summary>
    [Test]
    public void The_link_check_fails_when_it_cannot_read_a_warning_count()
    {
        var script = Script();

        Assert.That(script, Does.Not.Match(@"\$warnings\s*=\s*0\b"),
            $"{ScriptPath} must not seed the warning count with 0. That default is what "
            + "turned a parse failure into a passing gate: the value reported for 'could "
            + "not read the output' was identical to the value reported for 'the corpus is "
            + "clean'.");

        Assert.That(script, Does.Match(@"\$null\s+-eq\s+\$warnings"),
            $"{ScriptPath} must branch on an unread count explicitly, so that 'no count' is "
            + "a distinct state from 'a count of zero'.");

        Assert.That(script, Does.Match(@"(?s)\$null\s+-eq\s+\$warnings.{0,600}?throw"),
            $"{ScriptPath} must THROW when it cannot read a count while a ceiling is in "
            + "force. Reporting an unreadable count as a pass is the whole defect; a "
            + "detector that cannot see is not the same as one that sees nothing wrong.");
    }

    /// <summary>
    /// The ceiling must stay wired, and at zero. The parser fix is worth
    /// nothing if the workflow stops asking for a ceiling at all.
    /// </summary>
    [Test]
    public void The_docs_workflow_runs_the_link_check_with_a_zero_ceiling()
    {
        var yaml = File.ReadAllText(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            WorkflowPath.Replace('/', Path.DirectorySeparatorChar)));

        Assert.That(yaml, Does.Contain("docs-site/build.ps1"),
            $"{WorkflowPath} must invoke {ScriptPath}; it is the only link-integrity check "
            + "over the documentation corpus.");

        Assert.That(yaml, Does.Match(@"build\.ps1\s+-MaxWarnings\s+0"),
            $"{WorkflowPath} must pass -MaxWarnings 0. Without a ceiling the script reports "
            + "the count and exits successfully whatever it is, so a broken link would "
            + "merge with the warning printed and nothing failing.");
    }

    /// <summary>
    /// Extracts the ANSI-stripping pattern from the script, resolving the
    /// <c>[char]27</c> the script builds it with so the test drives the real
    /// expression rather than a copy of it.
    /// </summary>
    private static Regex AnsiPattern(string script)
    {
        var match = Regex.Match(script, @"\[regex\]""(?<pattern>[^""]*\[char\]27[^""]*)""");

        Assert.That(match.Success, Is.True,
            $"expected {ScriptPath} to build its ANSI pattern as [regex]\"...[char]27...\". "
            + "This fixture lifts that expression out of the file and runs it, so a change "
            + "in how it is written leaves the pattern untested rather than failing here.");

        var pattern = match.Groups["pattern"].Value.Replace("$([char]27)", "\u001b");

        Assert.That(pattern, Does.Contain("\u001b"),
            $"the ANSI pattern in {ScriptPath} must contain the escape character itself.");

        return new Regex(pattern);
    }

    /// <summary>Extracts the summary-line pattern the script matches the count with.</summary>
    private static Regex SummaryPattern(string script)
    {
        var match = Regex.Match(script, @"Select-String -Pattern '(?<pattern>[^']*warning\\\(s\\\)[^']*)'");

        Assert.That(match.Success, Is.True,
            $"expected {ScriptPath} to select the docfx summary line with a "
            + "Select-String -Pattern over 'warning(s)'. Without it this fixture would "
            + "assert nothing about the expression that actually reads the count.");

        return new Regex(match.Groups["pattern"].Value);
    }

    /// <summary>
    /// The variable the summary <c>Select-String</c> reads from, so the test can
    /// tell a stripped copy from the raw output.
    /// </summary>
    private static string SummaryMatchSubject(string script)
    {
        var match = Regex.Match(
            script,
            @"\$summary\s*=\s*\$(?<subject>\w+)\s*\|\s*Select-String -Pattern '[^']*warning");

        Assert.That(match.Success, Is.True,
            $"expected {ScriptPath} to assign $summary by piping a variable into "
            + "Select-String. If the shape changed, this fixture can no longer tell whether "
            + "the count is read from stripped or raw output.");

        return match.Groups["subject"].Value;
    }

    /// <summary>Reads the script, failing rather than returning an empty string.</summary>
    private static string Script()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ScriptPath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True,
            $"expected {ScriptPath} to exist; it is the documentation link-integrity gate.");

        var text = File.ReadAllText(path);

        Assert.That(text.Trim(), Is.Not.Empty,
            $"{ScriptPath} is empty, so nothing asserted about it means anything");

        return text;
    }
}
