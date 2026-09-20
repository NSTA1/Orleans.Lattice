using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Duplicate-XML-summary hygiene gate. Fails when a member in <c>src/</c>
/// carries two consecutive <c>summary</c> elements, which ships the first
/// (stale) one and silently discards the second.
/// <para>
/// <b>The failure this defends against.</b> A documentation rewrite that ADDS
/// a summary block instead of REPLACING the existing one leaves the member
/// with two. C# does not diagnose it: there is no warning and no error.
/// Documentation tooling and IntelliSense take the FIRST element, so the
/// rewrite has no effect, and because XML summaries ship inside the NuGet
/// packages the published documentation for a public member is the stale text.
/// </para>
/// <para>
/// <b>Why a gate rather than review.</b> The defect that produced this gate
/// (issue #3291, found in #3288) passed all fifteen repository-wide gates, 210
/// hygiene tests, 134 fixtures, and a 3427-test package suite in silence. It
/// was caught by a human reading the resulting file. It is close to invisible
/// in a diff, because the stale block renders as unchanged context directly
/// above the added block, where it reads as the surrounding code rather than
/// as a duplicate. Nothing about that is specific to the author who hit it, so
/// the next occurrence cannot be assumed to meet a reviewer who reads files
/// rather than diffs.
/// </para>
/// <para>
/// <b>Why it matters more than the count of occurrences suggests.</b> The text
/// a rewrite adds is disproportionately the text explaining why the current
/// form is deliberate, so this defect preferentially destroys exactly the
/// documentation that exists to prevent a regression. In the #3288 case the
/// discarded block was the warning against reverting a readiness signal to its
/// inverted form.
/// </para>
/// <para>
/// <b>Scope is <c>src/</c>, not the whole repository.</b> Only <c>src/</c>
/// ships XML documentation to consumers, which is what makes a stale summary a
/// shipped artefact rather than an internal untidiness. It is deliberately NOT
/// registered in <see cref="CoreHygieneScope.AllPackageSliceRoots"/>, because
/// that registry exists to stop the sliced gates double-scanning and this gate
/// is not sliced.
/// </para>
/// </summary>
[TestFixture]
public sealed class DuplicateXmlSummaryHygieneTests
{
    // Assembled rather than written as one literal for the same reason the
    // perturbation-residue gate splits its marker: a scanner that spells the
    // shape it detects can match itself. This fixture scans src/ and lives in
    // test/, so it is out of scope by path today - but a later widening of the
    // scope would otherwise make the gate red on its own source, and the
    // obvious remedy for that red is to suppress the gate.
    private const string DocPrefix = "///";

    private const string SummaryOpenTag = "<summary>";

    private const string SummaryCloseTag = "</summary>";

    /// <summary>The opening token a documentation block starts with.</summary>
    private static string SummaryOpen => DocPrefix + " " + SummaryOpenTag;

    /// <summary>
    /// Every member under <c>src/</c> carries at most one summary element.
    /// </summary>
    [Test]
    public void No_member_in_src_carries_two_consecutive_xml_summary_blocks()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var src = Path.Combine(repoRoot, "src");

        var violations = new List<string>();
        var filesScanned = 0;
        var summaryOpenings = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(src, "*.cs"))
        {
            filesScanned++;
            var lines = HygieneFiles.TryReadLines(file, out var failure);
            if (lines is null)
            {
                // Unreadable is not clean. A tracked source file the gate could
                // not open must be reported as itself rather than folded into a
                // silent pass, which is the shape of false green this whole
                // family of gates exists to refuse.
                violations.Add(Path.GetRelativePath(repoRoot, file).Replace('\\', '/') + ": " + failure);
                continue;
            }

            var relative = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
            summaryOpenings += CountSummaryOpenings(lines);
            violations.AddRange(FindDuplicateSummaries(relative, lines));
        }

        // Anti-vacuity control on the DENOMINATOR, never on the violation list.
        // A scan that examined nothing produces an empty violation list too, so
        // an empty result is evidence only once these two have fired.
        HygieneDenominator.RequireExamined(
            filesScanned,
            nameof(DuplicateXmlSummaryHygieneTests),
            "C# files",
            "src/ (every tracked *.cs file beneath it)");

        // The second denominator, and the one the first cannot substitute for.
        // A file count stays healthy while the matcher stops recognising the
        // token it keys on - a whitespace change, a prefix typo, a trim that is
        // dropped - and the gate then reports a clean repository it read in
        // full and understood none of. Counting recognised openings is the only
        // observation that separates "no duplicates" from "no comprehension".
        Assert.That(
            summaryOpenings,
            Is.GreaterThan(0),
            $"HYGIENE GATE VACUOUS: '{nameof(DuplicateXmlSummaryHygieneTests)}' read {filesScanned} "
                + $"C# files under src/ and recognised not one '{SummaryOpen}' line. Every file in this "
                + "repository's source tree is documented, so a zero here is a failure of the MATCHER, "
                + "not a statement about the repository: the gate would report a pass no matter how many "
                + "duplicate summary blocks src/ contained. Fix the recognition, do not delete this "
                + "assertion.");

        Assert.That(
            violations,
            Is.Empty,
            "A member carries two consecutive XML summary elements. Documentation tooling takes the "
                + "FIRST one, so the second is silently discarded and the shipped NuGet documentation is "
                + "the stale text. This is almost always a rewrite that ADDED a block instead of "
                + "REPLACING the existing one. Resolve it by reading the FILE, not the diff - the diff "
                + "renders the stale block as unchanged context directly above the added one. If the "
                + "first block documents a NEIGHBOURING member that was displaced, move it to that "
                + "member rather than deleting it; deleting it is how the documentation gets lost."
                + Environment.NewLine
                + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// The positive control, and the only executable evidence that this gate
    /// works. The assertion above concludes something from an EMPTY violation
    /// list, which is also what a matcher that can match nothing produces.
    /// <para>
    /// This defect is not otherwise perturbation-provable: reintroducing a
    /// duplicate summary breaks no compile and fails no other test, so there is
    /// no independent signal to fall back on. A gate for an undetectable defect
    /// is itself undetectable when broken, which is what makes this control
    /// load-bearing rather than decorative.
    /// </para>
    /// </summary>
    [Test]
    public void Control_the_scan_finds_a_duplicate_that_is_present()
    {
        var blockForm = new[]
        {
            "    " + SummaryOpen,
            "    " + DocPrefix + " ... the stale text ...",
            "    " + DocPrefix + " " + SummaryCloseTag,
            "    " + SummaryOpen,
            "    " + DocPrefix + " ... the new text ...",
            "    " + DocPrefix + " " + SummaryCloseTag,
            "    public bool IsReady => IsReadyPhase(Phase);",
        };

        // The single-line stale form. This one is a deliberate superset of the
        // predicate issue #3291 specifies ("a summary-opening line whose
        // immediately preceding line is a summary-closing line"), because a
        // one-line block closes on the same line it opens and would otherwise
        // be the one duplicate shape this gate cannot see.
        var singleLineForm = new[]
        {
            "    " + SummaryOpen + "The stale text." + SummaryCloseTag,
            "    " + SummaryOpen,
            "    " + DocPrefix + " ... the new text ...",
            "    " + DocPrefix + " " + SummaryCloseTag,
            "    private const string Reserved = \"sys-\";",
        };

        Assert.Multiple(() =>
        {
            var blockViolations = FindDuplicateSummaries("Probe.cs", blockForm);
            Assert.That(blockViolations, Has.Count.EqualTo(1),
                "One duplicated block, one violation. A zero here means the matcher matches nothing "
                + "and every green this fixture reports is vacuous.");
            Assert.That(blockViolations[0], Does.StartWith("Probe.cs(4):"),
                "The line number is part of the contract: the failure has to point an author at the "
                + "second block, and an off-by-one sends them to a line they already believe is fine.");

            var singleLineViolations = FindDuplicateSummaries("Probe.cs", singleLineForm);
            Assert.That(singleLineViolations, Has.Count.EqualTo(1),
                "A one-line stale summary followed by a block-form rewrite is the same defect and must "
                + "be caught. A zero here means the gate sees only the shape it was first written for.");
            Assert.That(singleLineViolations[0], Does.StartWith("Probe.cs(2):"));
        });
    }

    /// <summary>
    /// The negative control. Two DIFFERENT members whose documentation blocks
    /// abut is the legitimate shape this predicate must never match, and it is
    /// the one that decides whether the gate is usable: a gate that reddens on
    /// ordinary well-documented code is removed rather than obeyed.
    /// <para>
    /// A member declaration or an attribute line always separates two such
    /// blocks - that is what makes them two members - so the predicate is safe
    /// precisely because it keys on ADJACENCY rather than on counting summary
    /// elements within some parsed span.
    /// </para>
    /// </summary>
    [Test]
    public void Control_the_scan_does_not_fire_on_abutting_blocks_of_two_different_members()
    {
        var blankLineSeparated = new[]
        {
            "    " + SummaryOpen,
            "    " + DocPrefix + " The first member.",
            "    " + DocPrefix + " " + SummaryCloseTag,
            "    public int First { get; init; }",
            string.Empty,
            "    " + SummaryOpen,
            "    " + DocPrefix + " The second member.",
            "    " + DocPrefix + " " + SummaryCloseTag,
            "    public int Second { get; init; }",
        };

        // The tightest legal abutment: no blank line, and an attribute between
        // the documentation and the declaration it documents. This is the
        // serialization shape this repository uses everywhere, so a predicate
        // that fired here would redden hundreds of correct files.
        var attributeSeparated = new[]
        {
            "    " + SummaryOpen + "The first member." + SummaryCloseTag,
            "    [Id(0)]",
            "    public int First { get; init; }",
            "    " + SummaryOpen + "The second member." + SummaryCloseTag,
            "    [Id(1)]",
            "    public int Second { get; init; }",
        };

        Assert.Multiple(() =>
        {
            Assert.That(FindDuplicateSummaries("Probe.cs", blankLineSeparated), Is.Empty,
                "Two different members, each with one summary element, separated by a declaration and a "
                + "blank line. Firing here would make the gate unusable.");
            Assert.That(FindDuplicateSummaries("Probe.cs", attributeSeparated), Is.Empty,
                "Two different members whose one-line documentation abuts across an attribute and a "
                + "declaration. This is the dominant shape in src/, so a match here is a false positive "
                + "on hundreds of correct files.");
        });
    }

    /// <summary>
    /// Counts the documentation lines that open a summary element, which is the
    /// denominator proving the matcher still recognises its own token.
    /// </summary>
    /// <param name="lines">The file's lines. Must not be <see langword="null"/>.</param>
    /// <returns>The number of summary-opening lines.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="lines"/> is <see langword="null"/>.</exception>
    private static int CountSummaryOpenings(IReadOnlyList<string> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);

        var count = 0;
        foreach (var line in lines)
        {
            if (OpensSummary(line)) count++;
        }

        return count;
    }

    /// <summary>
    /// Finds every summary-opening line whose immediately preceding line closed
    /// a summary element, which is the whole detection predicate and needs no
    /// parser.
    /// </summary>
    /// <param name="relativePath">The path reported in a violation. Must not be <see langword="null"/>.</param>
    /// <param name="lines">The file's lines. Must not be <see langword="null"/>.</param>
    /// <returns>One entry per duplicate, formatted <c>path(line): text</c>.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    internal static List<string> FindDuplicateSummaries(string relativePath, IReadOnlyList<string> lines)
    {
        ArgumentNullException.ThrowIfNull(relativePath);
        ArgumentNullException.ThrowIfNull(lines);

        var violations = new List<string>();
        for (var i = 1; i < lines.Count; i++)
        {
            if (!OpensSummary(lines[i])) continue;
            if (!ClosesSummary(lines[i - 1])) continue;

            violations.Add(
                $"{relativePath}({i + 1}): a second summary element opens here, immediately after the one "
                + $"closed on line {i}. The first is what ships; this one is discarded.");
        }

        return violations;
    }

    private static bool OpensSummary(string line) =>
        line.TrimStart().StartsWith(SummaryOpen, StringComparison.Ordinal);

    private static bool ClosesSummary(string line)
    {
        var trimmed = line.Trim();
        return trimmed.StartsWith(DocPrefix, StringComparison.Ordinal)
            && trimmed.EndsWith(SummaryCloseTag, StringComparison.Ordinal);
    }
}
