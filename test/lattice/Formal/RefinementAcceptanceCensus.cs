using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One finding of the acceptance census over a refinement note.
/// </summary>
/// <param name="LineNumber">The 1-based line in the note, for failure messages.</param>
/// <param name="Label">The row label, or the section a prose finding sits in.</param>
/// <param name="Reason">Why the line fails the census.</param>
/// <param name="Text">The offending text, so the message quotes it.</param>
internal sealed record RefinementCensusFinding(int LineNumber, string Label, string Reason, string Text)
{
    /// <inheritdoc />
    public override string ToString() => $"line {LineNumber} ({Label}): {Reason}: {Text}";
}

/// <summary>
/// The acceptance census of epic #4430 (issue #4442), made mechanical: a
/// refinement note may carry no row whose Detector verdict admits the
/// behaviour is not detected, and no prose or cell that cites an open issue as
/// the place a gap is tracked.
/// <para>
/// WHY THIS IS A GATE AND NOT A ONE-OFF READING. The epic is accepted only once
/// every row of every area's refinement notes is Yes. A census taken by hand
/// at acceptance is true at the commit it was taken and false again the first
/// time a later change writes a Partial row back, which is the exact drift the
/// sibling gates in this fixture exist to stop. Gating it makes "every row is
/// Yes" a property of the tree rather than of a review.
/// </para>
/// <para>
/// THE VERDICT RULE. A behaviour-asserting row's Detector cell must open with
/// <c>Yes:</c> or <c>Yes,</c>, and the words straight after it must not take
/// the claim back (<c>Yes: partially</c>, <c>Yes, by assumption</c>). Any
/// other table that has a Detector column - a row that is not
/// behaviour-asserting, or an auxiliary table - must not open with a verdict
/// that admits a gap. The sibling gates check that a Yes cell names tests that
/// resolve; this one checks that no cell is anything but Yes.
/// </para>
/// <para>
/// THE GAP-CITATION RULE, AND ITS BOUND. Whether an issue is open is a fact
/// about GitHub, which a test that must run offline cannot read. So this rule
/// is syntactic: it flags the phrasing that cites an issue as the owner of
/// unfinished work - "tracked by #N", "#N (open)", "until #N lands", "the gap
/// is filed as #N" - and it requires every sub-heading under a note's
/// "Territory owned by other open issues" section to declare that no open
/// issue owns a claim, or that the issues it names are closed. It does not
/// flag a past-tense citation such as "until #4476" or "fixed by #4528",
/// because a closed defect recorded as history is what the notes are meant to
/// carry. A determined author can still smuggle an open gap past it in other
/// words; that bound is stated rather than glossed, because a gate whose reach
/// is overstated is the defect the parent audit (#2299) keeps finding.
/// </para>
/// </summary>
internal static class RefinementAcceptanceCensus
{
    /// <summary>The heading of the section that records ownership by other open issues.</summary>
    public const string TerritorySectionPrefix = "Territory owned by";

    private const string Issue = @"#\d{3,}";

    /// <summary>A verdict a behaviour-asserting row may open with.</summary>
    private static readonly Regex AcceptedVerdict = new(
        @"^Yes\s*[:,]",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Words that, straight after <c>Yes:</c>, take the claim back, or that
    /// anywhere in the cell qualify it as argued rather than detected.
    /// </summary>
    private static readonly Regex QualifiedYes = new(
        @"^Yes\s*[:,]\s*(?:partial(?:ly)?|in\s+part|not\s+(?:yet|fully)|assum\w*|argued|by\s+(?:argument|assumption))\b"
        + @"|\bassumption[\s-]only\b|\bargued[\s-]only\b|\bby\s+assumption\s+only\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>A verdict that admits the behaviour is not, or not fully, detected.</summary>
    private static readonly Regex GapVerdict = new(
        @"^(?:Partial(?:ly)?|None|No|Gap|Assum\w*|Argued|Unverified|Undetected|Not\s+detected|Pending|TBD|TODO)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>Phrasings that cite an issue as the owner of unfinished work.</summary>
    private static readonly Regex[] OpenGapCitations =
    [
        new($@"{Issue}\s*\((?:still\s+)?open\)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"{Issue}\s*,\s*(?:still\s+)?(?:open|unfixed|unresolved)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\bopen\s+(?:issue|gap|defect|bug|finding)s?\b[^.;|]{{0,60}}{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"{Issue}[^.;|]{{0,40}}\b(?:is|are|remains?|stays?)\s+(?:still\s+)?(?:open|unfixed|unresolved)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\b(?:tracked|owned|carried)\s+(?:by|in|under)\s+(?:open\s+)?(?:issue\s+)?{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\bgaps?\b[^.;|]{{0,40}}\b(?:filed|tracked|recorded|logged)\b[^.;|]{{0,20}}{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\b(?:filed|logged)\s+as\s+{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\b(?:pending|awaiting|blocked\s+(?:by|on)|waits?\s+(?:for|on))\s+{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\buntil\s+{Issue}\s+(?:lands|merges|closes|is\s+(?:fixed|merged|closed|resolved|done))\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\bnot\s+yet\s+(?:fixed|modelled|modeled|covered|detected|pinned|checked)\b[^.|]{{0,60}}{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"{Issue}[^.|]{{0,60}}\bnot\s+yet\s+(?:fixed|modelled|modeled|covered|detected|pinned|checked)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
        new($@"\b(?:follow-?up|todo|tbd)\b[^.;|]{{0,20}}{Issue}", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled),
    ];

    /// <summary>The sub-headings a territory section may carry: no owner, or closed owners.</summary>
    private static readonly Regex PermittedTerritoryHeading = new(
        @"^(?:No\s+open\s+issue|Closed\b)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// The verdict findings over every table with a Detector column. Rows of
    /// <paramref name="behaviourSections"/> other than
    /// <paramref name="nonBehaviouralRows"/> must read Yes; every other row
    /// must not open with a gap verdict.
    /// </summary>
    /// <param name="tables">Every table of the note, keyed by section.</param>
    /// <param name="behaviourSections">The sections whose rows assert production behaviour.</param>
    /// <param name="nonBehaviouralRows">Row labels that assert no production behaviour.</param>
    public static IReadOnlyList<RefinementCensusFinding> VerdictFindings(
        IReadOnlyDictionary<string, RefinementTable> tables,
        IReadOnlyCollection<string> behaviourSections,
        IReadOnlyCollection<string> nonBehaviouralRows)
    {
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(behaviourSections);
        ArgumentNullException.ThrowIfNull(nonBehaviouralRows);

        var findings = new List<RefinementCensusFinding>();
        foreach (var table in tables.Values)
        {
            var column = RefinementDetectorRule.DetectorColumnOf(table);
            if (column < 0)
            {
                continue;
            }

            var behaviour = behaviourSections.Contains(table.Section, StringComparer.Ordinal);
            foreach (var row in table.Rows)
            {
                var cell = column < row.Cells.Count ? row.Cells[column].Trim() : string.Empty;
                var asserting = behaviour && !nonBehaviouralRows.Contains(row.Label, StringComparer.Ordinal);
                var reason = VerdictProblem(cell, asserting);
                if (reason is not null)
                {
                    findings.Add(new RefinementCensusFinding(row.LineNumber, row.Label, reason, Excerpt(cell)));
                }
            }
        }

        return findings;
    }

    /// <summary>
    /// Why <paramref name="detectorCell"/> fails the verdict rule, or null when
    /// it passes. Exposed over a string so the rule can be driven by
    /// hand-written cells, including the ones it must reject.
    /// </summary>
    /// <param name="detectorCell">The Detector cell's text.</param>
    /// <param name="behaviourAsserting">Whether the row asserts a production behaviour.</param>
    public static string? VerdictProblem(string detectorCell, bool behaviourAsserting)
    {
        ArgumentNullException.ThrowIfNull(detectorCell);
        var cell = detectorCell.Trim();

        if (GapVerdict.IsMatch(cell))
        {
            return "the Detector verdict admits the behaviour is not, or not fully, detected";
        }

        if (QualifiedYes.IsMatch(cell))
        {
            return "the Detector verdict is qualified as partial, argued or assumption-only";
        }

        if (behaviourAsserting && !AcceptedVerdict.IsMatch(cell))
        {
            return "a behaviour-asserting row must open its Detector cell with 'Yes:'";
        }

        return null;
    }

    /// <summary>
    /// The lines of <paramref name="markdown"/> that cite an issue as the owner
    /// of unfinished work, and the territory sub-headings that name an owner.
    /// Fenced code and headings are not prose and are skipped by the phrase
    /// rule; territory sub-headings are checked by their own rule.
    /// </summary>
    /// <param name="markdown">The note's text.</param>
    public static IReadOnlyList<RefinementCensusFinding> OpenGapCitationFindings(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);

        var findings = new List<RefinementCensusFinding>();
        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var inFence = false;
        var section = string.Empty;

        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();

            if (line.StartsWith("```", StringComparison.Ordinal))
            {
                inFence = !inFence;
                continue;
            }

            if (inFence)
            {
                continue;
            }

            if (line.StartsWith("### ", StringComparison.Ordinal))
            {
                var heading = line.TrimStart('#').Trim();
                if (section.StartsWith(TerritorySectionPrefix, StringComparison.OrdinalIgnoreCase)
                    && !PermittedTerritoryHeading.IsMatch(heading))
                {
                    findings.Add(new RefinementCensusFinding(
                        i + 1,
                        section,
                        "a territory sub-heading names an open owner; only 'No open issue ...' or 'Closed ...' may stand there",
                        heading));
                }

                continue;
            }

            if (line.StartsWith("#", StringComparison.Ordinal) && line.TrimStart('#').StartsWith(' '))
            {
                section = line.TrimStart('#').Trim();
                continue;
            }

            foreach (var pattern in OpenGapCitations)
            {
                var match = pattern.Match(line);
                if (match.Success)
                {
                    findings.Add(new RefinementCensusFinding(
                        i + 1,
                        section,
                        "the line cites an issue as the owner of unfinished work",
                        match.Value));
                    break;
                }
            }
        }

        return findings;
    }

    private static string Excerpt(string cell) => cell.Length <= 120 ? cell : cell[..120] + "...";
}
