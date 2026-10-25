using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One hand-maintained census count found in the prose of
/// <c>spec/atomic-commit/Refinement.md</c>.
/// </summary>
/// <param name="LineNumber">The 1-based line in the note, for failure messages.</param>
/// <param name="Line">The whole offending line, so the message quotes context.</param>
/// <param name="Text">The matched fragment, so the message names the claim.</param>
internal sealed record CensusCountClaim(int LineNumber, string Line, string Text);

/// <summary>
/// Detects a hand-maintained census count reappearing in the prose of
/// <c>spec/atomic-commit/Refinement.md</c> - a claim of the shape "ten rows detected", "9
/// detected", or "four gaps".
/// <para>
/// WHY THIS EXISTS. #2560 was a false census sentence in the note ("ten rows
/// detected, three partial or undetected", against an actual 9 / 3 / 1) that
/// was restated with three different wrong figures in the gate's own comment.
/// Correcting the numbers would have fixed the instance and left the class:
/// the note is precisely the document whose counts change as #2551-#2554 land,
/// so any figure written by hand into it is false again within days. The
/// numbers were therefore removed and the method of re-deriving them written
/// down instead. This detector is what stops the next author putting a figure
/// back.
/// </para>
/// <para>
/// SCOPE, AND WHY IT IS DRAWN HERE. Three deliberate narrowings keep this from
/// firing on legitimate prose.
/// </para>
/// <para>
/// (1) Prose only. Markdown table rows and fenced code blocks are skipped. The
/// forbidden artefact is a *summary of* the Detector column sitting outside
/// it; a Detector cell is free to say "only one of the two production paths
/// this row names is detected", because that cell is the thing being
/// summarised rather than a summary of it.
/// </para>
/// <para>
/// (2) Verdict vocabulary only. The trailing noun must be census-verdict
/// vocabulary (<c>detected</c>, <c>undetected</c>, <c>partial</c>,
/// <c>gap</c>, <c>detector</c>) and never a bare "rows", so "the note's 13
/// behaviour-asserting rows" stays legal. That denominator is legal precisely
/// because it *is* gated, by
/// <c>The_note_yields_the_expected_behaviour_asserting_denominator</c>; a
/// derived-and-gated number is the one shape this rule has no quarrel with.
/// A bare gap is also a domain object (for example, a missing WAL range),
/// not necessarily a detector verdict. Gap counts therefore need census
/// vocabulary in the same sentence clause, an open/unresolved status, or
/// a standalone inventory statement.
/// </para>
/// <para>
/// (3) Issue references are not numerals. A number preceded by <c>#</c> or by
/// a word character is skipped, so "#2551" and "level-C Phases 1-4" are not
/// numerals for this purpose.
/// </para>
/// <para>
/// The parser is deliberately syntactic and therefore approximate. It is a
/// cheap guard against the specific regression that happened, not a proof that
/// the note contains no quantitative claim; a determined author can still
/// smuggle one past it in words. That bound is stated rather than glossed,
/// because a gate whose reach is overstated is the exact defect the parent
/// audit (#2299) keeps finding.
/// </para>
/// <para>
/// Gap classification uses whole words, not identifier substrings, and does
/// not borrow census vocabulary from an earlier sentence. Otherwise a valid
/// configuration description such as "PartialPrefix ... finish one gap"
/// becomes a false census claim (#5001).
/// "Coverage" alone does not identify a detector census: a replay range also
/// has coverage gaps. A remaining-gap inventory is still a quantitative claim.
/// </para>
/// </summary>
internal static class RefinementCensusProse
{
    private const string Numeral =
        @"\d{1,3}|zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
        + @"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty";

    private const string Verdict = @"detected|undetected|detectors?|partial|gaps?";

    /// <summary>
    /// A numeral, then at most two intervening words, then a census verdict.
    /// The window is short on purpose: "three partial", "ten rows detected"
    /// and "9 detected" are the shapes that occurred, while "Three decisions
    /// shaped that column" and "Four options were weighed" - both real
    /// sentences in the note - fall outside it.
    /// </summary>
    private static readonly Regex CountClaim = new(
        $@"(?<![#\w])(?:{Numeral})\b(?:\s+\w+){{0,2}}\s+(?<verdict>{Verdict})\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex CensusContext = new(
        @"\b(?:census|detectors?|detected|undetected|partial)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex OpenGapStatus = new(
        @"^\s+(?:(?:remain|remains|are|is)\s+)?(?:still\s+)?(?:open|unresolved|uncovered|undetected)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex GapInventoryPrefix = new(
        @"^\s*(?:there\s+(?:are|is)\s+)?$",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex GapInventorySuffix = new(
        @"^\s*(?:(?:remain|remains)\s*)?$",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Finds every hand-maintained census count in the prose of the supplied
    /// markdown. Exposed over a string rather than reading the note directly
    /// so it can be driven by hand-written input, including the input it is
    /// supposed to reject and the input it must not flag.
    /// </summary>
    public static IReadOnlyList<CensusCountClaim> FindCountClaims(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);

        var claims = new List<CensusCountClaim>();
        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var inFence = false;

        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            var trimmed = line.Trim();

            if (trimmed.StartsWith("```", StringComparison.Ordinal))
            {
                inFence = !inFence;
                continue;
            }

            if (inFence || trimmed.StartsWith('|'))
            {
                continue;
            }

            foreach (Match match in CountClaim.Matches(line))
            {
                var verdict = match.Groups["verdict"].Value;
                if (verdict.Equals("gap", StringComparison.OrdinalIgnoreCase)
                    || verdict.Equals("gaps", StringComparison.OrdinalIgnoreCase))
                {
                    var start = match.Index;
                    while (start > 0 && ".;!?".IndexOf(line[start - 1]) < 0)
                        start--;
                    var end = match.Index + match.Length;
                    while (end < line.Length && ".;!?".IndexOf(line[end]) < 0)
                        end++;

                    var suffix = line[(match.Index + match.Length)..end];
                    var standaloneInventory = GapInventoryPrefix.IsMatch(line[start..match.Index])
                        && GapInventorySuffix.IsMatch(suffix);
                    if (!standaloneInventory
                        && !CensusContext.IsMatch(line[start..end])
                        && !OpenGapStatus.IsMatch(suffix))
                        continue;
                }

                claims.Add(new CensusCountClaim(i + 1, trimmed, match.Value));
            }
        }

        return claims;
    }
}
