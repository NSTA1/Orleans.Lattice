using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One <c>Type.Member</c> reference to production C#, named in backticks by a
/// row of a <c>spec/Refinement.md</c> mapping table.
/// </summary>
internal sealed record RefinementCodeSymbol
{
    /// <summary>The mapping section the reference was found in.</summary>
    public required string Section { get; init; }

    /// <summary>The row's first cell, naming the spec construct it maps.</summary>
    public required string Row { get; init; }

    /// <summary>The 1-based line of the note the reference was found on.</summary>
    public required int LineNumber { get; init; }

    /// <summary>The reference exactly as written, for example <c>ShardRootGrain.TxTerminal</c>.</summary>
    public required string Text { get; init; }

    /// <summary>The part before the dot.</summary>
    public required string TypeName { get; init; }

    /// <summary>
    /// The part after the dot. Named "member" because that is what it usually
    /// is, but the note also uses this form for a partial-class file suffix and
    /// for a nested type, so a resolver must not assume it is a member.
    /// </summary>
    public required string MemberName { get; init; }

    /// <summary>The reference as written, which is what NUnit renders.</summary>
    public override string ToString() => Text;
}

/// <summary>
/// Extracts the production-code references out of the mapping tables in
/// <c>spec/Refinement.md</c>.
/// <para>
/// WHAT IS EXTRACTED, AND WHY IT IS NARROW. Only backticked, dotted
/// <c>Type.Member</c> forms are taken, and in the production columns both
/// halves must begin with an upper-case letter (or an underscore on the
/// right-hand side). That rule is chosen to
/// be conservative in one specific direction: it must not manufacture
/// references that were never claims about code, because a staleness gate that
/// cries wolf is suppressed within a week and is then worse than no gate at
/// all. The <c>Detector</c> column relaxes the right-hand side, for the
/// reasons given on <c>DetectorReference</c> below.
/// </para>
/// <para>
/// The tables are full of backticked text that looks like an identifier but is
/// not a code symbol: TLA+ variables (<c>phase[t]</c>, <c>vote[t][k]</c>),
/// spec-level string values (<c>init</c>, <c>prepared</c>), enum members quoted
/// without their type (<c>InFlight</c>, <c>Committed</c>), parameter names
/// (<c>alreadyTerminal</c>), and issue references (<c>#1584</c>). A bare
/// backticked identifier is not distinguishable from any of those without a
/// judgement call, so bare identifiers are deliberately NOT checked, and the
/// note's file-path forms such as <c>AtomicCommit.tla</c> are excluded by the
/// upper-case requirement on the right-hand side.
/// </para>
/// <para>
/// THE DETECTOR COLUMN IS EXCLUDED, BY HEADER NAME. The property and action
/// tables carry a <c>Detector</c> column (added by #2527) whose cells name
/// TEST fixtures and methods, for example
/// <c>AtomicVisibilityGateTests.InFlight_always_falls_through</c>. Those match
/// the dotted shape perfectly but live under <c>test/</c>, not <c>src/</c>, so
/// resolving them here would fail every one of them and break the gate. They
/// are a different kind of claim and get their own resolver
/// (<see cref="RefinementDetectorResolver"/>), so this extractor skips that
/// column outright and <see cref="ExtractDetectors"/> reads it alone, under a
/// pattern of its own. The skip is keyed on the header text rather than on a
/// column index, so inserting a column ahead of it cannot silently
/// re-include it.
/// </para>
/// <para>
/// The cost of that narrowing is honest and worth stating: a rename of a
/// symbol the note names only in bare form (<c>ExecutePhaseAsync</c>,
/// <c>AppendTxTerminalAsync</c>, <c>TxDecisionView</c>) is not caught here.
/// Multi-segment forms (<c>A.B.C</c>) are skipped for the same reason and none
/// appear today.
/// </para>
/// </summary>
internal static class RefinementCodeSymbols
{
    /// <summary>
    /// The header of the column naming detecting tests rather than production
    /// code. Matched case-insensitively against the table's header cells.
    /// </summary>
    public const string DetectorHeader = "Detector";

    private static readonly Regex BacktickedSpan = new("`([^`]+)`", RegexOptions.Compiled);

    /// <summary>
    /// The production-column pattern. Both halves must begin upper-case (or an
    /// underscore on the right-hand side), which is what keeps the note's
    /// backticked file paths (<c>AtomicCommit.tla</c>,
    /// <c>spec/mutations/README.md</c>) from being read as claims about code.
    /// </summary>
    private static readonly Regex DottedReference = new(
        @"(?<![A-Za-z0-9_.])([A-Z][A-Za-z0-9_]*)\.([A-Z_][A-Za-z0-9_]*)(?![A-Za-z0-9_.])",
        RegexOptions.Compiled);

    /// <summary>
    /// The Detector-column pattern, identical except that the right-hand side
    /// may begin lower-case (issue #2561).
    /// <para>
    /// WHY THIS COLUMN GETS ITS OWN PATTERN. The upper-case requirement above
    /// exists to stop the production extractor manufacturing references out of
    /// the note's prose and file paths. The Detector column does not have that
    /// ambiguity: its cells are known to cite <c>Fixture.TestMethod</c> names
    /// and nothing else. Meanwhile this repository's test methods are
    /// conventionally lower_snake_case after the first word, so a rename to a
    /// name that begins lower-case is an ordinary, expected shape - and under
    /// the shared pattern such a name extracted to NOTHING and was therefore
    /// never resolved. That is the exact "a rename leaves the note behind"
    /// rot this gate exists to catch, passing on letter case alone.
    /// </para>
    /// <para>
    /// The two patterns fail in opposite directions, which is why they are
    /// tuned differently rather than unified. A false positive in the
    /// production columns is a gate crying wolf about correct prose, which
    /// gets the gate suppressed. A false positive here is a name that does not
    /// resolve, which is loud, points at one cell, and is one edit to fix. The
    /// cost is honest and worth stating: a backticked dotted file name written
    /// into a Detector cell now extracts and goes red. That is the safe
    /// direction, and no such cell exists.
    /// </para>
    /// </summary>
    private static readonly Regex DetectorReference = new(
        @"(?<![A-Za-z0-9_.])([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_.])",
        RegexOptions.Compiled);

    /// <summary>
    /// Extracts every distinct reference from the given tables, in document
    /// order. A reference repeated within one row is returned once; the same
    /// reference in two different rows is returned twice, because each row is
    /// a separate claim and a failure should name the row it came from.
    /// </summary>
    public static IReadOnlyList<RefinementCodeSymbol> Extract(IEnumerable<RefinementTable> tables) =>
        ExtractCore(tables, detectorColumnOnly: false);

    /// <summary>
    /// Extracts the dotted references from the <c>Detector</c> column only -
    /// the mirror image of <see cref="Extract"/>. These name TEST fixtures and
    /// methods, so they are resolved by
    /// <see cref="RefinementDetectorResolver"/> against <c>test/</c>, never by
    /// <see cref="RefinementSymbolResolver"/> against <c>src/</c>.
    /// </summary>
    public static IReadOnlyList<RefinementCodeSymbol> ExtractDetectors(IEnumerable<RefinementTable> tables) =>
        ExtractCore(tables, detectorColumnOnly: true);

    private static IReadOnlyList<RefinementCodeSymbol> ExtractCore(
        IEnumerable<RefinementTable> tables,
        bool detectorColumnOnly)
    {
        ArgumentNullException.ThrowIfNull(tables);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var symbols = new List<RefinementCodeSymbol>();
        var pattern = detectorColumnOnly ? DetectorReference : DottedReference;

        foreach (var table in tables)
        {
            var detectorColumn = -1;
            for (var i = 0; i < table.Headers.Count; i++)
            {
                if (string.Equals(table.Headers[i], DetectorHeader, StringComparison.OrdinalIgnoreCase))
                {
                    detectorColumn = i;
                    break;
                }
            }

            foreach (var row in table.Rows)
            {
                for (var column = 0; column < row.Cells.Count; column++)
                {
                    if (detectorColumnOnly ? column != detectorColumn : column == detectorColumn)
                    {
                        continue;
                    }

                    var cell = row.Cells[column];

                    foreach (Match span in BacktickedSpan.Matches(cell))
                    {
                        foreach (Match reference in pattern.Matches(span.Groups[1].Value))
                        {
                            var symbol = new RefinementCodeSymbol
                            {
                                Section = row.Section,
                                Row = row.Label,
                                LineNumber = row.LineNumber,
                                Text = reference.Value,
                                TypeName = reference.Groups[1].Value,
                                MemberName = reference.Groups[2].Value,
                            };

                            if (seen.Add($"{row.Section}|{row.Label}|{symbol.Text}"))
                            {
                                symbols.Add(symbol);
                            }
                        }
                    }
                }
            }
        }

        return symbols;
    }
}
