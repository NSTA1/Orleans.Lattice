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
/// <c>Type.Member</c> forms whose both halves begin with an upper-case letter
/// (or an underscore on the right-hand side) are taken. That rule is chosen to
/// be conservative in one specific direction: it must not manufacture
/// references that were never claims about code, because a staleness gate that
/// cries wolf is suppressed within a week and is then worse than no gate at
/// all.
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
/// The cost of that narrowing is honest and worth stating: a rename of a
/// symbol the note names only in bare form (<c>ExecutePhaseAsync</c>,
/// <c>AppendTxTerminalAsync</c>, <c>TxDecisionView</c>) is not caught here.
/// Multi-segment forms (<c>A.B.C</c>) are skipped for the same reason and none
/// appear today.
/// </para>
/// </summary>
internal static class RefinementCodeSymbols
{
    private static readonly Regex BacktickedSpan = new("`([^`]+)`", RegexOptions.Compiled);

    private static readonly Regex DottedReference = new(
        @"(?<![A-Za-z0-9_.])([A-Z][A-Za-z0-9_]*)\.([A-Z_][A-Za-z0-9_]*)(?![A-Za-z0-9_.])",
        RegexOptions.Compiled);

    /// <summary>
    /// Extracts every distinct reference from the given tables, in document
    /// order. A reference repeated within one row is returned once; the same
    /// reference in two different rows is returned twice, because each row is
    /// a separate claim and a failure should name the row it came from.
    /// </summary>
    public static IReadOnlyList<RefinementCodeSymbol> Extract(IEnumerable<RefinementTable> tables)
    {
        ArgumentNullException.ThrowIfNull(tables);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var symbols = new List<RefinementCodeSymbol>();

        foreach (var table in tables)
        {
            foreach (var row in table.Rows)
            {
                foreach (var cell in row.Cells)
                {
                    foreach (Match span in BacktickedSpan.Matches(cell))
                    {
                        foreach (Match reference in DottedReference.Matches(span.Groups[1].Value))
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
