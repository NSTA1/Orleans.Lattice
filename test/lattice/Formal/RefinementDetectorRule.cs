namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The per-row rule behind the <c>Detector</c> column gate: a row that asserts
/// a production behaviour must name at least one test that resolves, whatever
/// verdict its cell declares. It was scoped to rows claiming detection until
/// #2557 widened it; see <see cref="MustNameAResolvableDetector"/>.
/// <para>
/// WHY THIS IS PER ROW AND NOT GLOBAL (issue #2561). The gate as first written
/// asked two questions, and a cell could evade both. It asked whether a cell
/// was non-empty, which prose satisfies, and it asked whether every extracted
/// name resolves, which a cell yielding zero names satisfies vacuously. The
/// fixture's global anti-vacuity guard did not notice, because the other rows
/// kept the extracted set non-empty. So a row could read
/// "Yes: covered by the registry fixture terminal-transition tests." - a claim
/// of detection naming nothing - with the whole suite green. That is the exact
/// failure mode the column was built to prevent: not the column being deleted,
/// but the column flattening into uniform reassurance.
/// </para>
/// <para>
/// Counting per row rather than globally also makes a rotted name loud rather
/// than silent. A name that fails to extract - because it was renamed into a
/// shape the extractor does not recognise - leaves its row with zero
/// detectors, which this rule reports, where previously it simply vanished
/// from the set being checked.
/// </para>
/// </summary>
internal static class RefinementDetectorRule
{
    /// <summary>
    /// The verdict prefix a cell uses to claim the row's behaviour is
    /// detected. The other verdicts in use are <c>None</c>, <c>Partial</c> and
    /// <c>Not applicable</c>.
    /// </summary>
    public const string DetectedVerdict = "Yes:";

    /// <summary>
    /// WIDENED BY #2557, and now unconditional: every behaviour-asserting row
    /// must name at least one resolvable test, whatever verdict it declares.
    /// The caller has already filtered out the non-behavioural rows, so every
    /// row reaching here is one the Detector column's question applies to.
    /// <para>
    /// #2561 left this scoped to <see cref="DetectedVerdict"/> cells and named
    /// #2557 as the issue that would widen it, on the reasoning that a
    /// <c>None</c> or <c>Partial</c> cell legitimately names no test. That
    /// reasoning held for <c>None</c>, which no row now declares. It did not
    /// hold for <c>Partial</c>, and the distinction is the point of the
    /// widening: a partial cell is not a cell that cites nothing, it is a cell
    /// that cites real coverage and then says what that coverage does not
    /// reach. Both open gap rows already cited a resolvable test while
    /// reporting <c>Partial</c>, so the widened rule was satisfiable before the
    /// gaps closed rather than only after - which is why this landed ahead of
    /// them instead of behind them.
    /// </para>
    /// <para>
    /// The predicate is kept rather than inlined so the column still has
    /// exactly one place that answers "which rows must cite a detector", and
    /// so a future narrowing has to be written down here rather than smuggled
    /// into a caller. It must not grow a second predicate beside it: two rules
    /// over one column is how the column ends up with two different answers
    /// about what it requires.
    /// </para>
    /// </summary>
    public static bool MustNameAResolvableDetector(string detectorCell)
    {
        ArgumentNullException.ThrowIfNull(detectorCell);
        return true;
    }

    /// <summary>
    /// The number of rows the rule actually examines: every row of the
    /// supplied tables that is not listed as non-behavioural. This is the
    /// denominator behind
    /// <see cref="BehaviourRowsWithoutAResolvableTest"/>, exposed so a
    /// caller can assert the rule read a corpus at all.
    /// <para>
    /// Anti-vacuity has to be asserted on this number rather than on the
    /// failure list, because an empty failure list is exactly what a passing
    /// gate and a gate that read nothing both produce. #2561 found the same
    /// shape one level down, where a cell yielding zero detector names
    /// satisfied "every extracted name resolves" vacuously.
    /// </para>
    /// </summary>
    public static int BehaviourRowsExamined(
        IEnumerable<RefinementTable> tables,
        IReadOnlyCollection<string> nonBehaviouralRows)
    {
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(nonBehaviouralRows);

        return tables
            .SelectMany(t => t.Rows)
            .Count(r => !nonBehaviouralRows.Contains(r.Label, StringComparer.Ordinal));
    }

    /// <summary>
    /// The index of the <c>Detector</c> column in a table, or -1 when the
    /// table has none. Matched on header text rather than position, so
    /// inserting a column ahead of it cannot silently point the gate at the
    /// wrong cells.
    /// </summary>
    public static int DetectorColumnOf(RefinementTable table)
    {
        ArgumentNullException.ThrowIfNull(table);

        for (var i = 0; i < table.Headers.Count; i++)
        {
            if (string.Equals(table.Headers[i], RefinementCodeSymbols.DetectorHeader, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// One message per behaviour-asserting row that is backed by no resolvable
    /// test, ready to be asserted empty.
    /// <para>
    /// Named for the rows it examines rather than for a verdict it no longer
    /// filters on. Before #2557 it considered only cells opening
    /// <see cref="DetectedVerdict"/>, and a name carrying that scope would now
    /// describe the method as checking less than it does - the drift this
    /// column exists to catch, in the gate itself.
    /// </para>
    /// </summary>
    /// <param name="tables">The behaviour-asserting tables to check.</param>
    /// <param name="nonBehaviouralRows">
    /// Row labels that assert no production behaviour and so are outside the
    /// question the column asks.
    /// </param>
    /// <param name="resolves">
    /// Whether an extracted detector reference names a test that exists.
    /// Injected rather than hard-wired so the rule can be exercised over
    /// hand-written markdown without indexing the repository.
    /// </param>
    public static IReadOnlyList<string> BehaviourRowsWithoutAResolvableTest(
        IEnumerable<RefinementTable> tables,
        IReadOnlyCollection<string> nonBehaviouralRows,
        Func<RefinementCodeSymbol, bool> resolves)
    {
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(nonBehaviouralRows);
        ArgumentNullException.ThrowIfNull(resolves);

        var tableList = tables.ToList();
        var detectors = RefinementCodeSymbols.ExtractDetectors(tableList);

        var named = detectors
            .GroupBy(d => RowKey(d.Section, d.Row), StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

        var failures = new List<string>();

        foreach (var table in tableList)
        {
            var column = DetectorColumnOf(table);

            foreach (var row in table.Rows)
            {
                if (nonBehaviouralRows.Contains(row.Label, StringComparer.Ordinal))
                {
                    continue;
                }

                var cell = column >= 0 && column < row.Cells.Count ? row.Cells[column] : string.Empty;

                if (!MustNameAResolvableDetector(cell))
                {
                    continue;
                }

                var cited = named.TryGetValue(RowKey(row.Section, row.Label), out var list)
                    ? list
                    : new List<RefinementCodeSymbol>();

                if (cited.Any(resolves))
                {
                    continue;
                }

                failures.Add(Describe(row, cited));
            }
        }

        return failures;
    }

    private static string RowKey(string section, string label) => $"{section}|{label}";

    private static string Describe(RefinementRow row, IReadOnlyList<RefinementCodeSymbol> cited) =>
        cited.Count == 0
            ? $"spec/Refinement.md line {row.LineNumber}: row {row.Label} asserts a production "
              + "behaviour but its Detector cell names no test at all. Every behaviour-asserting "
              + "row has to cite a backticked `Fixture.TestMethod` name, whatever verdict it "
              + $"declares. A '{DetectedVerdict}' cell that cites nothing is prose that nothing "
              + "can falsify. A 'Partial' or 'None' cell still has to name the coverage that does "
              + "exist and cite the issue for the part that does not; if there is genuinely no "
              + "test at all, the row is a gap that has not been written down yet."
            : $"spec/Refinement.md line {row.LineNumber}: row {row.Label} asserts a production "
              + "behaviour but none of the tests its Detector cell names resolves: "
              + string.Join(", ", cited.Select(c => $"'{c.Text}'"))
              + ". The row was probably left behind by a rename.";
}
