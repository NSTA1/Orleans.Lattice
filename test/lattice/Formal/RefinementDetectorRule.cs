namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The per-row rule behind the <c>Detector</c> column gate: a row that claims
/// its behaviour is detected must name at least one test that resolves.
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
    /// THE ONE CONDITION #2557 WIDENS. Scoped today to cells claiming
    /// <see cref="DetectedVerdict"/>, because a <c>None</c> or <c>Partial</c>
    /// cell legitimately names no test - that is what a gap row IS, and gaps
    /// are open right now (#2552, #2554). Demanding a resolvable detector from
    /// every behaviour-asserting row is therefore not implementable until
    /// those close, and it is #2557's job to demand it once they have.
    /// <para>
    /// When #2557 lands, it replaces the body of this method with
    /// <c>true</c> - every row reaching here already had the non-behavioural
    /// rows filtered out by the caller - and everything downstream tightens
    /// with it. It does not need, and must not grow, a second predicate
    /// alongside this one: two rules over one column is how the column ends up
    /// with two different answers about what it requires.
    /// </para>
    /// </summary>
    public static bool MustNameAResolvableDetector(string detectorCell)
    {
        ArgumentNullException.ThrowIfNull(detectorCell);
        return detectorCell.TrimStart().StartsWith(DetectedVerdict, StringComparison.Ordinal);
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
    /// One message per row that claims detection but is backed by no
    /// resolvable test, ready to be asserted empty.
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
    public static IReadOnlyList<string> RowsClaimingDetectionWithoutAResolvableTest(
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
            ? $"spec/Refinement.md line {row.LineNumber}: row {row.Label} claims detection "
              + $"('{DetectedVerdict}') but its Detector cell names no test at all. A verdict of "
              + $"'{DetectedVerdict}' has to cite a backticked `Fixture.TestMethod` name, or the "
              + "claim is prose that nothing can falsify. If the behaviour is genuinely undetected, "
              + "say 'None' or 'Partial' and cite the gap issue instead."
            : $"spec/Refinement.md line {row.LineNumber}: row {row.Label} claims detection "
              + $"('{DetectedVerdict}') but none of the tests it names resolves: "
              + string.Join(", ", cited.Select(c => $"'{c.Text}'"))
              + ". The row was probably left behind by a rename.";
}
