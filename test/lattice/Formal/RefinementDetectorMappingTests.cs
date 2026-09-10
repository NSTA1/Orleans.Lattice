using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Gates the <c>Detector</c> column of <c>spec/Refinement.md</c> (issue #2527).
/// <para>
/// WHAT THIS PROVES, AND WHAT IT DOES NOT. It proves the column cannot rot into
/// prose: every behaviour-asserting row declares a verdict, every test a row
/// names still exists, and every row admitting a gap cites an issue. It does
/// NOT prove a named test is a *good* detector - only a human reading each
/// test against the row it answers can claim that, and only a human re-doing
/// that reading can revise the claim. Saying so plainly matters, because a
/// gate whose reach is overstated is the exact defect the parent audit (#2299)
/// keeps finding.
/// </para>
/// <para>
/// WHY THE DETECTOR RESOLVER IS SEPARATE FROM THE SYMBOL RESOLVER. The other
/// columns name production code and resolve against <c>src/</c>; this column
/// names test fixtures and resolves against <c>test/</c>. Sending either set to
/// the other resolver fails every entry, so
/// <see cref="RefinementCodeSymbols.ExtractDetectors"/> and
/// <see cref="RefinementCodeSymbols.Extract"/> partition the table by header.
/// </para>
/// </summary>
[TestFixture]
internal sealed partial class RefinementDetectorMappingTests
{
    /// <summary>
    /// Rows that assert no production behaviour, and so are outside the
    /// question the Detector column asks. Kept as an explicit list rather than
    /// inferred, so adding a row cannot silently opt itself out.
    /// </summary>
    private static readonly string[] NonBehaviouralRows = ["`Stutter`"];

    private static IReadOnlyList<RefinementTable> BehaviourTables()
    {
        var tables = RefinementNote.ReadTables();
        return [tables[RefinementNote.ActionSection], tables[RefinementNote.PropertySection]];
    }

    private static (RefinementRow Row, string Detector) DetectorOf(RefinementTable table, RefinementRow row)
    {
        var column = -1;
        for (var i = 0; i < table.Headers.Count; i++)
        {
            if (string.Equals(table.Headers[i], RefinementCodeSymbols.DetectorHeader, StringComparison.OrdinalIgnoreCase))
            {
                column = i;
                break;
            }
        }

        var cell = column >= 0 && column < row.Cells.Count ? row.Cells[column] : string.Empty;
        return (row, cell);
    }

    private static List<(RefinementRow Row, string Detector)> BehaviourRows()
    {
        var result = new List<(RefinementRow, string)>();

        foreach (var table in BehaviourTables())
        {
            foreach (var row in table.Rows)
            {
                if (NonBehaviouralRows.Contains(row.Label, StringComparer.Ordinal))
                {
                    continue;
                }

                result.Add(DetectorOf(table, row));
            }
        }

        return result;
    }

    [Test]
    public void The_note_yields_the_expected_behaviour_asserting_denominator()
    {
        // Anti-vacuity, and a standing check on the census's own arithmetic.
        // The note states 13 behaviour-asserting rows (8 property, 5 action);
        // if a row is added or removed, this fails and the prose must be
        // re-derived rather than quietly drifting out of date.
        Assert.That(BehaviourRows(), Has.Count.EqualTo(13));
    }

    [Test]
    public void Every_behaviour_asserting_row_declares_a_detector()
    {
        var missing = BehaviourRows()
            .Where(r => string.IsNullOrWhiteSpace(r.Detector))
            .Select(r => $"line {r.Row.LineNumber}: row {r.Row.Label} has an empty Detector cell")
            .ToList();

        Assert.That(missing, Is.Empty, string.Join(Environment.NewLine, missing));
    }

    [Test]
    public void Every_test_named_as_a_detector_exists()
    {
        var resolver = RefinementDetectorResolver.ForRepository();
        var detectors = RefinementCodeSymbols.ExtractDetectors(BehaviourTables());

        Assert.That(
            detectors,
            Is.Not.Empty,
            "No detector references were extracted at all, which means this gate is checking nothing. " +
            "Either the Detector column was removed or its header was renamed.");

        var broken = detectors
            .Where(d => !resolver.TestExists(d.TypeName, d.MemberName))
            .Select(d =>
                $"spec/Refinement.md line {d.LineNumber} (row {d.Row}) names detector '{d.Text}', " +
                $"which no longer resolves: {resolver.Explain(d.TypeName, d.MemberName)}")
            .ToList();

        Assert.That(broken, Is.Empty, string.Join(Environment.NewLine + Environment.NewLine, broken));
    }

    [Test]
    public void Every_row_reporting_a_gap_cites_an_issue()
    {
        // A row admitting "None" or "Partial" coverage is the valuable output
        // of the census (#2527 calls filing those issues "the point of the
        // exercise"). An admitted gap with no issue behind it is a finding that
        // will be forgotten, so the citation is mandatory.
        var uncited = BehaviourRows()
            .Where(r => r.Detector.Contains("None", StringComparison.Ordinal)
                     || r.Detector.Contains("Partial", StringComparison.Ordinal))
            .Where(r => !System.Text.RegularExpressions.Regex.IsMatch(r.Detector, @"#\d{3,}"))
            .Select(r => $"line {r.Row.LineNumber}: row {r.Row.Label} reports a gap but cites no issue")
            .ToList();

        Assert.That(uncited, Is.Empty, string.Join(Environment.NewLine, uncited));
    }

    [Test]
    public void At_least_one_row_reports_a_gap()
    {
        // A floor, not a census: it asserts only that the Detector column is
        // not uniformly reassuring. No tally is recorded in this comment, on
        // purpose. The tallies move every time one of the gap issues lands, so
        // a figure written here rots exactly as the note's own census
        // paragraph did - that comment and that paragraph were both wrong, and
        // no gate evaluated either (#2560). Re-derive from the Detector column
        // instead; the method is stated in the note's "The Detector column"
        // section. If this ever reports zero gaps, either the gaps were
        // genuinely closed - in which case #2557 replaces this floor with the
        // stronger assertion that every behaviour-asserting row cites a
        // resolvable test - or, far more likely, the column was flattened into
        // uniform reassurance. #2527 exists because the second failure mode is
        // the one that actually happens.
        var gaps = BehaviourRows()
            .Count(r => r.Detector.Contains("None", StringComparison.Ordinal)
                     || r.Detector.Contains("Partial", StringComparison.Ordinal));

        Assert.That(gaps, Is.GreaterThan(0));
    }

    [Test]
    public void Production_symbols_and_detector_tests_are_partitioned_by_column()
    {
        // The two extractors must not overlap: a test name resolved against
        // src/ fails, and a production symbol resolved against test/ fails.
        // This is the regression that guards that partition.
        var tables = BehaviourTables();
        var production = RefinementCodeSymbols.Extract(tables).Select(s => s.Text).ToHashSet(StringComparer.Ordinal);
        var detectors = RefinementCodeSymbols.ExtractDetectors(tables).Select(s => s.Text).ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(production, Is.Not.Empty);
            Assert.That(detectors, Is.Not.Empty);
            Assert.That(
                production.Intersect(detectors, StringComparer.Ordinal),
                Is.Empty,
                "A reference appears in both the production columns and the Detector column.");
        });
    }
}
