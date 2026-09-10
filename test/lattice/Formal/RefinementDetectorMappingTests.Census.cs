using NUnit.Framework;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Keeps a hand-maintained census count out of the prose of
/// <c>spec/Refinement.md</c> (issue #2560).
/// <para>
/// The sibling gates in this fixture check the note's structure. This one
/// checks the note's summary *of* that structure, which is where #2560 lived:
/// a quantitative claim about the Detector column, sitting inside the note,
/// that no gate evaluated and that was false. Correcting the figure would have
/// closed the instance and left the class open, because the note is precisely
/// the document whose counts change as #2551-#2554 land. The figure was
/// removed and the derivation method written down in its place; this fixture
/// is what stops the figure coming back.
/// </para>
/// <para>
/// The two hand-written cases below are not decoration. A detector that is
/// never shown to fire is indistinguishable from one that cannot, which is the
/// failure mode the parent audit (#2299) exists to find, so the positive case
/// pins the exact sentences that were wrong and the negative case pins the
/// real surviving prose that must not trip it.
/// </para>
/// </summary>
internal sealed partial class RefinementDetectorMappingTests
{
    [Test]
    public void The_note_records_no_hand_maintained_census_count()
    {
        var claims = RefinementCensusProse.FindCountClaims(RefinementNote.ReadText());

        var offenders = claims
            .Select(c =>
                $"spec/Refinement.md line {c.LineNumber} states a hand-maintained census count "
                + $"('{c.Text}'): {c.Line}")
            .ToList();

        Assert.That(
            offenders,
            Is.Empty,
            string.Join(Environment.NewLine, offenders)
            + Environment.NewLine
            + "The note must not state how many rows are detected, partial or undetected. "
            + "Those tallies change every time one of the gap issues lands, so a figure "
            + "written here is false again within days - which is exactly how #2560 arose. "
            + "State the method of re-deriving the census instead, as the 'The Detector "
            + "column' section does. If a count genuinely must be kept it has to be derived "
            + "from the table and asserted by a test, never written into the prose.");
    }

    [Test]
    public void The_census_count_detector_fires_on_the_claims_it_was_written_for()
    {
        // Anti-vacuity, and the negative experiment made permanent. Every
        // string here is a real census claim: the first two are the exact
        // wordings #2560 removed from the note and from the comment on
        // At_least_one_row_reports_a_gap, and the rest are the shapes a
        // well-meaning author would reach for next, including the "corrected"
        // figures that would themselves be stale once #2551-#2554 land.
        string[] claims =
        [
            "The census found ten rows detected, three partial or undetected.",
            "The census found 10 detected, 2 partial, 1 undetected.",
            "The census found nine rows detected, four partial or undetected.",
            "So the census is 9 detected and 4 gaps.",
            "Four gaps remain open.",
            "There are 3 partial detectors.",
        ];

        Assert.Multiple(() =>
        {
            foreach (var claim in claims)
            {
                Assert.That(
                    RefinementCensusProse.FindCountClaims(claim),
                    Is.Not.Empty,
                    $"The census-count detector did not fire on '{claim}', so it would not have "
                    + "caught #2560 and is not guarding what this fixture claims it guards.");
            }
        });
    }

    [Test]
    public void The_census_count_detector_ignores_the_prose_the_note_legitimately_carries()
    {
        // Every string here is either real surviving prose from the note or a
        // claim that is legitimate because it is gated elsewhere. A guard that
        // fires on these would be removed by the first author it obstructed,
        // so the absence of false positives is part of what makes it durable.
        string[] permitted =
        [
            // Real prose from the note's "The Detector column" section.
            "Three decisions shaped that column, recorded here because each was a real fork.",
            "Four options were weighed.",
            // The denominator is legal because it is derived and gated by
            // The_note_yields_the_expected_behaviour_asserting_denominator.
            "The note has 13 behaviour-asserting rows.",
            // Issue references are not numerals.
            "The gaps are filed as #2551, #2552, #2553 and #2554.",
            "These are being extracted across level-C Phases 1-4.",
            // A Detector cell may summarise its own row; it is the thing a
            // census summarises, not a summary of one.
            "| `ShadowForwardOrphan(t,k)` | role | code | Partial: only one of the two "
                + "production paths this row names is detected. Gap filed as #2554. |",
            // Fenced code is skipped, so a worked re-derivation may quote a result.
            "```text\nten rows detected\n```",
        ];

        Assert.Multiple(() =>
        {
            foreach (var line in permitted)
            {
                Assert.That(
                    RefinementCensusProse.FindCountClaims(line),
                    Is.Empty,
                    $"The census-count detector fired on legitimate prose: '{line}'.");
            }
        });
    }
}
