namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// The Shell's non-text contrast gate (WCAG 2.2 SC 1.4.11): every boundary a
/// reader must perceive to find a control, the focus ring, the ring that pairs
/// with the marker, the switch track, and every state glyph clear 3:1 on every
/// surface in both materials, and 4.5:1 under the high-contrast overlay.
/// </summary>
/// <remarks>
/// It also measures the reason the Marker Is Never Alone Rule exists: the paper
/// marker is too pale to be a state on its own, so its ring must carry it.
/// </remarks>
[TestFixture]
public sealed class ShellNonTextContrastTests
{
    /// <summary>Every token drawn as a boundary, ring, indicator or glyph.</summary>
    private static readonly string[] IndicatorTokens =
    [
        "--lt-op-control-border",
        "--lt-op-focus-ring-color",
        "--lt-op-selected-ring",
        "--lt-op-switch-on",
        "--lt-op-state-installed",
        "--lt-op-state-enabled",
        "--lt-op-state-disabled",
        "--lt-op-state-uninstalled",
        "--lt-op-state-drift",
        "--lt-op-state-healthy",
        "--lt-op-state-lagging",
        "--lt-op-state-stalled",
        "--lt-op-state-failed",
        "--lt-op-state-unknown",
    ];

    [Test]
    [TestCase(ShellPalette.Paper)]
    [TestCase(ShellPalette.Board)]
    public void Every_indicator_clears_the_non_text_minimum_on_every_surface(ShellPalette palette)
    {
        AssertEveryIndicatorClears(palette, ShellStylesheets.NonTextMinimum);
    }

    [Test]
    [TestCase(ShellPalette.PaperMore)]
    [TestCase(ShellPalette.BoardMore)]
    public void Every_indicator_clears_the_enhanced_minimum_under_high_contrast(ShellPalette palette)
    {
        AssertEveryIndicatorClears(palette, ShellStylesheets.NonTextEnhancedMinimum);
    }

    [Test]
    [TestCase(ShellPalette.Paper)]
    [TestCase(ShellPalette.Board)]
    [TestCase(ShellPalette.PaperMore)]
    [TestCase(ShellPalette.BoardMore)]
    public void A_switch_thumb_is_distinguishable_from_its_track_in_both_states(ShellPalette palette)
    {
        var tokens = ShellStylesheets.Palette(palette);
        Assert.Multiple(() =>
        {
            var off = ShellStylesheets.ContrastRatio(
                ShellStylesheets.Colour(tokens, palette, "--lt-op-control-border"),
                ShellStylesheets.Colour(tokens, palette, "--lt-surface-sunken"));
            Assert.That(off, Is.GreaterThanOrEqualTo(ShellStylesheets.NonTextMinimum), $"{palette}: off thumb on track");

            var on = ShellStylesheets.ContrastRatio(
                ShellStylesheets.Colour(tokens, palette, "--lt-surface"),
                ShellStylesheets.Colour(tokens, palette, "--lt-op-switch-on"));
            Assert.That(on, Is.GreaterThanOrEqualTo(ShellStylesheets.NonTextMinimum), $"{palette}: on thumb on track");
        });
    }

    [Test]
    public void The_paper_marker_cannot_carry_a_state_alone_so_its_ring_must()
    {
        var paper = ShellStylesheets.Palette(ShellPalette.Paper);
        var surface = ShellStylesheets.Colour(paper, ShellPalette.Paper, "--lt-surface");

        Assert.Multiple(() =>
        {
            Assert.That(
                ShellStylesheets.ContrastRatio(ShellStylesheets.Colour(paper, ShellPalette.Paper, "--lt-marker"), surface),
                Is.LessThan(ShellStylesheets.NonTextMinimum),
                "if the paper marker ever cleared 3:1 this test is stale; until then it proves the rule is needed");
            Assert.That(
                ShellStylesheets.ContrastRatio(ShellStylesheets.Colour(paper, ShellPalette.Paper, "--lt-op-selected-ring"), surface),
                Is.GreaterThanOrEqualTo(ShellStylesheets.NonTextMinimum),
                "the ring that pairs with the marker must itself be perceivable");
        });
    }

    [Test]
    public void The_focus_ring_is_at_least_two_pixels_wide()
    {
        var paper = ShellStylesheets.Palette(ShellPalette.Paper);
        Assert.Multiple(() =>
        {
            Assert.That(ShellStylesheets.Pixels(paper["--lt-op-focus-ring-width"]), Is.GreaterThanOrEqualTo(2));
            Assert.That(ShellStylesheets.Pixels(paper["--lt-op-focus-ring-offset"]), Is.GreaterThanOrEqualTo(1));
        });
    }

    [Test]
    public void Compact_rows_and_controls_keep_the_minimum_target_size()
    {
        // WCAG 2.2 SC 2.5.8: 24 by 24 CSS pixels. Compact is the densest the
        // Explorer sets, so it is the one that has to clear it.
        var paper = ShellStylesheets.Palette(ShellPalette.Paper);
        Assert.Multiple(() =>
        {
            Assert.That(ShellStylesheets.Pixels(paper["--lt-op-row-height-compact"]), Is.GreaterThanOrEqualTo(24));
            Assert.That(ShellStylesheets.Pixels(paper["--lt-op-control-height-compact"]), Is.GreaterThanOrEqualTo(24));
            Assert.That(
                ShellStylesheets.Pixels(paper["--lt-op-row-height-compact"]),
                Is.LessThan(ShellStylesheets.Pixels(paper["--lt-op-row-height-comfortable"])),
                "compact must actually be denser than comfortable");
        });
    }

    [Test]
    public void Comfortable_rows_and_controls_are_touch_targets()
    {
        // The responsive contract (epic #3807): in the default density every row
        // and control is at least 44 by 44 CSS pixels, so the Explorer reads and
        // acts on a phone without a denser mode.
        var paper = ShellStylesheets.Palette(ShellPalette.Paper);
        Assert.Multiple(() =>
        {
            Assert.That(ShellStylesheets.Pixels(paper["--lt-op-row-height-comfortable"]), Is.GreaterThanOrEqualTo(44));
            Assert.That(ShellStylesheets.Pixels(paper["--lt-op-control-height-comfortable"]), Is.GreaterThanOrEqualTo(44));
            Assert.That(paper["--lt-op-row-height"], Is.EqualTo(paper["--lt-op-row-height-comfortable"]), "comfortable is the default");
            Assert.That(paper["--lt-op-control-height"], Is.EqualTo(paper["--lt-op-control-height-comfortable"]), "comfortable is the default");
        });
    }

    [Test]
    public void Compact_density_rebinds_every_density_token()
    {
        var compact = ShellStylesheets.Block(ShellStylesheets.Operate, "[data-lt-density=\"compact\"]");
        Assert.That(compact, Is.EquivalentTo(new Dictionary<string, string>
        {
            ["--lt-op-row-height"] = "var(--lt-op-row-height-compact)",
            ["--lt-op-control-height"] = "var(--lt-op-control-height-compact)",
            ["--lt-op-cell-padding-x"] = "var(--lt-op-cell-padding-x-compact)",
        }));
    }

    private static void AssertEveryIndicatorClears(ShellPalette palette, double minimum)
    {
        var tokens = ShellStylesheets.Palette(palette);
        var failures = new List<string>();
        var measured = 0;

        foreach (var indicator in IndicatorTokens)
        {
            var colour = ShellStylesheets.Colour(tokens, palette, indicator);
            foreach (var surface in ShellStylesheets.Surfaces)
            {
                var ratio = ShellStylesheets.ContrastRatio(colour, ShellStylesheets.Colour(tokens, palette, surface));
                measured++;
                if (ratio < minimum)
                {
                    failures.Add($"{indicator} ({colour}) on {surface} is {ShellStylesheets.Format(ratio)}");
                }
            }
        }

        Assert.That(measured, Is.EqualTo(IndicatorTokens.Length * ShellStylesheets.Surfaces.Length));
        Assert.That(failures, Is.Empty,
            $"{palette} fails the {ShellStylesheets.Format(minimum)} non-text minimum:" + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }
}
