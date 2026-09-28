namespace Orleans.Lattice.Explorer.Tests.Shell.Design;

/// <summary>
/// The Shell's text contrast gate: every colour the Operate register draws text
/// in clears WCAG 2.2 AA (4.5:1) on every surface in both materials, and a full
/// step more (7:1) under the high-contrast overlay - re-derived from
/// <c>tokens.css</c> and <c>lattice-operate.css</c> as a browser would resolve
/// them, never asserted by eye.
/// </summary>
/// <remarks>
/// The method is the one <c>TextContrastTokenHygieneTests</c> established for
/// the old design system: resolve each palette through the cascade, measure
/// every foreground against every surface, and prove the measurement itself
/// against published WCAG values so the gate cannot pass vacuously.
/// </remarks>
[TestFixture]
public sealed class ShellTextContrastTests
{
    /// <summary>Every token the Shell draws text in, including all ten state roles.</summary>
    private static readonly string[] TextTokens =
    [
        "--lt-ink",
        "--lt-ink-2",
        "--lt-ink-3",
        "--lt-link",
        "--lt-info",
        "--lt-warning",
        "--lt-danger",
        "--lt-success",
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

    /// <summary>Text set on a filled surface rather than on the page: a pressed button, a pressed destructive button.</summary>
    private static readonly (string Foreground, string Background)[] FilledPairs =
    [
        ("--lt-marker-ink", "--lt-marker"),
        ("--lt-surface", "--lt-danger"),
        ("--lt-surface", "--lt-ink"),
    ];

    private static readonly string[] EmphasisLadder = ["--lt-ink", "--lt-ink-2", "--lt-ink-3"];

    [Test]
    [TestCase(ShellPalette.Paper)]
    [TestCase(ShellPalette.Board)]
    public void Every_text_token_clears_wcag_aa_on_every_surface(ShellPalette palette)
    {
        AssertEveryTextTokenClears(palette, ShellStylesheets.TextMinimum);
    }

    [Test]
    [TestCase(ShellPalette.PaperMore)]
    [TestCase(ShellPalette.BoardMore)]
    public void Every_text_token_clears_wcag_aaa_under_the_high_contrast_overlay(ShellPalette palette)
    {
        AssertEveryTextTokenClears(palette, ShellStylesheets.TextEnhancedMinimum);
    }

    [Test]
    [TestCase(ShellPalette.Paper)]
    [TestCase(ShellPalette.Board)]
    [TestCase(ShellPalette.PaperMore)]
    [TestCase(ShellPalette.BoardMore)]
    public void Text_on_filled_surfaces_clears_wcag_aa(ShellPalette palette)
    {
        var tokens = ShellStylesheets.Palette(palette);
        Assert.Multiple(() =>
        {
            foreach (var (foreground, background) in FilledPairs)
            {
                var ratio = ShellStylesheets.ContrastRatio(
                    ShellStylesheets.Colour(tokens, palette, foreground),
                    ShellStylesheets.Colour(tokens, palette, background));
                Assert.That(ratio, Is.GreaterThanOrEqualTo(ShellStylesheets.TextMinimum),
                    $"{palette}: {foreground} on {background} is {ShellStylesheets.Format(ratio)}");
            }
        });
    }

    [Test]
    [TestCase(ShellPalette.Paper)]
    [TestCase(ShellPalette.Board)]
    [TestCase(ShellPalette.PaperMore)]
    [TestCase(ShellPalette.BoardMore)]
    public void The_ink_steps_stay_a_descending_emphasis_ladder(ShellPalette palette)
    {
        // Raising a failing step until it passes is not a fix if it becomes
        // indistinguishable from the step above it.
        var tokens = ShellStylesheets.Palette(palette);
        var surface = ShellStylesheets.Colour(tokens, palette, "--lt-surface");

        for (var i = 1; i < EmphasisLadder.Length; i++)
        {
            var stronger = ShellStylesheets.ContrastRatio(ShellStylesheets.Colour(tokens, palette, EmphasisLadder[i - 1]), surface);
            var weaker = ShellStylesheets.ContrastRatio(ShellStylesheets.Colour(tokens, palette, EmphasisLadder[i]), surface);
            Assert.That(weaker, Is.LessThan(stronger),
                $"{palette}: {EmphasisLadder[i]} must read as less emphatic than {EmphasisLadder[i - 1]}");
        }
    }

    [Test]
    public void The_high_contrast_media_copies_match_the_attribute_overlays()
    {
        // The platform-driven copy and the reader-chosen copy must be the same
        // values, or "more contrast" means two different things.
        Assert.Multiple(() =>
        {
            Assert.That(
                ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.PaperMoreMediaSelector, ShellStylesheets.PrefersContrastQuery),
                Is.EquivalentTo(ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.PaperMoreSelector)),
                "the Paper prefers-contrast copy must equal the Paper data-lt-contrast overlay");
            Assert.That(
                ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.BoardMoreMediaSelector, ShellStylesheets.PrefersContrastQuery),
                Is.EquivalentTo(ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.BoardMoreSelector)),
                "the Board prefers-contrast copy must equal the Board data-lt-contrast overlay");
        });
    }

    [Test]
    public void The_contrast_calculation_agrees_with_the_wcag_reference_values()
    {
        // Battery test for the smoke detector: wrong luminance maths would make
        // every gate above pass while measuring nothing.
        Assert.Multiple(() =>
        {
            Assert.That(ShellStylesheets.ContrastRatio("#000000", "#ffffff"), Is.EqualTo(21.0).Within(0.0001));
            Assert.That(ShellStylesheets.ContrastRatio("#ffffff", "#ffffff"), Is.EqualTo(1.0).Within(0.0001));
            Assert.That(ShellStylesheets.ContrastRatio("#777777", "#ffffff"), Is.EqualTo(4.478).Within(0.001));
            Assert.That(ShellStylesheets.ContrastRatio("#777777", "#ffffff"), Is.LessThan(ShellStylesheets.TextMinimum));
            Assert.That(ShellStylesheets.ContrastRatio("#15191f", "#ffffff"), Is.EqualTo(17.6).Within(0.05),
                "the documentation site's ink on paper, as DESIGN.md records it");
        });
    }

    [Test]
    public void The_gate_rejects_an_ink_three_that_fails_on_the_current_row_band()
    {
        // The defect this gate caught while the tokens were being written: the
        // chalk marker at 14% over the slate left ink-3 at 4.42:1 on the current
        // row. It must stay a failure, or the band could creep back.
        Assert.That(ShellStylesheets.ContrastRatio("#8b978f", "#30301b"), Is.LessThan(ShellStylesheets.TextMinimum));
    }

    private static void AssertEveryTextTokenClears(ShellPalette palette, double minimum)
    {
        var tokens = ShellStylesheets.Palette(palette);
        var failures = new List<string>();
        var measured = 0;

        foreach (var text in TextTokens)
        {
            var colour = ShellStylesheets.Colour(tokens, palette, text);
            foreach (var surface in ShellStylesheets.Surfaces)
            {
                var ratio = ShellStylesheets.ContrastRatio(colour, ShellStylesheets.Colour(tokens, palette, surface));
                measured++;
                if (ratio < minimum)
                {
                    failures.Add($"{text} ({colour}) on {surface} is {ShellStylesheets.Format(ratio)}");
                }
            }
        }

        Assert.That(measured, Is.EqualTo(TextTokens.Length * ShellStylesheets.Surfaces.Length),
            "every text/surface pair must be measured");
        Assert.That(failures, Is.Empty,
            $"{palette} fails the {ShellStylesheets.Format(minimum)} text minimum:" + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }
}
