namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The design system's text contrast gate (issues #1801 and #1846): every
/// foreground colour token clears the WCAG 2.1 AA minimum against every
/// background colour token in its own palette, measured rather than asserted by
/// eye, and no palette may quietly give back a ratio it once held.
/// </summary>
/// <remarks>
/// <para>
/// The defect this guard exists for was found by the axe sweep in the browser
/// lane: the dark palette's <c>--lx-color-text-dim</c> was <c>#5a6373</c>, which
/// is 3.19:1 on the <c>#0b0e14</c> canvas against a 4.5:1 requirement. Two
/// things about how it was found are the reason this test is here and not only
/// there.
/// </para>
/// <list type="number">
/// <item>The browser lane is advisory and path-filtered, so it does not run on
/// most pull requests. This fixture needs no browser, so it runs in the required
/// build-and-test check on every one of them.</item>
/// <item>The sweep only ever sees the default (dark) theme in whatever states it
/// happens to visit. It could not have found the same defect in the light
/// palette, whose dim token was a worse 2.91:1. Reading the tokens directly
/// covers every palette exhaustively instead of sampling one.</item>
/// </list>
/// <para>
/// The bar is the 4.5:1 for normal-size text rather than the relaxed 3:1 for
/// large text, because these tokens are spent at the body and caption sizes of
/// the type scale, all of which sit below the 18.66px / 14pt-bold threshold that
/// the relaxed allowance requires.
/// </para>
/// <para>
/// Issue #1846 widened the scope in two directions. The accent and status tokens
/// used to be excluded on the grounds that they are spent as fills as often as
/// as text; they are now held to the text bar as well, because they are
/// unambiguously spent as foregrounds - a link, a status label - and the retune
/// moved them far enough to clear it. Their other role, as a fill with
/// <c>--lx-color-accent-contrast</c> drawn on top, is measured by
/// <see cref="NonTextContrastTokenHygieneTests"/>.
/// </para>
/// <para>
/// The second direction is <see cref="No_palette_gives_back_a_recorded_canvas_ratio"/>.
/// Contrast against a fixed canvas is zero-sum, so a later issue tasked with
/// raising border contrast could meet its own bar by darkening the text that
/// sits on the same surfaces. Pinning the ratios each palette held before #1846
/// makes that trade fail loudly instead of passing silently.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TextContrastTokenHygieneTests
{
    /// <summary>
    /// The foreground tokens, ordered from most to least emphatic. The ladder
    /// test below depends on that order.
    /// </summary>
    private static readonly string[] EmphasisLadder =
    [
        "--lx-color-text",
        "--lx-color-text-muted",
        "--lx-color-text-dim",
    ];

    /// <summary>
    /// Every token the Explorer draws as a foreground. All of them are held to
    /// the same bar, on every background in the palette.
    /// </summary>
    private static readonly string[] ForegroundTokens =
    [
        "--lx-color-text",
        "--lx-color-text-muted",
        "--lx-color-text-dim",
        "--lx-color-state-selected-fg",
        "--lx-color-state-rest-fg",
        "--lx-color-accent",
        "--lx-color-accent-hover",
        "--lx-color-success",
        "--lx-color-warning",
        "--lx-color-danger",
    ];

    /// <summary>
    /// What each palette's canvas ratios measured before issue #1846 retuned
    /// them. These are floors, not targets: a retune may improve them and may
    /// never give one back.
    /// </summary>
    private static readonly (string Palette, string Token, double Recorded)[] RecordedCanvasRatios =
    [
        (DesignTokenPalettes.Dark, "--lx-color-text", 14.30),
        (DesignTokenPalettes.Dark, "--lx-color-text-muted", 6.24),
        (DesignTokenPalettes.Dark, "--lx-color-text-dim", 5.31),
        (DesignTokenPalettes.Light, "--lx-color-text", 14.84),
        (DesignTokenPalettes.Light, "--lx-color-text-muted", 5.63),
        (DesignTokenPalettes.Light, "--lx-color-text-dim", 4.79),
    ];

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    public void Every_foreground_token_clears_wcag_aa_on_every_surface(string paletteName)
    {
        AssertEveryForegroundClears(paletteName, DesignTokens.TextMinimum);
    }

    [Test]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void Every_high_contrast_foreground_token_clears_wcag_aaa_on_every_surface(string paletteName)
    {
        // A reader who asks for more contrast gets a full step more, not the
        // same palette under a different name.
        AssertEveryForegroundClears(paletteName, DesignTokens.TextEnhancedMinimum);
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void Every_palette_keeps_its_text_tokens_in_a_descending_emphasis_ladder(string paletteName)
    {
        // Raising a failing token until the number passes is not on its own a
        // fix: setting dim equal to the primary text colour would clear the bar
        // above while destroying the only thing the token exists to express.
        // Measured against its own canvas, each token must be strictly less
        // emphatic than the one before it.
        var palette = DesignTokenPalettes.Resolve(paletteName);
        var canvas = DesignTokens.Colour(palette, paletteName, "--lx-color-canvas");

        for (var i = 1; i < EmphasisLadder.Length; i++)
        {
            var stronger = DesignTokens.ContrastRatio(
                DesignTokens.Colour(palette, paletteName, EmphasisLadder[i - 1]), canvas);
            var weaker = DesignTokens.ContrastRatio(
                DesignTokens.Colour(palette, paletteName, EmphasisLadder[i]), canvas);

            Assert.That(weaker, Is.LessThan(stronger),
                $"{paletteName}: {EmphasisLadder[i]} ({DesignTokens.Format(weaker)}) must read as less "
                + $"emphatic than {EmphasisLadder[i - 1]} ({DesignTokens.Format(stronger)}) on the canvas. "
                + "A token that clears the contrast bar by becoming indistinguishable from the one above "
                + "it has not been fixed.");
        }
    }

    [Test]
    public void No_palette_gives_back_a_recorded_canvas_ratio()
    {
        // Contrast against a fixed canvas is zero-sum. Without this, an issue
        // tasked with raising non-text contrast could meet its bar by spending
        // text contrast, and every other gate here would still be green.
        Assert.Multiple(() =>
        {
            foreach (var (paletteName, token, recorded) in RecordedCanvasRatios)
            {
                var palette = DesignTokenPalettes.Resolve(paletteName);
                var measured = DesignTokens.ContrastRatio(
                    DesignTokens.Colour(palette, paletteName, token),
                    DesignTokens.Colour(palette, paletteName, "--lx-color-canvas"));

                // Compared at the two decimal places the ratios were recorded
                // to, so the floor is the published number rather than whatever
                // binary floating point makes of it.
                Assert.That(Math.Round(measured, 2), Is.GreaterThanOrEqualTo(recorded),
                    $"{paletteName}: {token} now measures {DesignTokens.Format(measured)} on the canvas, "
                    + $"below the {DesignTokens.Format(recorded)} it held before issue #1846. Text contrast "
                    + "is not a budget to spend on borders.");
            }
        });
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void Every_palette_resolves_every_measured_token(string paletteName)
    {
        // Without this, a token dropped or misspelled in one palette would make
        // that palette's gate quietly measure fewer pairs rather than fail.
        var palette = DesignTokenPalettes.Resolve(paletteName);

        Assert.Multiple(() =>
        {
            foreach (var token in ForegroundTokens.Concat(DesignTokens.BackgroundTokens))
            {
                Assert.That(palette.ContainsKey(token), Is.True,
                    $"{paletteName} must resolve {token}");
            }
        });
    }

    [Test]
    public void The_contrast_calculation_agrees_with_the_wcag_reference_values()
    {
        // Battery test for the smoke detector. If the luminance maths is ever
        // wrong, the gates above pass vacuously and report a compliance they
        // never measured, so pin it to values published with the specification.
        Assert.Multiple(() =>
        {
            Assert.That(DesignTokens.ContrastRatio("#000000", "#ffffff"), Is.EqualTo(21.0).Within(0.0001),
                "black on white is the maximum ratio the formula can produce");
            Assert.That(DesignTokens.ContrastRatio("#ffffff", "#ffffff"), Is.EqualTo(1.0).Within(0.0001),
                "a colour against itself is the minimum ratio");

            // The canonical worked example: #777777 on white is the greyscale
            // value that sits just under the AA bar.
            Assert.That(DesignTokens.ContrastRatio("#777777", "#ffffff"), Is.EqualTo(4.478).Within(0.001));
            Assert.That(DesignTokens.ContrastRatio("#777777", "#ffffff"),
                Is.LessThan(DesignTokens.TextMinimum));

            // The ratio is symmetric: which colour is the foreground does not
            // change the measurement.
            Assert.That(DesignTokens.ContrastRatio("#0b0e14", "#7c8797"),
                Is.EqualTo(DesignTokens.ContrastRatio("#7c8797", "#0b0e14")).Within(0.0001));
        });
    }

    [Test]
    public void The_guard_rejects_the_exact_contrast_defect_reported_in_1801()
    {
        // The retired values, kept here as the regression this fixture was
        // written for. Both are literals on purpose: they must stay measurable
        // after the stylesheet stops declaring them.
        Assert.Multiple(() =>
        {
            Assert.That(DesignTokens.ContrastRatio("#5a6373", "#0b0e14"), Is.EqualTo(3.19).Within(0.01),
                "the dark palette's retired dim token, as measured on the issue");
            Assert.That(DesignTokens.ContrastRatio("#5a6373", "#0b0e14"),
                Is.LessThan(DesignTokens.TextMinimum),
                "and it must be judged a failure, or this fixture guards nothing");

            Assert.That(DesignTokens.ContrastRatio("#8a93a3", "#f6f8fb"), Is.EqualTo(2.91).Within(0.01),
                "the light palette's retired dim token, a worse instance of the same defect");
            Assert.That(DesignTokens.ContrastRatio("#8a93a3", "#f6f8fb"),
                Is.LessThan(DesignTokens.TextMinimum));
        });
    }

    private static void AssertEveryForegroundClears(string paletteName, double minimum)
    {
        var palette = DesignTokenPalettes.Resolve(paletteName);

        var failures = new List<string>();
        var measured = 0;

        foreach (var foreground in ForegroundTokens)
        {
            var colour = DesignTokens.Colour(palette, paletteName, foreground);

            foreach (var background in DesignTokens.BackgroundTokens)
            {
                var surface = DesignTokens.Colour(palette, paletteName, background);
                var ratio = DesignTokens.ContrastRatio(colour, surface);
                measured++;

                if (ratio < minimum)
                {
                    failures.Add(
                        $"{foreground} ({colour}) on {background} ({surface}) is "
                        + $"{DesignTokens.Format(ratio)}, below the {DesignTokens.Format(minimum)} minimum");
                }
            }
        }

        // Without this the gate would pass vacuously if the palette parser ever
        // stopped finding declarations.
        Assert.That(measured, Is.EqualTo(ForegroundTokens.Length * DesignTokens.BackgroundTokens.Length),
            "every foreground/background pair in the palette must be measured");

        Assert.That(failures, Is.Empty,
            $"{paletteName} in {DesignTokens.Stylesheet} fails the WCAG 2.1 minimum for normal-size text. "
            + "On a dark palette the fix is to lighten the token, on a light palette to darken it, "
            + "and in both cases to keep it less emphatic than the token above it in the ladder."
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }
}
