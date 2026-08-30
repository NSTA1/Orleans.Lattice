using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The design system's contrast gate (issue #1801): every text colour token
/// clears the WCAG 2.1 AA minimum against every background colour token in its
/// own palette, measured rather than asserted by eye.
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
/// covers both palettes exhaustively instead of sampling one.</item>
/// </list>
/// <para>
/// The bar is the 4.5:1 for normal-size text rather than the relaxed 3:1 for
/// large text, because these tokens are spent at the body and caption sizes of
/// the type scale, all of which sit below the 18.66px / 14pt-bold threshold that
/// the relaxed allowance requires.
/// </para>
/// <para>
/// Scope is deliberately the text-on-surface family. The status and accent
/// tokens are a separate question: they are spent as fills as often as as text,
/// and a fill carries no contrast requirement of its own, so folding them in
/// here would assert a rule that does not apply to how they are used.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TextContrastTokenHygieneTests
{
    /// <summary>
    /// The one file that declares a palette. Every colour measured here is read
    /// from it, so the gate cannot drift from what the Explorer actually ships.
    /// </summary>
    private const string TokenStylesheet =
        "src/lattice.explorer/DesignSystem/wwwroot/lattice-tokens.css";

    /// <summary>
    /// WCAG 2.1 success criterion 1.4.3, normal-size text.
    /// </summary>
    private const double MinimumContrastRatio = 4.5;

    private const string DarkPaletteSelector = ":root[data-theme=\"dark\"]";
    private const string LightPaletteSelector = ":root[data-theme=\"light\"]";

    /// <summary>
    /// The foreground tokens, ordered from most to least emphatic. The ladder
    /// test below depends on that order.
    /// </summary>
    private static readonly string[] TextTokens =
    [
        "--lx-color-text",
        "--lx-color-text-muted",
        "--lx-color-text-dim",
    ];

    /// <summary>
    /// Every token a surface can paint behind that text. A text token has to
    /// clear the bar on all of them, not merely on the canvas, because a raised
    /// or sunken surface is a different background with a different ratio.
    /// </summary>
    private static readonly string[] BackgroundTokens =
    [
        "--lx-color-canvas",
        "--lx-color-surface",
        "--lx-color-surface-raised",
        "--lx-color-surface-sunken",
    ];

    private static readonly Regex CssComment = new(
        @"/\*.*?\*/", RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex CustomProperty = new(
        @"(--[a-z0-9-]+)\s*:\s*([^;]+);", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex HexColour = new(
        @"^#[0-9a-f]{6}$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    [Test]
    public void Every_dark_palette_text_token_clears_wcag_aa_on_every_surface()
    {
        AssertPaletteClearsAa(DarkPaletteSelector);
    }

    [Test]
    public void Every_light_palette_text_token_clears_wcag_aa_on_every_surface()
    {
        AssertPaletteClearsAa(LightPaletteSelector);
    }

    [Test]
    public void Every_palette_keeps_its_text_tokens_in_a_descending_emphasis_ladder()
    {
        // Raising a failing token until the number passes is not on its own a
        // fix: setting dim equal to the primary text colour would clear the bar
        // above while destroying the only thing the token exists to express.
        // Measured against its own canvas, each token must be strictly less
        // emphatic than the one before it.
        foreach (var selector in new[] { DarkPaletteSelector, LightPaletteSelector })
        {
            var palette = ReadPalette(selector);
            var canvas = Colour(palette, selector, "--lx-color-canvas");

            for (var i = 1; i < TextTokens.Length; i++)
            {
                var stronger = ContrastRatio(Colour(palette, selector, TextTokens[i - 1]), canvas);
                var weaker = ContrastRatio(Colour(palette, selector, TextTokens[i]), canvas);

                Assert.That(weaker, Is.LessThan(stronger),
                    $"{selector}: {TextTokens[i]} ({Format(weaker)}) must read as less emphatic than "
                    + $"{TextTokens[i - 1]} ({Format(stronger)}) on the canvas. A token that clears the "
                    + "contrast bar by becoming indistinguishable from the one above it has not been fixed.");
            }
        }
    }

    [Test]
    public void Both_palettes_declare_the_same_measured_tokens()
    {
        // Without this, dropping a token from one palette would make that
        // palette's gate quietly measure fewer pairs rather than fail.
        var dark = ReadPalette(DarkPaletteSelector);
        var light = ReadPalette(LightPaletteSelector);

        Assert.Multiple(() =>
        {
            foreach (var token in TextTokens.Concat(BackgroundTokens))
            {
                Assert.That(dark.ContainsKey(token), Is.True,
                    $"{DarkPaletteSelector} must declare {token}");
                Assert.That(light.ContainsKey(token), Is.True,
                    $"{LightPaletteSelector} must declare {token}");
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
            Assert.That(ContrastRatio("#000000", "#ffffff"), Is.EqualTo(21.0).Within(0.0001),
                "black on white is the maximum ratio the formula can produce");
            Assert.That(ContrastRatio("#ffffff", "#ffffff"), Is.EqualTo(1.0).Within(0.0001),
                "a colour against itself is the minimum ratio");

            // The canonical worked example: #777777 on white is the greyscale
            // value that sits just under the AA bar.
            Assert.That(ContrastRatio("#777777", "#ffffff"), Is.EqualTo(4.478).Within(0.001));
            Assert.That(ContrastRatio("#777777", "#ffffff"), Is.LessThan(MinimumContrastRatio));

            // The ratio is symmetric: which colour is the foreground does not
            // change the measurement.
            Assert.That(ContrastRatio("#0b0e14", "#7c8797"),
                Is.EqualTo(ContrastRatio("#7c8797", "#0b0e14")).Within(0.0001));
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
            Assert.That(ContrastRatio("#5a6373", "#0b0e14"), Is.EqualTo(3.19).Within(0.01),
                "the dark palette's retired dim token, as measured on the issue");
            Assert.That(ContrastRatio("#5a6373", "#0b0e14"), Is.LessThan(MinimumContrastRatio),
                "and it must be judged a failure, or this fixture guards nothing");

            Assert.That(ContrastRatio("#8a93a3", "#f6f8fb"), Is.EqualTo(2.91).Within(0.01),
                "the light palette's retired dim token, a worse instance of the same defect");
            Assert.That(ContrastRatio("#8a93a3", "#f6f8fb"), Is.LessThan(MinimumContrastRatio));
        });
    }

    private static void AssertPaletteClearsAa(string selector)
    {
        var palette = ReadPalette(selector);

        var failures = new List<string>();
        var measured = 0;

        foreach (var text in TextTokens)
        {
            var foreground = Colour(palette, selector, text);

            foreach (var background in BackgroundTokens)
            {
                var surface = Colour(palette, selector, background);
                var ratio = ContrastRatio(foreground, surface);
                measured++;

                if (ratio < MinimumContrastRatio)
                {
                    failures.Add(
                        $"{text} ({foreground}) on {background} ({surface}) is {Format(ratio)}, "
                        + $"below the {Format(MinimumContrastRatio)} WCAG 2.1 AA minimum for normal text");
                }
            }
        }

        // Without this the gate would pass vacuously if the palette parser ever
        // stopped finding declarations.
        Assert.That(measured, Is.EqualTo(TextTokens.Length * BackgroundTokens.Length),
            "every text/background pair in the palette must be measured");

        Assert.That(failures, Is.Empty,
            $"{selector} in {TokenStylesheet} fails WCAG 2.1 AA (1.4.3) for normal-size text. "
            + "On a dark palette the fix is to lighten the token, on a light palette to darken it, "
            + "and in both cases to keep it less emphatic than the token above it in the ladder."
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// Reads the custom properties declared in one palette block. Comments are
    /// stripped first so a hex value quoted in prose - this stylesheet documents
    /// the retired values in exactly that way - can never be mistaken for a
    /// declaration.
    /// </summary>
    private static IReadOnlyDictionary<string, string> ReadPalette(string selector)
    {
        var css = CssComment.Replace(ReadTokenStylesheet(), string.Empty);

        var selectorIndex = css.IndexOf(selector, StringComparison.Ordinal);
        Assert.That(selectorIndex, Is.GreaterThanOrEqualTo(0),
            $"{selector} must be declared in {TokenStylesheet}");

        var open = css.IndexOf('{', selectorIndex);
        Assert.That(open, Is.GreaterThanOrEqualTo(0), $"{selector} must open a declaration block");

        var close = css.IndexOf('}', open);
        Assert.That(close, Is.GreaterThan(open), $"{selector} must close its declaration block");

        var block = css[(open + 1)..close];

        var declarations = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match declaration in CustomProperty.Matches(block))
        {
            declarations[declaration.Groups[1].Value] = declaration.Groups[2].Value.Trim();
        }

        Assert.That(declarations, Is.Not.Empty, $"{selector} must declare custom properties");

        return declarations;
    }

    /// <summary>
    /// Resolves one token to the literal colour it declares, failing if it is
    /// absent or is not a plain <c>#rrggbb</c> value. An indirection such as a
    /// <c>var()</c> reference would otherwise slip past the measurement.
    /// </summary>
    private static string Colour(
        IReadOnlyDictionary<string, string> palette,
        string selector,
        string token)
    {
        Assert.That(palette.ContainsKey(token), Is.True, $"{selector} must declare {token}");

        var value = palette[token];
        Assert.That(HexColour.IsMatch(value), Is.True,
            $"{selector}: {token} must be a literal #rrggbb colour so its contrast can be measured, "
            + $"but it is '{value}'");

        return value;
    }

    /// <summary>
    /// The WCAG 2.1 contrast ratio between two opaque sRGB colours, defined as
    /// <c>(Lighter + 0.05) / (Darker + 0.05)</c> over their relative luminances.
    /// </summary>
    private static double ContrastRatio(string foreground, string background)
    {
        var a = RelativeLuminance(foreground) + 0.05;
        var b = RelativeLuminance(background) + 0.05;

        return a > b ? a / b : b / a;
    }

    private static double RelativeLuminance(string hex) =>
        (0.2126 * Linearise(Channel(hex, 1)))
        + (0.7152 * Linearise(Channel(hex, 3)))
        + (0.0722 * Linearise(Channel(hex, 5)));

    private static double Channel(string hex, int offset) =>
        int.Parse(hex.Substring(offset, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture) / 255.0;

    private static double Linearise(double channel) =>
        channel <= 0.03928 ? channel / 12.92 : Math.Pow((channel + 0.055) / 1.055, 2.4);

    private static string Format(double ratio) =>
        ratio.ToString("0.00", CultureInfo.InvariantCulture) + ":1";

    private static string ReadTokenStylesheet()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var path = Path.Combine(repoRoot, TokenStylesheet.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, TokenStylesheet + " must exist");

        return File.ReadAllText(path);
    }
}
