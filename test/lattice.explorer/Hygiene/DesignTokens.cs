using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// Reads the Explorer's one design-token stylesheet and resolves it into the
/// palettes a browser would compute, so a contrast gate can measure the values
/// the Explorer actually ships rather than a copy of them.
/// </summary>
/// <remarks>
/// <para>
/// This exists so <see cref="TextContrastTokenHygieneTests"/> and
/// <see cref="NonTextContrastTokenHygieneTests"/> share one parser and one
/// implementation of the WCAG luminance maths. Two copies would be two things to
/// keep correct, and a contrast gate that measures wrongly passes vacuously
/// while reporting a compliance it never checked.
/// </para>
/// <para>
/// It models two things about the cascade that matter for correctness:
/// </para>
/// <list type="number">
/// <item><b>Layering.</b> The light and high-contrast blocks restate only the
/// tokens that differ, exactly as a stylesheet should. Resolving a palette
/// therefore means layering its blocks in cascade order, not reading one block
/// in isolation - otherwise the gate would measure a palette with holes in it
/// and mistake a missing declaration for an absent requirement.</item>
/// <item><b>Aliases.</b> A token declared as <c>var(--other)</c> resolves
/// against the palette in force, so overriding the target in a later block
/// moves the alias too. Resolving them here is what lets the stylesheet express
/// "the rest state is the dim step" once instead of restating a literal in every
/// palette and letting the copies drift.</item>
/// </list>
/// </remarks>
internal static class DesignTokens
{
    /// <summary>
    /// The one file that declares a palette. Every colour measured by either
    /// fixture is read from it, so a gate cannot drift from what ships.
    /// </summary>
    public const string Stylesheet =
        "src/lattice.explorer/DesignSystem/wwwroot/lattice-tokens.css";

    /// <summary>The block holding the tokens every palette shares.</summary>
    public const string RootSelector = ":root";

    /// <summary>The default palette.</summary>
    public const string DarkSelector = ":root[data-theme=\"dark\"]";

    /// <summary>The light palette, layered over the dark one.</summary>
    public const string LightSelector = ":root[data-theme=\"light\"]";

    /// <summary>
    /// The high-contrast overlay for the dark palette. It excludes the light
    /// theme explicitly: the two overlays carry the same specificity, so mutual
    /// exclusion is what stops a token the dark overlay sets and the light one
    /// forgets from winning on document order.
    /// </summary>
    public const string DarkHighContrastSelector =
        ":root:not([data-theme=\"light\"])[data-contrast=\"more\"]";

    /// <summary>The high-contrast overlay for the light palette.</summary>
    public const string LightHighContrastSelector =
        ":root[data-theme=\"light\"][data-contrast=\"more\"]";

    /// <summary>
    /// The operating-system-driven copy of the dark high-contrast overlay.
    /// </summary>
    public const string DarkHighContrastMediaSelector =
        ":root:not([data-theme=\"light\"]):not([data-contrast=\"standard\"])";

    /// <summary>
    /// The operating-system-driven copy of the light high-contrast overlay.
    /// </summary>
    public const string LightHighContrastMediaSelector =
        ":root[data-theme=\"light\"]:not([data-contrast=\"standard\"])";

    /// <summary>The query that honours a platform request for more contrast.</summary>
    public const string PrefersContrastQuery = "@media (prefers-contrast: more)";

    /// <summary>The query that honours a platform-replaced palette.</summary>
    public const string ForcedColorsQuery = "@media (forced-colors: active)";

    /// <summary>WCAG 2.1 SC 1.4.3, normal-size text.</summary>
    public const double TextMinimum = 4.5;

    /// <summary>
    /// WCAG 2.1 SC 1.4.11, non-text contrast: any boundary, indicator or focus
    /// ring a reader needs to perceive.
    /// </summary>
    public const double NonTextMinimum = 3.0;

    /// <summary>WCAG 2.1 SC 1.4.6, the AAA bar the high-contrast overlay holds.</summary>
    public const double TextEnhancedMinimum = 7.0;

    /// <summary>
    /// The non-text bar in high contrast: one full step above the standard 3:1.
    /// </summary>
    public const double NonTextEnhancedMinimum = 4.5;

    /// <summary>
    /// Every token a surface can paint behind a foreground. A foreground has to
    /// clear its bar on all of them, not merely on the canvas, because a raised
    /// panel, a sunken well, a selected row and a danger callout are each a
    /// different background with a different ratio.
    /// </summary>
    public static readonly string[] BackgroundTokens =
    [
        "--lx-color-canvas",
        "--lx-color-surface",
        "--lx-color-surface-raised",
        "--lx-color-surface-sunken",
        "--lx-color-surface-selected",
        "--lx-color-danger-surface",
    ];

    /// <summary>
    /// The three elevation levels, ordered from lowest to highest. A palette has
    /// to keep them ordered and separated; the light palette declared all three
    /// as <c>#ffffff</c> before issue #1846.
    /// </summary>
    public static readonly string[] ElevationTokens =
    [
        "--lx-color-surface-sunken",
        "--lx-color-surface",
        "--lx-color-surface-raised",
    ];

    private static readonly Regex CssComment = new(
        @"/\*.*?\*/", RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex CustomProperty = new(
        @"(--[a-z0-9-]+)\s*:\s*([^;]+);", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex HexColour = new(
        @"^#[0-9a-f]{6}$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex Alias = new(
        @"^var\(\s*(--[a-z0-9-]+)\s*\)$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// The stylesheet with its comments stripped, so a hex value quoted in prose
    /// - this file documents its own retired values in exactly that way - can
    /// never be mistaken for a declaration.
    /// </summary>
    /// <remarks>
    /// Read once and cached. Every fixture in this folder resolves several
    /// palettes, each palette layers several blocks, and each block would
    /// otherwise re-read the file and re-run the comment-stripping regex over
    /// it - roughly a hundred reads of the same unchanging file across one test
    /// run, for no signal. The file cannot change mid-run, so caching costs
    /// nothing in fidelity.
    /// </remarks>
    public static string Source() => CachedSource.Value;

    private static readonly Lazy<string> CachedSource = new(() =>
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var path = Path.Combine(repoRoot, Stylesheet.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, Stylesheet + " must exist");

        return CssComment.Replace(File.ReadAllText(path), string.Empty);
    });

    /// <summary>
    /// The custom properties declared by one selector's block, taken verbatim
    /// and without resolving aliases.
    /// </summary>
    /// <param name="selector">The selector whose block to read.</param>
    /// <param name="within">
    /// An optional region of the stylesheet to search, so a selector inside an
    /// at-rule can be addressed without colliding with the same selector at the
    /// top level.
    /// </param>
    public static IReadOnlyDictionary<string, string> Block(string selector, string? within = null)
    {
        var css = within ?? Source();

        var selectorIndex = IndexOfSelector(css, selector);
        Assert.That(selectorIndex, Is.GreaterThanOrEqualTo(0),
            $"{selector} must be declared in {Stylesheet}");

        var open = css.IndexOf('{', selectorIndex);
        Assert.That(open, Is.GreaterThanOrEqualTo(0), $"{selector} must open a declaration block");

        var close = css.IndexOf('}', open);
        Assert.That(close, Is.GreaterThan(open), $"{selector} must close its declaration block");

        var declarations = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match declaration in CustomProperty.Matches(css[(open + 1)..close]))
        {
            declarations[declaration.Groups[1].Value] = declaration.Groups[2].Value.Trim();
        }

        Assert.That(declarations, Is.Not.Empty, $"{selector} must declare custom properties");

        return declarations;
    }

    /// <summary>
    /// The body of an at-rule, brace-matched so a nested block does not truncate
    /// it. Used to address the selectors inside the media queries.
    /// </summary>
    public static string Region(string atRule)
    {
        var css = Source();

        var start = css.IndexOf(atRule, StringComparison.Ordinal);
        Assert.That(start, Is.GreaterThanOrEqualTo(0), $"{atRule} must be declared in {Stylesheet}");

        var open = css.IndexOf('{', start);
        Assert.That(open, Is.GreaterThanOrEqualTo(0), $"{atRule} must open a block");

        var depth = 0;
        for (var i = open; i < css.Length; i++)
        {
            if (css[i] == '{')
            {
                depth++;
            }
            else if (css[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return css[(open + 1)..i];
                }
            }
        }

        Assert.Fail($"{atRule} must close its block");
        return string.Empty;
    }

    /// <summary>
    /// Resolves the palette a browser would compute for a set of blocks applied
    /// in cascade order, with single-token <c>var()</c> aliases followed to the
    /// value they land on.
    /// </summary>
    /// <param name="selectors">
    /// The blocks to layer, least specific first. A later block overrides an
    /// earlier one, exactly as the cascade does.
    /// </param>
    public static IReadOnlyDictionary<string, string> Palette(params string[] selectors)
    {
        var layered = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var selector in selectors)
        {
            foreach (var (token, value) in Block(selector))
            {
                layered[token] = value;
            }
        }

        var resolved = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (token, _) in layered)
        {
            resolved[token] = Dereference(layered, token);
        }

        return resolved;
    }

    /// <summary>
    /// Resolves one token to the literal colour it declares, failing if it is
    /// absent or is not a plain <c>#rrggbb</c> value. A translucent
    /// <c>rgba()</c> would otherwise slip past the measurement: it composites
    /// differently over every surface, so its contrast is not a number the
    /// stylesheet can promise.
    /// </summary>
    public static string Colour(
        IReadOnlyDictionary<string, string> palette,
        string paletteName,
        string token)
    {
        var value = Value(palette, paletteName, token);

        Assert.That(HexColour.IsMatch(value), Is.True,
            $"{paletteName}: {token} must resolve to a literal #rrggbb colour so its contrast can "
            + $"be measured, but it is '{value}'. A translucent value composites differently over "
            + "every surface, so the stylesheet cannot promise a ratio for it.");

        return value;
    }

    /// <summary>Resolves one token to its declared value, whatever its form.</summary>
    public static string Value(
        IReadOnlyDictionary<string, string> palette,
        string paletteName,
        string token)
    {
        Assert.That(palette.ContainsKey(token), Is.True, $"{paletteName} must declare {token}");

        return palette[token];
    }

    /// <summary>
    /// The WCAG 2.1 contrast ratio between two opaque sRGB colours, defined as
    /// <c>(Lighter + 0.05) / (Darker + 0.05)</c> over their relative luminances.
    /// </summary>
    public static double ContrastRatio(string foreground, string background)
    {
        var a = RelativeLuminance(foreground) + 0.05;
        var b = RelativeLuminance(background) + 0.05;

        return a > b ? a / b : b / a;
    }

    /// <summary>
    /// The WCAG 2.1 relative luminance of an opaque sRGB colour. This is also
    /// the greyscale value a colour collapses to, which is why a contrast ratio
    /// is exactly the test of whether two colours survive a greyscale rendering.
    /// </summary>
    public static double RelativeLuminance(string hex) =>
        (0.2126 * Linearise(Channel(hex, 1)))
        + (0.7152 * Linearise(Channel(hex, 3)))
        + (0.0722 * Linearise(Channel(hex, 5)));

    /// <summary>Renders a ratio the way the WCAG tooling does.</summary>
    public static string Format(double ratio) =>
        ratio.ToString("0.00", CultureInfo.InvariantCulture) + ":1";

    /// <summary>
    /// The worst ratio a foreground reaches across every background in a
    /// palette, and the background it reached it on.
    /// </summary>
    public static (double Ratio, string Background) Worst(
        IReadOnlyDictionary<string, string> palette,
        string paletteName,
        string token)
    {
        var foreground = Colour(palette, paletteName, token);

        var worst = double.MaxValue;
        var on = BackgroundTokens[0];

        foreach (var background in BackgroundTokens)
        {
            var ratio = ContrastRatio(foreground, Colour(palette, paletteName, background));
            if (ratio < worst)
            {
                worst = ratio;
                on = background;
            }
        }

        return (worst, on);
    }

    /// <summary>
    /// Follows a chain of single-token <c>var()</c> aliases to the value it
    /// lands on. The hop count is capped so a cycle fails loudly rather than
    /// hanging the suite.
    /// </summary>
    private static string Dereference(IReadOnlyDictionary<string, string> layered, string token)
    {
        var value = layered[token];

        for (var hop = 0; hop < 8; hop++)
        {
            var alias = Alias.Match(value);
            if (!alias.Success)
            {
                return value;
            }

            var target = alias.Groups[1].Value;
            Assert.That(layered.ContainsKey(target), Is.True,
                $"{token} aliases {target}, which no layered block declares");

            value = layered[target];
        }

        Assert.Fail($"{token} does not resolve within eight hops - the aliases are cyclic");
        return value;
    }

    /// <summary>
    /// Finds a selector as a whole selector rather than as a prefix of a longer
    /// one, so searching for <c>:root[data-theme="dark"]</c> can never land
    /// inside <c>:root[data-theme="dark"][data-contrast="more"]</c>.
    /// </summary>
    private static int IndexOfSelector(string css, string selector)
    {
        var index = 0;
        while ((index = css.IndexOf(selector, index, StringComparison.Ordinal)) >= 0)
        {
            var after = index + selector.Length;
            if (after >= css.Length)
            {
                return index;
            }

            var next = css[after];
            if (next is ',' or '{' || char.IsWhiteSpace(next))
            {
                return index;
            }

            index = after;
        }

        return -1;
    }

    private static double Channel(string hex, int offset) =>
        int.Parse(hex.Substring(offset, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture) / 255.0;

    private static double Linearise(double channel) =>
        channel <= 0.03928 ? channel / 12.92 : Math.Pow((channel + 0.055) / 1.055, 2.4);
}
