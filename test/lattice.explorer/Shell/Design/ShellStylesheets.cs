using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design;

/// <summary>
/// Reads the Shell design system's stylesheets - including the documentation
/// site's <c>tokens.css</c> the Shell links - into rules and resolved palettes,
/// so the contrast, state-role and design-rule gates measure what ships rather
/// than a copy of it.
/// </summary>
/// <remarks>
/// It models the two parts of the cascade the gates depend on: layering (a
/// later block restates only the tokens that differ, so a palette is its blocks
/// applied in order) and single-token <c>var()</c> aliases, which resolve
/// against the palette in force. It also keeps each rule's enclosing at-rule,
/// so a selector inside <c>@media (prefers-contrast: more)</c> is never
/// confused with the same selector at the top level.
/// </remarks>
internal static class ShellStylesheets
{
    /// <summary>The Shell's design folder, relative to the repository root.</summary>
    public const string DesignRoot = "src/lattice.explorer/Shell/wwwroot/design";

    /// <summary>The documentation site's tokens, which the Shell links rather than copies.</summary>
    public const string DocsSiteTokens = "docs-site/template/public/tokens.css";

    /// <summary>The Explorer-only Operate tokens.</summary>
    public const string Operate = DesignRoot + "/lattice-operate.css";

    /// <summary>The one Shell stylesheet allowed to name a width.</summary>
    public const string Breakpoints = DesignRoot + "/lattice-breakpoints.css";

    /// <summary>The design primitives.</summary>
    public const string Primitives = DesignRoot + "/lattice-primitives.css";

    /// <summary>The block every palette starts from, in both files.</summary>
    public const string PaperSelector = ":root,[data-bs-theme=\"light\"]";

    /// <summary>The Board (dark) block, in both files.</summary>
    public const string BoardSelector = "[data-bs-theme=\"dark\"]";

    /// <summary>The Paper high-contrast overlay chosen by the reader.</summary>
    public const string PaperMoreSelector = ":root:not([data-bs-theme=\"dark\"])[data-lt-contrast=\"more\"]";

    /// <summary>The Board high-contrast overlay chosen by the reader.</summary>
    public const string BoardMoreSelector = "[data-bs-theme=\"dark\"][data-lt-contrast=\"more\"]";

    /// <summary>The Paper high-contrast overlay the platform asks for.</summary>
    public const string PaperMoreMediaSelector = ":root:not([data-bs-theme=\"dark\"]):not([data-lt-contrast=\"standard\"])";

    /// <summary>The Board high-contrast overlay the platform asks for.</summary>
    public const string BoardMoreMediaSelector = "[data-bs-theme=\"dark\"]:not([data-lt-contrast=\"standard\"])";

    /// <summary>The query that honours a platform request for more contrast.</summary>
    public const string PrefersContrastQuery = "@media (prefers-contrast: more)";

    /// <summary>WCAG 2.2 SC 1.4.3, normal-size text.</summary>
    public const double TextMinimum = 4.5;

    /// <summary>WCAG 2.2 SC 1.4.6, the enhanced bar the high-contrast overlay holds.</summary>
    public const double TextEnhancedMinimum = 7.0;

    /// <summary>WCAG 2.2 SC 1.4.11, non-text contrast.</summary>
    public const double NonTextMinimum = 3.0;

    /// <summary>The non-text bar in high contrast: a full step above 3:1.</summary>
    public const double NonTextEnhancedMinimum = 4.5;

    /// <summary>
    /// Every opaque surface a foreground can sit on: the page, sunken paper
    /// (inputs, code), raised surfaces (dialogs, toasts) and the current-row band.
    /// </summary>
    public static readonly string[] Surfaces =
    [
        "--lt-surface",
        "--lt-surface-sunken",
        "--lt-surface-raised",
        "--lt-op-row-current",
    ];

    private static readonly Regex CssComment = new(@"/\*.*?\*/", RegexOptions.Singleline | RegexOptions.Compiled);

    private static readonly Regex CustomProperty = new(
        @"(--[a-z0-9-]+)\s*:\s*([^;]+);", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex HexColour = new(@"^#[0-9a-f]{6}$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex Alias = new(@"^var\(\s*(--[a-z0-9-]+)\s*\)$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Dictionary<string, IReadOnlyList<CssRule>> Cache = new(StringComparer.Ordinal);

    /// <summary>The Shell's whole static web asset root: the design system and every owner's own folder.</summary>
    public const string WebRoot = "src/lattice.explorer/Shell/wwwroot";

    /// <summary>
    /// Every stylesheet the Shell ships under <see cref="WebRoot"/> - the design
    /// system's and each chrome or area owner's own - so a class an owner defines
    /// in its folder counts as defined, and its rules obey the same design gates.
    /// </summary>
    public static IReadOnlyList<string> ShellStylesheetPaths()
    {
        var root = Absolute(WebRoot);
        Assert.That(Directory.Exists(Absolute(DesignRoot)), Is.True, DesignRoot + " must exist");
        var paths = HygieneRepository.EnumerateFiles(root, "*.css").OrderBy(path => path, StringComparer.Ordinal).ToArray();

        // The enumeration is git-tracked files only; without this every gate
        // that walks it would pass vacuously on a tree it never read.
        Assert.That(paths, Has.Length.GreaterThanOrEqualTo(4),
            "the scan must reach the Shell's fonts, operate, breakpoint and primitive stylesheets");
        return paths;
    }

    /// <summary>The absolute path of a repository-relative path.</summary>
    /// <param name="relative">A path relative to the repository root, with forward slashes.</param>
    public static string Absolute(string relative) =>
        Path.Combine(HygieneRepository.FindRepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>A stylesheet's text with its comments blanked, line structure preserved.</summary>
    /// <param name="relative">The stylesheet, relative to the repository root.</param>
    public static string WithoutComments(string relative)
    {
        var path = Absolute(relative);
        Assert.That(File.Exists(path), Is.True, relative + " must exist");
        return BlankComments(File.ReadAllText(path));
    }

    /// <summary>Blanks every CSS comment while preserving line numbers.</summary>
    /// <param name="css">The stylesheet text.</param>
    public static string BlankComments(string css) =>
        CssComment.Replace(css.Replace("\r\n", "\n"), match => new string(match.Value.Select(c => c == '\n' ? '\n' : ' ').ToArray()));

    /// <summary>Every rule in a stylesheet, with its enclosing at-rule (or empty at top level).</summary>
    /// <param name="relative">The stylesheet, relative to the repository root.</param>
    public static IReadOnlyList<CssRule> Rules(string relative)
    {
        lock (Cache)
        {
            if (!Cache.TryGetValue(relative, out var rules))
            {
                rules = Parse(WithoutComments(relative));
                Cache[relative] = rules;
            }

            return rules;
        }
    }

    /// <summary>Parses stylesheet text into rules.</summary>
    /// <param name="css">Comment-free stylesheet text.</param>
    public static IReadOnlyList<CssRule> Parse(string css)
    {
        var rules = new List<CssRule>();
        ParseInto(css, string.Empty, rules);
        return rules;
    }

    /// <summary>The custom properties declared by exactly one selector's block.</summary>
    /// <param name="relative">The stylesheet.</param>
    /// <param name="selector">The selector, in the normalised form <see cref="Normalise"/> produces.</param>
    /// <param name="atRule">The enclosing at-rule, or empty for the top level.</param>
    public static IReadOnlyDictionary<string, string> Block(string relative, string selector, string atRule = "")
    {
        var matches = Rules(relative)
            .Where(rule => rule.Selector == selector && rule.AtRule == atRule)
            .ToArray();

        Assert.That(matches, Has.Length.EqualTo(1),
            $"{relative} must declare exactly one '{selector}' block{(atRule.Length == 0 ? string.Empty : " inside " + atRule)}");

        return Declarations(matches[0].Body);
    }

    /// <summary>The custom-property declarations in a rule body.</summary>
    /// <param name="body">The text between a rule's braces.</param>
    public static IReadOnlyDictionary<string, string> Declarations(string body)
    {
        var declarations = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match match in CustomProperty.Matches(body))
        {
            declarations[match.Groups[1].Value] = match.Groups[2].Value.Trim();
        }

        return declarations;
    }

    /// <summary>Resolves one of the four palettes a browser can compute.</summary>
    /// <param name="palette">The palette to resolve.</param>
    public static IReadOnlyDictionary<string, string> Palette(ShellPalette palette)
    {
        var layers = new List<IReadOnlyDictionary<string, string>>
        {
            Block(DocsSiteTokens, PaperSelector),
            Block(Operate, PaperSelector),
        };

        if (palette is ShellPalette.Board or ShellPalette.BoardMore)
        {
            layers.Add(Block(DocsSiteTokens, BoardSelector));
            layers.Add(Block(Operate, BoardSelector));
        }

        if (palette == ShellPalette.PaperMore)
        {
            layers.Add(Block(Operate, PaperMoreSelector));
        }

        if (palette == ShellPalette.BoardMore)
        {
            layers.Add(Block(Operate, BoardMoreSelector));
        }

        var layered = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var layer in layers)
        {
            foreach (var (token, value) in layer)
            {
                layered[token] = value;
            }
        }

        return layered.ToDictionary(pair => pair.Key, pair => Dereference(layered, pair.Key), StringComparer.Ordinal);
    }

    /// <summary>Resolves one token to a literal <c>#rrggbb</c> colour, failing on anything else.</summary>
    /// <param name="palette">The resolved palette.</param>
    /// <param name="name">The palette's name, for messages.</param>
    /// <param name="token">The token.</param>
    public static string Colour(IReadOnlyDictionary<string, string> palette, ShellPalette name, string token)
    {
        Assert.That(palette.ContainsKey(token), Is.True, $"{name} must declare {token}");
        var value = palette[token];
        Assert.That(HexColour.IsMatch(value), Is.True,
            $"{name}: {token} must resolve to a literal #rrggbb colour so its contrast can be measured, but it is '{value}'.");
        return value;
    }

    /// <summary>The WCAG 2.2 contrast ratio between two opaque sRGB colours.</summary>
    /// <param name="foreground">One colour, as <c>#rrggbb</c>.</param>
    /// <param name="background">The other colour, as <c>#rrggbb</c>.</param>
    public static double ContrastRatio(string foreground, string background)
    {
        var a = RelativeLuminance(foreground) + 0.05;
        var b = RelativeLuminance(background) + 0.05;
        return a > b ? a / b : b / a;
    }

    /// <summary>Renders a ratio the way the WCAG tooling does.</summary>
    /// <param name="ratio">The ratio.</param>
    public static string Format(double ratio) => ratio.ToString("0.00", CultureInfo.InvariantCulture) + ":1";

    /// <summary>Normalises selector text: collapsed white space, none around commas.</summary>
    /// <param name="selector">Raw selector text.</param>
    public static string Normalise(string selector) =>
        Regex.Replace(Regex.Replace(selector.Trim(), @"\s+", " "), @"\s*,\s*", ",");

    /// <summary>Parses a CSS length in <c>rem</c> or <c>px</c> into CSS pixels at a 16px root.</summary>
    /// <param name="length">The length, such as <c>1.75rem</c>.</param>
    public static double Pixels(string length)
    {
        var match = Regex.Match(length.Trim(), @"^(?<n>\d+(?:\.\d+)?)(?<u>rem|px)$");
        Assert.That(match.Success, Is.True, $"'{length}' must be a plain rem or px length");
        var number = double.Parse(match.Groups["n"].Value, CultureInfo.InvariantCulture);
        return match.Groups["u"].Value == "rem" ? number * 16 : number;
    }

    private static void ParseInto(string css, string atRule, List<CssRule> rules)
    {
        var index = 0;
        while (index < css.Length)
        {
            var open = css.IndexOf('{', index);
            if (open < 0)
            {
                return;
            }

            var prelude = css[index..open].Trim();
            var close = MatchingBrace(css, open);
            var body = css[(open + 1)..close];

            if (prelude.StartsWith("@media", StringComparison.Ordinal)
                || prelude.StartsWith("@container", StringComparison.Ordinal)
                || prelude.StartsWith("@supports", StringComparison.Ordinal))
            {
                ParseInto(body, Normalise(prelude), rules);
            }
            else if (!prelude.StartsWith('@'))
            {
                rules.Add(new CssRule(atRule, Normalise(prelude), body));
            }

            index = close + 1;
        }
    }

    private static int MatchingBrace(string css, int open)
    {
        var depth = 0;
        for (var i = open; i < css.Length; i++)
        {
            if (css[i] == '{')
            {
                depth++;
            }
            else if (css[i] == '}' && --depth == 0)
            {
                return i;
            }
        }

        Assert.Fail("an unbalanced brace in a Shell stylesheet");
        return css.Length - 1;
    }

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
            Assert.That(layered.ContainsKey(target), Is.True, $"{token} aliases {target}, which no layered block declares");
            value = layered[target];
        }

        Assert.Fail($"{token} does not resolve within eight hops - the aliases are cyclic");
        return value;
    }

    private static double RelativeLuminance(string hex) =>
        (0.2126 * Linearise(Channel(hex, 1))) + (0.7152 * Linearise(Channel(hex, 3))) + (0.0722 * Linearise(Channel(hex, 5)));

    private static double Channel(string hex, int offset) =>
        int.Parse(hex.Substring(offset, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture) / 255.0;

    private static double Linearise(double channel) =>
        channel <= 0.03928 ? channel / 12.92 : Math.Pow((channel + 0.055) / 1.055, 2.4);
}
