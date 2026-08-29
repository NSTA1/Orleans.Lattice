using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The design system's structural gate (issue #1749): the breakpoint widths are
/// declared in exactly one stylesheet and one .NET constant pair, and nothing
/// else in the Explorer is allowed to hard-code one.
/// </summary>
/// <remarks>
/// <para>
/// This is the guard the design-system issue requires. Without it, the
/// responsive layer decays exactly the way the 65 KB monolith did: one
/// component at a time, each inventing its own width, until "responsive" means
/// forty disagreeing opinions instead of one named set (epic decision D7).
/// </para>
/// <para>
/// It checks three things:
/// </para>
/// <list type="number">
/// <item>No stylesheet outside the breakpoint layer contains a width or height
/// media feature. Non-dimensional queries such as
/// <c>prefers-reduced-motion</c> are unaffected, so the Explorer's existing
/// accessibility rule stays legal.</item>
/// <item>No C#, Razor, or JavaScript source outside the token layer hard-codes a
/// breakpoint width, whether as a bare number in a <c>matchMedia</c> or
/// <c>min-width</c> string or as a stray copy of the constant.</item>
/// <item>The widths in the breakpoint stylesheet - both the media queries and
/// the custom properties it publishes - agree exactly with
/// <see cref="LatticeBreakpoints"/>, so the CSS and .NET copies can never drift
/// apart in silence.</item>
/// </list>
/// </remarks>
[TestFixture]
public sealed class BreakpointTokenHygieneTests
{
    private const string ExplorerSourceRoot = "src/lattice.explorer";

    /// <summary>
    /// The one file allowed to name a breakpoint width. Everything else refers
    /// to a breakpoint by name.
    /// </summary>
    private const string BreakpointStylesheet =
        "src/lattice.explorer/DesignSystem/wwwroot/lattice-breakpoints.css";

    /// <summary>
    /// The one .NET file allowed to hold the breakpoint constants.
    /// </summary>
    private const string BreakpointTokenSource =
        "src/lattice.explorer/DesignSystem/Tokens/LatticeBreakpoints.cs";

    private static readonly string[] ScannedSourceExtensions = [".cs", ".razor", ".js"];

    // A media feature that constrains a viewport dimension, in either the
    // classic `(min-width: 600px)` form or the range form `(width >= 600px)`.
    private static readonly Regex DimensionalMediaFeature = new(
        @"\(\s*(?:min-|max-)?(?:width|height|device-width|device-height)\s*[:<>=]",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex MediaAtRule = new(
        @"@media[^{]*", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex PixelWidth = new(
        @"(\d+)px", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    [Test]
    public void No_stylesheet_outside_the_breakpoint_layer_declares_a_width_media_query()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var allowed = Path.Combine(repoRoot, BreakpointStylesheet.Replace('/', Path.DirectorySeparatorChar));

        var violations = new List<string>();
        var scanned = 0;
        foreach (var file in EnumerateExplorerFiles(repoRoot, "*.css"))
        {
            scanned++;
            if (string.Equals(file, allowed, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (!lines[i].Contains("@media", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (DimensionalMediaFeature.IsMatch(lines[i]))
                {
                    violations.Add($"{Relative(repoRoot, file)}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        // Without this the gate would pass vacuously if the scan root ever moved.
        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the Explorer's stylesheets");

        Assert.That(violations, Is.Empty,
            "A width media query may only appear in the breakpoint layer ("
            + BreakpointStylesheet
            + "). Refer to a breakpoint by name instead - compose the "
            + "lx-only-compact / lx-medium-up / lx-expanded-up utilities, or take the "
            + "cascaded LatticeAdaptiveContext and branch on its Breakpoint. Per-component "
            + "media queries reproduce the monolith in a new form (epic decision D7)."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void No_source_outside_the_token_layer_hard_codes_a_breakpoint_width()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var allowed = Path.Combine(repoRoot, BreakpointTokenSource.Replace('/', Path.DirectorySeparatorChar));

        var forbidden = new[]
        {
            LatticeBreakpoints.MediumMinimumWidth.ToString(CultureInfo.InvariantCulture),
            LatticeBreakpoints.ExpandedMinimumWidth.ToString(CultureInfo.InvariantCulture),
        };

        var violations = new List<string>();
        var scanned = 0;
        foreach (var extension in ScannedSourceExtensions)
        {
            foreach (var file in EnumerateExplorerFiles(repoRoot, "*" + extension))
            {
                scanned++;
                if (string.Equals(file, allowed, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var lines = File.ReadAllLines(file);
                for (var i = 0; i < lines.Length; i++)
                {
                    var line = lines[i];

                    // Only a line that is talking about viewport width at all can
                    // be hard-coding a breakpoint; a `600` in unrelated arithmetic
                    // is not this gate's business.
                    if (!MentionsViewportWidth(line))
                    {
                        continue;
                    }

                    foreach (var width in forbidden)
                    {
                        if (ContainsStandaloneNumber(line, width))
                        {
                            violations.Add($"{Relative(repoRoot, file)}:{i + 1}: {line.Trim()}");
                            break;
                        }
                    }
                }
            }
        }

        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the Explorer's sources");

        Assert.That(violations, Is.Empty,
            "A breakpoint width may only be named in the token layer ("
            + BreakpointTokenSource
            + "). Use LatticeBreakpoints.MediumMinimumWidth / ExpandedMinimumWidth, or "
            + "LatticeBreakpoints.Resolve, instead of restating the number."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_breakpoint_stylesheets_media_queries_use_only_the_declared_widths()
    {
        var css = ReadBreakpointStylesheet();

        var widths = new SortedSet<int>();
        foreach (Match media in MediaAtRule.Matches(css))
        {
            if (!DimensionalMediaFeature.IsMatch(media.Value))
            {
                continue;
            }

            foreach (Match pixels in PixelWidth.Matches(media.Value))
            {
                widths.Add(int.Parse(pixels.Groups[1].Value, CultureInfo.InvariantCulture));
            }
        }

        Assert.That(widths, Is.EqualTo(new[]
        {
            LatticeBreakpoints.MediumMinimumWidth,
            LatticeBreakpoints.ExpandedMinimumWidth,
        }), "the breakpoint layer's media queries must use exactly the two declared boundary widths");
    }

    [Test]
    public void The_breakpoint_stylesheets_custom_properties_match_the_dotnet_constants()
    {
        var css = ReadBreakpointStylesheet();

        Assert.Multiple(() =>
        {
            Assert.That(
                ReadCustomPropertyPixels(css, LatticeBreakpoints.MediumMinimumWidthCustomProperty),
                Is.EqualTo(LatticeBreakpoints.MediumMinimumWidth),
                LatticeBreakpoints.MediumMinimumWidthCustomProperty
                + " must equal LatticeBreakpoints.MediumMinimumWidth");
            Assert.That(
                ReadCustomPropertyPixels(css, LatticeBreakpoints.ExpandedMinimumWidthCustomProperty),
                Is.EqualTo(LatticeBreakpoints.ExpandedMinimumWidth),
                LatticeBreakpoints.ExpandedMinimumWidthCustomProperty
                + " must equal LatticeBreakpoints.ExpandedMinimumWidth");
        });
    }

    [Test]
    public void The_breakpoint_layer_keeps_the_reduced_motion_rule()
    {
        var css = ReadBreakpointStylesheet();

        Assert.That(css, Does.Contain("prefers-reduced-motion"),
            "the design system must carry the reduced-motion guarantee on its own, "
            + "so a head that does not load the legacy stylesheet still honours it");
    }

    [Test]
    public void The_legacy_stylesheet_still_carries_its_reduced_motion_rule()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var appCss = Path.Combine(
            repoRoot,
            "src/lattice.explorer/UI/wwwroot/app.css".Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(appCss), Is.True, "app.css is retired by a later issue in the epic, not this one");
        Assert.That(File.ReadAllText(appCss), Does.Contain("prefers-reduced-motion"));
    }

    [Test]
    public void The_scanner_detects_a_width_media_query_it_is_shown()
    {
        // Battery test for the smoke detector: a change that neuters the pattern
        // must fail here rather than silently passing the gate above.
        Assert.Multiple(() =>
        {
            Assert.That(DimensionalMediaFeature.IsMatch("@media (min-width: 600px) {"), Is.True);
            Assert.That(DimensionalMediaFeature.IsMatch("@media (max-width:599px){"), Is.True);
            Assert.That(DimensionalMediaFeature.IsMatch("@media not all and (min-width: 1024px) {"), Is.True);
            Assert.That(DimensionalMediaFeature.IsMatch("@media (width >= 600px) {"), Is.True);
            Assert.That(DimensionalMediaFeature.IsMatch("@media (min-height: 400px) {"), Is.True);

            Assert.That(DimensionalMediaFeature.IsMatch("@media (prefers-reduced-motion: reduce) {"), Is.False);
            Assert.That(DimensionalMediaFeature.IsMatch("@media print {"), Is.False);
            Assert.That(DimensionalMediaFeature.IsMatch("@media (prefers-color-scheme: dark) {"), Is.False);
        });
    }

    [Test]
    public void The_scanner_detects_a_hard_coded_width_it_is_shown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(MentionsViewportWidth("window.matchMedia(\"(min-width: 600px)\")"), Is.True);
            Assert.That(ContainsStandaloneNumber("window.matchMedia(\"(min-width: 600px)\")", "600"), Is.True);

            Assert.That(MentionsViewportWidth("if (viewportWidth >= 1024)"), Is.True);
            Assert.That(ContainsStandaloneNumber("if (viewportWidth >= 1024)", "1024"), Is.True);

            // A longer number that merely contains the digits is not a match.
            Assert.That(ContainsStandaloneNumber("var width = 10240;", "1024"), Is.False);
            Assert.That(ContainsStandaloneNumber("var width = 16000;", "600"), Is.False);

            // Arithmetic that has nothing to do with a viewport is out of scope.
            Assert.That(MentionsViewportWidth("var timeoutMs = 600;"), Is.False);
        });
    }

    private static string ReadBreakpointStylesheet()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var path = Path.Combine(repoRoot, BreakpointStylesheet.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, BreakpointStylesheet + " must exist");

        return File.ReadAllText(path);
    }

    private static int ReadCustomPropertyPixels(string css, string customProperty)
    {
        var match = Regex.Match(
            css,
            Regex.Escape(customProperty) + @"\s*:\s*(\d+)px\s*;",
            RegexOptions.IgnoreCase);

        Assert.That(match.Success, Is.True, customProperty + " must be declared in the breakpoint layer");

        return int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Whether a line is talking about a viewport dimension at all, and is
    /// therefore in scope for the hard-coded-width check.
    /// </summary>
    private static bool MentionsViewportWidth(string line) =>
        line.Contains("min-width", StringComparison.OrdinalIgnoreCase)
        || line.Contains("max-width", StringComparison.OrdinalIgnoreCase)
        || line.Contains("matchMedia", StringComparison.OrdinalIgnoreCase)
        || line.Contains("viewportWidth", StringComparison.OrdinalIgnoreCase)
        || line.Contains("innerWidth", StringComparison.OrdinalIgnoreCase)
        || line.Contains("breakpoint", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Whether <paramref name="line"/> contains <paramref name="number"/> as a
    /// whole number rather than as a run of digits inside a longer one.
    /// </summary>
    private static bool ContainsStandaloneNumber(string line, string number)
    {
        var index = 0;
        while ((index = line.IndexOf(number, index, StringComparison.Ordinal)) >= 0)
        {
            var before = index == 0 || !char.IsDigit(line[index - 1]);
            var afterIndex = index + number.Length;
            var after = afterIndex >= line.Length || !char.IsDigit(line[afterIndex]);

            if (before && after)
            {
                return true;
            }

            index = afterIndex;
        }

        return false;
    }

    private static IEnumerable<string> EnumerateExplorerFiles(string repoRoot, string pattern) =>
        HygieneRepository.EnumerateFiles(
            Path.Combine(repoRoot, ExplorerSourceRoot.Replace('/', Path.DirectorySeparatorChar)),
            pattern);

    /// <summary>
    /// Renders a scanned path relative to the repository root, with forward
    /// slashes, so a violation message is copy-pasteable on any platform.
    /// </summary>
    private static string Relative(string repoRoot, string file) =>
        Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
}
