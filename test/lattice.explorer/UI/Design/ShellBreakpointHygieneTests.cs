using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// The Shell's breakpoint gate, ported from <c>BreakpointTokenHygieneTests</c>:
/// the layout widths are named in exactly one stylesheet and one .NET type, and
/// nothing else in the Shell may hard-code one.
/// </summary>
/// <remarks>
/// <para>
/// A width query is a width query whether it is spelled <c>@media</c> or
/// <c>@container</c>, so both are gated. The Shell's own queries are
/// size-container queries against its root, which is also why the older gate,
/// which scans every Explorer stylesheet for dimensional <c>@media</c> rules and
/// allows only the retiring design system's file, stays green: the two gates
/// cover the two design systems until the cutover retires the old one.
/// </para>
/// </remarks>
[TestFixture]
public sealed class ShellBreakpointHygieneTests
{
    private const string ShellSourceRoot = "src/lattice.explorer/UI";
    private const string BreakpointTokenSource = "src/lattice.explorer/UI/Design/Tokens/LtBreakpoints.cs";

    private static readonly Regex DimensionalQuery = new(
        @"@(?:media|container)\b[^{]*\(\s*(?:min-|max-)?(?:width|height|inline-size|block-size|device-width|device-height)\s*[:<>=]",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex QueryPrelude = new(@"@(?:media|container)[^{]*", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex PixelWidth = new(@"(\d+)px", RegexOptions.Compiled);

    [Test]
    public void No_shell_stylesheet_outside_the_breakpoint_layer_declares_a_width_query()
    {
        var allowed = ShellStylesheets.Absolute(ShellStylesheets.Breakpoints);
        var violations = new List<string>();
        var scanned = 0;

        foreach (var file in HygieneRepository.EnumerateFiles(ShellStylesheets.Absolute(ShellSourceRoot), "*.css"))
        {
            scanned++;
            if (string.Equals(file, allowed, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var lines = ShellStylesheets.BlankComments(File.ReadAllText(file)).Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                if (DimensionalQuery.IsMatch(lines[i]))
                {
                    violations.Add($"{Relative(file)}:{i + 1}: {lines[i].Trim()}");
                }
            }
        }

        Assert.That(scanned, Is.GreaterThan(2), "the scan must reach the Shell's stylesheets");
        Assert.That(violations, Is.Empty,
            "A width query may only appear in " + ShellStylesheets.Breakpoints
            + ". Compose the lt-only-compact / lt-medium-up / lt-expanded-up utilities, or branch on "
            + "LtBreakpoints.Resolve, instead of inventing a width per component."
            + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_breakpoint_layer_queries_only_the_declared_widths()
    {
        var css = ShellStylesheets.WithoutComments(ShellStylesheets.Breakpoints);
        var widths = new SortedSet<int>();

        foreach (Match prelude in QueryPrelude.Matches(css))
        {
            if (!DimensionalQuery.IsMatch(prelude.Value))
            {
                continue;
            }

            Assert.That(prelude.Value, Does.Contain(LtBreakpoints.ContainerName),
                "every width query must measure the Shell root container, not the viewport");

            foreach (Match pixels in PixelWidth.Matches(prelude.Value))
            {
                widths.Add(int.Parse(pixels.Groups[1].Value, CultureInfo.InvariantCulture));
            }
        }

        Assert.That(widths, Is.EqualTo(new[] { LtBreakpoints.MediumMinimumWidth, LtBreakpoints.ExpandedMinimumWidth }));
    }

    [Test]
    public void The_breakpoint_layers_custom_properties_match_the_dotnet_constants()
    {
        var root = ShellStylesheets.Block(ShellStylesheets.Breakpoints, ":root");
        Assert.Multiple(() =>
        {
            Assert.That(root[LtBreakpoints.MediumMinimumWidthCustomProperty], Is.EqualTo($"{LtBreakpoints.MediumMinimumWidth}px"));
            Assert.That(root[LtBreakpoints.ExpandedMinimumWidthCustomProperty], Is.EqualTo($"{LtBreakpoints.ExpandedMinimumWidth}px"));
        });
    }

    [Test]
    public void The_breakpoint_layer_declares_the_container_it_measures()
    {
        var viewport = ShellStylesheets.Rules(ShellStylesheets.Breakpoints).Single(rule => rule.Selector == ".lt-viewport");
        Assert.That(viewport.Body, Does.Contain($"container: {LtBreakpoints.ContainerName} / inline-size;"));
    }

    [Test]
    public void The_breakpoint_layer_keeps_the_reduced_motion_rule()
    {
        Assert.That(ShellStylesheets.WithoutComments(ShellStylesheets.Breakpoints), Does.Contain("prefers-reduced-motion: reduce"));
    }

    [Test]
    public void No_shell_source_outside_the_token_layer_hard_codes_a_breakpoint_width()
    {
        var allowed = ShellStylesheets.Absolute(BreakpointTokenSource);
        var forbidden = new[]
        {
            LtBreakpoints.MediumMinimumWidth.ToString(CultureInfo.InvariantCulture),
            LtBreakpoints.ExpandedMinimumWidth.ToString(CultureInfo.InvariantCulture),
        };

        var violations = new List<string>();
        var scanned = 0;
        foreach (var pattern in new[] { "*.cs", "*.razor", "*.js" })
        {
            foreach (var file in HygieneRepository.EnumerateFiles(ShellStylesheets.Absolute(ShellSourceRoot), pattern))
            {
                scanned++;
                if (string.Equals(file, allowed, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var lines = File.ReadAllLines(file);
                for (var i = 0; i < lines.Length; i++)
                {
                    if (MentionsWidth(lines[i]) && forbidden.Any(width => ContainsStandaloneNumber(lines[i], width)))
                    {
                        violations.Add($"{Relative(file)}:{i + 1}: {lines[i].Trim()}");
                    }
                }
            }
        }

        Assert.That(scanned, Is.GreaterThan(10), "the scan must reach the Shell's sources");
        Assert.That(violations, Is.Empty,
            "Use LtBreakpoints instead of restating a width." + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void Resolve_names_each_width_band()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LtBreakpoints.Resolve(-1), Is.EqualTo(LtBreakpoint.Compact));
            Assert.That(LtBreakpoints.Resolve(0), Is.EqualTo(LtBreakpoint.Compact));
            Assert.That(LtBreakpoints.Resolve(LtBreakpoints.MediumMinimumWidth - 0.5), Is.EqualTo(LtBreakpoint.Compact));
            Assert.That(LtBreakpoints.Resolve(LtBreakpoints.MediumMinimumWidth), Is.EqualTo(LtBreakpoint.Medium));
            Assert.That(LtBreakpoints.Resolve(LtBreakpoints.ExpandedMinimumWidth - 1), Is.EqualTo(LtBreakpoint.Medium));
            Assert.That(LtBreakpoints.Resolve(LtBreakpoints.ExpandedMinimumWidth), Is.EqualTo(LtBreakpoint.Expanded));
        });
    }

    [Test]
    public void The_scanner_detects_the_width_queries_it_is_shown()
    {
        // Battery test for the smoke detector.
        Assert.Multiple(() =>
        {
            Assert.That(DimensionalQuery.IsMatch("@media (min-width: 600px) {"), Is.True);
            Assert.That(DimensionalQuery.IsMatch("@media (width >= 600px) {"), Is.True);
            Assert.That(DimensionalQuery.IsMatch("@container lt-viewport (width < 768px) {"), Is.True);
            Assert.That(DimensionalQuery.IsMatch("@container card (min-inline-size: 20rem) {"), Is.True);
            Assert.That(DimensionalQuery.IsMatch("@media (prefers-reduced-motion: reduce) {"), Is.False);
            Assert.That(DimensionalQuery.IsMatch("@media (forced-colors: active) {"), Is.False);
            Assert.That(DimensionalQuery.IsMatch("@media (hover: none) {"), Is.False);
            Assert.That(ContainsStandaloneNumber("if (viewportWidth >= 768)", "768"), Is.True);
            Assert.That(ContainsStandaloneNumber("var w = 17680;", "768"), Is.False);
        });
    }

    private static bool MentionsWidth(string line) =>
        line.Contains("width", StringComparison.OrdinalIgnoreCase)
        || line.Contains("matchMedia", StringComparison.OrdinalIgnoreCase)
        || line.Contains("breakpoint", StringComparison.OrdinalIgnoreCase);

    private static bool ContainsStandaloneNumber(string line, string number)
    {
        var index = 0;
        while ((index = line.IndexOf(number, index, StringComparison.Ordinal)) >= 0)
        {
            var afterIndex = index + number.Length;
            var before = index == 0 || !char.IsDigit(line[index - 1]);
            var after = afterIndex >= line.Length || !char.IsDigit(line[afterIndex]);
            if (before && after)
            {
                return true;
            }

            index = afterIndex;
        }

        return false;
    }

    private static string Relative(string file) =>
        Path.GetRelativePath(HygieneRepository.FindRepoRoot(), file).Replace('\\', '/');
}
