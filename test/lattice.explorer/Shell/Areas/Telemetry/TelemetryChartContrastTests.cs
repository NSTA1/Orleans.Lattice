using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.Tests.Shell.Design;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Telemetry;

/// <summary>
/// The Telemetry chart series tokens, measured in both materials and under the
/// high-contrast overlay (WCAG 2.2 SC 1.4.11), and the stylesheet's promises: no
/// new hue (the series alias ink and the diagram's concurrent-write blue), and no
/// motion, so reduced motion is honoured by construction.
/// </summary>
[TestFixture]
public sealed class TelemetryChartContrastTests
{
    private const string Stylesheet = ShellStylesheets.WebRoot + "/telemetry/telemetry.css";

    private static readonly string[] SeriesTokens = ["--lt-telemetry-series-ink", "--lt-telemetry-series-blue"];

    [Test]
    public void The_series_are_ink_and_the_diagrams_concurrent_blue_never_a_new_hue()
    {
        var tokens = ChartTokens();

        Assert.Multiple(() =>
        {
            Assert.That(tokens["--lt-telemetry-series-ink"], Is.EqualTo("var(--lt-ink)"));
            Assert.That(tokens["--lt-telemetry-series-blue"], Is.EqualTo("var(--lt-diagram-concurrent)"));
            Assert.That(tokens.Values, Has.None.Matches<string>(value => Regex.IsMatch(value, "#[0-9a-fA-F]{3,8}|rgba?\\(|hsla?\\(")));
        });
    }

    [TestCase(ShellPalette.Paper, ShellStylesheets.NonTextMinimum)]
    [TestCase(ShellPalette.Board, ShellStylesheets.NonTextMinimum)]
    [TestCase(ShellPalette.PaperMore, ShellStylesheets.NonTextEnhancedMinimum)]
    [TestCase(ShellPalette.BoardMore, ShellStylesheets.NonTextEnhancedMinimum)]
    public void Every_series_pigment_clears_the_non_text_minimum_on_every_surface(ShellPalette palette, double minimum)
    {
        var resolved = Resolve(palette);
        var failures = new List<string>();
        var measured = 0;

        foreach (var token in SeriesTokens)
        {
            var colour = ShellStylesheets.Colour(resolved, palette, token);
            foreach (var surface in ShellStylesheets.Surfaces)
            {
                measured++;
                var ratio = ShellStylesheets.ContrastRatio(colour, ShellStylesheets.Colour(resolved, palette, surface));
                if (ratio < minimum)
                {
                    failures.Add($"{token} ({colour}) on {surface} is {ShellStylesheets.Format(ratio)}");
                }
            }
        }

        Assert.That(measured, Is.EqualTo(SeriesTokens.Length * ShellStylesheets.Surfaces.Length));
        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures));
    }

    [TestCase(ShellPalette.Paper, "#15191f", "#2234b0")]
    [TestCase(ShellPalette.Board, "#e4e9e5", "#a9b8ff")]
    public void Board_draws_chalk_and_chalk_blue_where_paper_draws_ink_and_link_blue(ShellPalette palette, string ink, string blue)
    {
        var resolved = Resolve(palette);

        Assert.Multiple(() =>
        {
            Assert.That(ShellStylesheets.Colour(resolved, palette, "--lt-telemetry-series-ink"), Is.EqualTo(ink).IgnoreCase);
            Assert.That(ShellStylesheets.Colour(resolved, palette, "--lt-telemetry-series-blue"), Is.EqualTo(blue).IgnoreCase);
        });
    }

    [Test]
    public void Nothing_in_the_area_moves()
    {
        var css = ShellStylesheets.WithoutComments(Stylesheet);

        Assert.Multiple(() =>
        {
            Assert.That(css, Does.Not.Contain("animation"));
            Assert.That(css, Does.Not.Contain("transition"));
            Assert.That(css, Does.Contain("@media (forced-colors: active)"), "forced colours keep every series cue");
        });
    }

    private static IReadOnlyDictionary<string, string> ChartTokens() =>
        ShellStylesheets.Rules(Stylesheet)
            .Where(rule => rule.Selector == ".lt-telemetry-chart" && rule.AtRule.Length == 0)
            .Select(rule => ShellStylesheets.Declarations(rule.Body))
            .Single();

    private static IReadOnlyDictionary<string, string> Resolve(ShellPalette palette)
    {
        var resolved = ShellStylesheets.Palette(palette).ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        foreach (var token in SeriesTokens)
        {
            var alias = Regex.Match(ChartTokens()[token], @"^var\((--[a-z0-9-]+)\)$");
            Assert.That(alias.Success, Is.True, $"{token} must alias one palette token");
            resolved[token] = resolved[alias.Groups[1].Value];
        }

        return resolved;
    }
}
