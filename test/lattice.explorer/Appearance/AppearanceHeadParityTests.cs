using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The no-flash guarantee, and the parity between the two heads that have to
/// honour it. The acceptance criterion this fixture exists for is
/// "the chosen theme is applied on first paint with no flash of the wrong
/// palette", and it is asserted structurally rather than visually - see
/// <see cref="AppearanceHeadParity"/> for why that is the stronger proof.
/// </summary>
[TestFixture]
public sealed class AppearanceHeadParityTests
{
    [Test]
    public void Every_head_applies_the_appearance_before_its_first_paint()
    {
        Assert.Multiple(() =>
        {
            foreach (var (name, source) in AppearanceHeadParity.Heads())
            {
                Assert.That(
                    AppearanceHeadParity.AppliesBeforeFirstPaint(source, out var reason),
                    Is.True,
                    name + " can flash the wrong palette: " + reason);
            }
        });
    }

    [Test]
    public void Every_head_stamps_the_chosen_density_before_any_content()
    {
        // <body> does not exist while <head> is parsing, so the density stamp is
        // the first thing in the body rather than part of the head script. Later
        // and it would shift a layout that had already been measured.
        Assert.Multiple(() =>
        {
            foreach (var (name, source) in AppearanceHeadParity.Heads())
            {
                Assert.That(AppearanceHeadParity.StampsDensityBeforeContent(source), Is.True, name);
            }
        });
    }

    [Test]
    public void Every_head_loads_the_appearance_stylesheet()
    {
        Assert.Multiple(() =>
        {
            foreach (var (name, source) in AppearanceHeadParity.Heads())
            {
                Assert.That(source, Does.Contain(AppearanceHeadParity.StylesheetAsset), name);
            }
        });
    }

    [Test]
    public void The_script_publishes_the_two_entry_points_the_heads_and_the_applier_use()
    {
        var script = AppearanceHeadParity.Script();

        Assert.Multiple(() =>
        {
            Assert.That(script, Does.Match(@"window\.latticeAppearance\s*=\s*\{"));
            Assert.That(script, Does.Match(@"apply\s*:\s*apply"));
            Assert.That(script, Does.Match(@"stamp\s*:\s*stampBody"));
        });
    }

    [Test]
    public void The_applier_calls_the_function_the_script_actually_publishes()
    {
        // The one name the .NET side and the script have to agree on.
        Assert.That(ExplorerAppearanceApplier.ApplyFunction, Is.EqualTo("latticeAppearance.apply"));
    }

    [Test]
    public void The_script_permits_exactly_the_values_the_applier_can_send()
    {
        // The script validates against a fixed allow-list rather than trusting
        // its cache, so a value the applier can produce and the script rejects
        // would silently do nothing, and a value the script permits and the
        // applier never sends is a rule with no palette behind it.
        Assert.Multiple(() =>
        {
            Assert.That(
                Permitted(nameof(ExplorerThemeChoice)),
                Is.EquivalentTo(Attributes(Enum.GetValues<ExplorerThemeChoice>(), ExplorerAppearanceNames.ThemeAttribute)));
            Assert.That(
                Permitted(nameof(ExplorerContrastChoice)),
                Is.EquivalentTo(Attributes(Enum.GetValues<ExplorerContrastChoice>(), ExplorerAppearanceNames.ContrastAttribute)));
            Assert.That(
                Permitted(nameof(ExplorerDensityChoice)),
                Is.EquivalentTo(Attributes(Enum.GetValues<ExplorerDensityChoice>(), ExplorerAppearanceNames.DensityAttribute)));
        });
    }

    [Test]
    public void The_script_resolves_an_unpinned_palette_from_the_environment()
    {
        // The token layer deliberately declares no prefers-color-scheme query, so
        // "follow the system" is resolved here or nowhere - and dark stays the
        // answer when the environment expresses no preference.
        var script = AppearanceHeadParity.Script();

        Assert.Multiple(() =>
        {
            Assert.That(script, Does.Contain("prefers-color-scheme: light"));
            Assert.That(script, Does.Match(@"current\.theme\s*\|\|\s*\(prefersLight\(\)\s*\?\s*""light""\s*:\s*""dark""\)"));
            Assert.That(
                script,
                Does.Contain("addEventListener(\"change\""),
                "a system theme switched while the Explorer is running must be followed");
        });
    }

    [Test]
    public void The_script_puts_each_attribute_where_the_stylesheets_select_on_it()
    {
        var script = AppearanceHeadParity.Script();

        Assert.Multiple(() =>
        {
            Assert.That(script, Does.Match(@"setAttribute\(root,\s*""data-theme"""));
            Assert.That(script, Does.Match(@"setAttribute\(root,\s*""data-contrast"""));
            Assert.That(script, Does.Match(@"setAttribute\(document\.body,\s*""data-lx-density"""));
        });
    }

    [Test]
    public void The_deferral_rule_hands_every_adaptive_root_the_chosen_density()
    {
        // Without it the nearest adaptive root, which always carries its own
        // breakpoint-derived data-lx-density, would out-declare the operator's
        // choice and the choice would do nothing.
        var block = DeferralBlock();

        Assert.Multiple(() =>
        {
            Assert.That(block, Is.Not.Null, "body[data-lx-density] [data-lx-density] must be declared");
            foreach (var token in DensityTokens)
            {
                Assert.That(
                    block,
                    Does.Match(Regex.Escape(token) + @"\s*:\s*inherit\s*;"),
                    token + " must defer to the ancestor rather than restate a value");
            }
        });
    }

    [Test]
    public void The_deferral_rule_restates_none_of_the_presets_values()
    {
        // The presets stay declared in exactly one place, so a retune of the
        // token layer cannot leave this feature behind.
        var block = DeferralBlock()!;

        Assert.That(
            Regex.Matches(block, @":(?!\s*inherit\s*;)[^;]+;"),
            Is.Empty,
            "the deferral block must declare nothing but `inherit`");
    }

    [Test]
    public void The_deferral_rule_out_ranks_the_token_layers_presets()
    {
        // Specificity, counted: the deferral selector is one element name plus
        // two attribute selectors (0,2,1); each preset is a single attribute
        // selector (0,1,0). The choice therefore wins wherever both apply.
        var presets = Regex.Matches(
            AppearanceHeadParity.TokenStylesheet(),
            @"(?<selector>\[data-lx-density=""[a-z]+""\])\s*\{");

        Assert.Multiple(() =>
        {
            Assert.That(presets, Is.Not.Empty, "the token layer must still declare the presets");
            foreach (Match preset in presets)
            {
                Assert.That(
                    preset.Groups["selector"].Value,
                    Does.Not.Contain(" "),
                    "a preset selector must stay a bare attribute selector for the deferral rule to out-rank it");
            }

            Assert.That(
                AppearanceHeadParity.Stylesheet(),
                Does.Match(@"body\[data-lx-density\]\s+\[data-lx-density\]\s*\{"));
        });
    }

    [Test]
    public void The_deferral_rule_only_applies_once_a_density_has_been_chosen()
    {
        // With no choice the rule cannot match, so the adaptive behaviour the
        // shell shipped with is untouched. That is the whole of "the
        // breakpoint-derived value becomes the default, not the only option".
        Assert.That(
            AppearanceHeadParity.Stylesheet(),
            Does.Match(@"body\[data-lx-density\]"),
            "the rule must be conditioned on the body carrying an explicit choice");
    }

    [Test]
    public void The_scanner_detects_every_way_of_reintroducing_the_flash()
    {
        // Battery test for the smoke detector. Each of these is a change somebody
        // could make to a head believing it harmless.
        const string Sound = """
            <html><head><script src="_content/Orleans.Lattice.Explorer.UI/lattice-appearance.js"></script></head>
            <body><script>latticeAppearance.stamp()</script></body></html>
            """;

        Assert.Multiple(() =>
        {
            Assert.That(AppearanceHeadParity.AppliesBeforeFirstPaint(Sound, out _), Is.True, "the sound head");

            Assert.That(
                AppearanceHeadParity.AppliesBeforeFirstPaint(Sound.Replace("<script src", "<script defer src"), out _),
                Is.False,
                "deferring it");
            Assert.That(
                AppearanceHeadParity.AppliesBeforeFirstPaint(Sound.Replace("<script src", "<script async src"), out _),
                Is.False,
                "making it async");
            Assert.That(
                AppearanceHeadParity.AppliesBeforeFirstPaint(
                    Sound.Replace("<script src", "<script type=\"module\" src"),
                    out _),
                Is.False,
                "making it a module");
            Assert.That(
                AppearanceHeadParity.AppliesBeforeFirstPaint(
                    "<html><head></head><body><script src=\"" + AppearanceHeadParity.ScriptAsset + "\"></script></body></html>",
                    out _),
                Is.False,
                "moving it into the body");
            Assert.That(
                AppearanceHeadParity.AppliesBeforeFirstPaint("<html><head></head><body></body></html>", out _),
                Is.False,
                "dropping it");
        });
    }

    [Test]
    public void The_scanner_detects_a_density_stamp_that_has_slipped_below_the_content()
    {
        const string Late = """
            <html><head></head><body><div class="lx-shell"></div><script>latticeAppearance.stamp()</script></body></html>
            """;

        Assert.Multiple(() =>
        {
            Assert.That(AppearanceHeadParity.StampsDensityBeforeContent(Late), Is.False, "below the content");
            Assert.That(
                AppearanceHeadParity.StampsDensityBeforeContent("<html><head></head><body></body></html>"),
                Is.False,
                "absent entirely");
        });
    }

    private static readonly string[] DensityTokens =
    [
        "--lx-density-row-height",
        "--lx-density-padding-block",
        "--lx-density-padding-inline",
        "--lx-density-gap",
    ];

    private static string? DeferralBlock()
    {
        var match = Regex.Match(
            AppearanceHeadParity.Stylesheet(),
            @"body\[data-lx-density\]\s+\[data-lx-density\]\s*\{(?<body>[^}]*)\}");

        return match.Success ? match.Groups["body"].Value : null;
    }

    private static string[] Permitted(string axis)
    {
        // The script names its allow-lists after the axis they guard, so the
        // mapping from a C# enum to the table that must agree with it is by name
        // rather than by position.
        var table = axis switch
        {
            nameof(ExplorerThemeChoice) => "THEMES",
            nameof(ExplorerContrastChoice) => "CONTRASTS",
            _ => "DENSITIES",
        };

        var match = Regex.Match(AppearanceHeadParity.Script(), @"var\s+" + table + @"\s*=\s*\{(?<body>[^}]*)\}");
        Assert.That(match.Success, Is.True, table + " must be declared in the bootstrap script");

        return Regex.Matches(match.Groups["body"].Value, @"(?<name>[a-z]+)\s*:")
            .Select(static m => m.Groups["name"].Value)
            .ToArray();
    }

    private static string[] Attributes<T>(T[] choices, Func<T, string?> attribute) =>
        choices.Select(attribute).OfType<string>().ToArray();
}
