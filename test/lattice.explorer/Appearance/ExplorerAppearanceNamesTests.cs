using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The one spelling of every appearance value: what is stored, what goes on the
/// document, and the fact that following the environment is a stored choice but
/// not an attribute.
/// </summary>
[TestFixture]
public sealed class ExplorerAppearanceNamesTests
{
    [Test]
    public void Theme_names_round_trip()
    {
        Assert.Multiple(() =>
        {
            foreach (var choice in Enum.GetValues<ExplorerThemeChoice>())
            {
                var name = ExplorerAppearanceNames.ThemeName(choice);
                Assert.That(ExplorerAppearanceNames.TryParseThemeName(name, out var parsed), Is.True, name);
                Assert.That(parsed, Is.EqualTo(choice));
            }
        });
    }

    [Test]
    public void Contrast_names_round_trip()
    {
        Assert.Multiple(() =>
        {
            foreach (var choice in Enum.GetValues<ExplorerContrastChoice>())
            {
                var name = ExplorerAppearanceNames.ContrastName(choice);
                Assert.That(ExplorerAppearanceNames.TryParseContrastName(name, out var parsed), Is.True, name);
                Assert.That(parsed, Is.EqualTo(choice));
            }
        });
    }

    [Test]
    public void Density_names_round_trip()
    {
        Assert.Multiple(() =>
        {
            foreach (var choice in Enum.GetValues<ExplorerDensityChoice>())
            {
                var name = ExplorerAppearanceNames.DensityName(choice);
                Assert.That(ExplorerAppearanceNames.TryParseDensityName(name, out var parsed), Is.True, name);
                Assert.That(parsed, Is.EqualTo(choice));
            }
        });
    }

    [Test]
    public void Every_stored_name_is_distinct_within_its_axis()
    {
        // A collision would make two choices indistinguishable once stored, and
        // the operator would silently get the wrong one back.
        Assert.Multiple(() =>
        {
            Assert.That(
                Enum.GetValues<ExplorerThemeChoice>().Select(ExplorerAppearanceNames.ThemeName).Distinct().Count(),
                Is.EqualTo(Enum.GetValues<ExplorerThemeChoice>().Length));
            Assert.That(
                Enum.GetValues<ExplorerContrastChoice>().Select(ExplorerAppearanceNames.ContrastName).Distinct().Count(),
                Is.EqualTo(Enum.GetValues<ExplorerContrastChoice>().Length));
            Assert.That(
                Enum.GetValues<ExplorerDensityChoice>().Select(ExplorerAppearanceNames.DensityName).Distinct().Count(),
                Is.EqualTo(Enum.GetValues<ExplorerDensityChoice>().Length));
        });
    }

    [Test]
    public void Following_the_environment_stores_a_name_but_sets_no_attribute()
    {
        // The distinction the whole model rests on: "I chose to follow the
        // system" must survive a reload, while the document must carry no
        // attribute so the environment's own answer is what applies.
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAppearanceNames.ThemeName(ExplorerThemeChoice.FollowSystem),
                Is.EqualTo(ExplorerAppearanceNames.FollowSystemName));
            Assert.That(ExplorerAppearanceNames.ThemeAttribute(ExplorerThemeChoice.FollowSystem), Is.Null);

            Assert.That(
                ExplorerAppearanceNames.ContrastName(ExplorerContrastChoice.FollowSystem),
                Is.EqualTo(ExplorerAppearanceNames.FollowSystemName));
            Assert.That(ExplorerAppearanceNames.ContrastAttribute(ExplorerContrastChoice.FollowSystem), Is.Null);

            Assert.That(
                ExplorerAppearanceNames.DensityName(ExplorerDensityChoice.FollowLayout),
                Is.EqualTo(ExplorerAppearanceNames.FollowLayoutName));
            Assert.That(ExplorerAppearanceNames.DensityAttribute(ExplorerDensityChoice.FollowLayout), Is.Null);
        });
    }

    [Test]
    public void Theme_attributes_are_the_values_the_token_layer_selects_on()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAppearanceNames.ThemeAttribute(ExplorerThemeChoice.Light), Is.EqualTo("light"));
            Assert.That(ExplorerAppearanceNames.ThemeAttribute(ExplorerThemeChoice.Dark), Is.EqualTo("dark"));
        });
    }

    [Test]
    public void High_contrast_is_the_contrast_axis_and_never_a_palette()
    {
        // The constraint issue #1846 fixed the token layer around: contrast layers
        // over whichever palette is active, so there is no high-contrast theme to
        // select and no combinatorial palette set to maintain.
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAppearanceNames.ContrastAttribute(ExplorerContrastChoice.More), Is.EqualTo("more"));
            Assert.That(
                ExplorerAppearanceNames.ContrastAttribute(ExplorerContrastChoice.Standard),
                Is.EqualTo("standard"));
            Assert.That(
                Enum.GetNames<ExplorerThemeChoice>(),
                Has.None.Contains("Contrast"),
                "contrast must not reappear as a theme value");
        });
    }

    [Test]
    public void Density_names_are_the_token_layers_own()
    {
        // Spelled once, in LatticeDensities, so the attribute this feature writes
        // and the preset the token layer declares cannot drift apart.
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAppearanceNames.DensityAttribute(ExplorerDensityChoice.Comfortable),
                Is.EqualTo(LatticeDensities.ComfortableName));
            Assert.That(
                ExplorerAppearanceNames.DensityAttribute(ExplorerDensityChoice.Cosy),
                Is.EqualTo(LatticeDensities.CosyName));
            Assert.That(
                ExplorerAppearanceNames.DensityAttribute(ExplorerDensityChoice.Compact),
                Is.EqualTo(LatticeDensities.CompactName));
        });
    }

    [Test]
    public void Parsing_is_case_insensitive()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAppearanceNames.TryParseThemeName("LIGHT", out var theme), Is.True);
            Assert.That(theme, Is.EqualTo(ExplorerThemeChoice.Light));

            Assert.That(ExplorerAppearanceNames.TryParseContrastName("More", out var contrast), Is.True);
            Assert.That(contrast, Is.EqualTo(ExplorerContrastChoice.More));

            Assert.That(ExplorerAppearanceNames.TryParseDensityName("Compact", out var density), Is.True);
            Assert.That(density, Is.EqualTo(ExplorerDensityChoice.Compact));
        });
    }

    [Test]
    public void An_unknown_stored_name_fails_to_parse_and_falls_back_to_following()
    {
        // A name from a newer build, or a corrupted entry. Falling back to
        // following the environment is the only safe answer: it is the one state
        // that cannot make the console unreadable.
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAppearanceNames.TryParseThemeName("solarized", out var theme), Is.False);
            Assert.That(theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));

            Assert.That(ExplorerAppearanceNames.TryParseContrastName("less", out var contrast), Is.False);
            Assert.That(contrast, Is.EqualTo(ExplorerContrastChoice.FollowSystem));

            Assert.That(ExplorerAppearanceNames.TryParseDensityName("microscopic", out var density), Is.False);
            Assert.That(density, Is.EqualTo(ExplorerDensityChoice.FollowLayout));
        });
    }

    [Test]
    public void A_null_or_empty_name_fails_to_parse()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAppearanceNames.TryParseThemeName(null, out _), Is.False);
            Assert.That(ExplorerAppearanceNames.TryParseContrastName(null, out _), Is.False);
            Assert.That(ExplorerAppearanceNames.TryParseDensityName(null, out _), Is.False);
            Assert.That(ExplorerAppearanceNames.TryParseThemeName(string.Empty, out _), Is.False);
            Assert.That(ExplorerAppearanceNames.TryParseContrastName(string.Empty, out _), Is.False);
            Assert.That(ExplorerAppearanceNames.TryParseDensityName(string.Empty, out _), Is.False);
        });
    }

    [Test]
    public void An_undeclared_choice_is_rejected_rather_than_named()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ExplorerAppearanceNames.ThemeName((ExplorerThemeChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => ExplorerAppearanceNames.ThemeAttribute((ExplorerThemeChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => ExplorerAppearanceNames.ContrastName((ExplorerContrastChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => ExplorerAppearanceNames.ContrastAttribute((ExplorerContrastChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => ExplorerAppearanceNames.DensityName((ExplorerDensityChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => ExplorerAppearanceNames.DensityAttribute((ExplorerDensityChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Naming_a_choice_allocates_nothing()
    {
        // Resolution runs on a render path, so every name must be an interned
        // literal handed back rather than a string composed per call.
        var first = ExplorerAppearanceNames.ThemeName(ExplorerThemeChoice.Light);
        var second = ExplorerAppearanceNames.ThemeName(ExplorerThemeChoice.Light);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(first, second), Is.True);
            Assert.That(
                ReferenceEquals(
                    ExplorerAppearanceNames.DensityAttribute(ExplorerDensityChoice.Compact),
                    ExplorerAppearanceNames.DensityAttribute(ExplorerDensityChoice.Compact)),
                Is.True);
        });
    }
}
