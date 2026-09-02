using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The resolved appearance value type: a struct, with value equality, whose
/// default is the out-of-the-box "follow everything" state.
/// </summary>
[TestFixture]
public sealed class ExplorerAppearanceStateTests
{
    [Test]
    public void The_default_follows_the_environment_on_every_axis()
    {
        var state = ExplorerAppearanceState.Default;

        Assert.Multiple(() =>
        {
            Assert.That(state.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
            Assert.That(state.Contrast, Is.EqualTo(ExplorerContrastChoice.FollowSystem));
            Assert.That(state.Density, Is.EqualTo(ExplorerDensityChoice.FollowLayout));
            Assert.That(state.IsFollowingEverything, Is.True);
        });
    }

    [Test]
    public void The_default_is_the_zero_value()
    {
        // So a struct that was never assigned is the safe appearance rather than
        // an arbitrary palette.
        Assert.That(default(ExplorerAppearanceState), Is.EqualTo(ExplorerAppearanceState.Default));
    }

    [Test]
    public void A_choice_on_any_single_axis_stops_it_following_everything()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAppearanceState.Default with { Theme = ExplorerThemeChoice.Light },
                Has.Property(nameof(ExplorerAppearanceState.IsFollowingEverything)).False);
            Assert.That(
                ExplorerAppearanceState.Default with { Contrast = ExplorerContrastChoice.Standard },
                Has.Property(nameof(ExplorerAppearanceState.IsFollowingEverything)).False);
            Assert.That(
                ExplorerAppearanceState.Default with { Density = ExplorerDensityChoice.Cosy },
                Has.Property(nameof(ExplorerAppearanceState.IsFollowingEverything)).False);
        });
    }

    [Test]
    public void Two_states_with_the_same_choices_are_equal()
    {
        var first = new ExplorerAppearanceState(
            ExplorerThemeChoice.Dark,
            ExplorerContrastChoice.More,
            ExplorerDensityChoice.Compact);
        var second = new ExplorerAppearanceState(
            ExplorerThemeChoice.Dark,
            ExplorerContrastChoice.More,
            ExplorerDensityChoice.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.GetHashCode(), Is.EqualTo(second.GetHashCode()));
            Assert.That(
                first,
                Is.Not.EqualTo(second with { Density = ExplorerDensityChoice.Cosy }));
        });
    }
}
