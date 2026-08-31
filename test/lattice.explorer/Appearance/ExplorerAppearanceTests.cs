using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The appearance state machine: what it restores, what it persists, what it
/// applies, and what it does when the remembered value is not one it knows.
/// </summary>
[TestFixture]
public sealed class ExplorerAppearanceTests
{
    [Test]
    public void Construction_rejects_missing_collaborators()
    {
        var preferences = CreatePreferences();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new ExplorerAppearance(null!, new FakeExplorerAppearanceApplier()),
                Throws.TypeOf<ArgumentNullException>());
            Assert.That(
                () => new ExplorerAppearance(preferences, null!),
                Throws.TypeOf<ArgumentNullException>());
        });
    }

    [Test]
    public void A_host_theme_is_optional()
    {
        // The web head registers none: the browser answers prefers-color-scheme
        // in the document itself, so there is nothing for the server to say.
        using var appearance = new ExplorerAppearance(
            CreatePreferences(),
            new FakeExplorerAppearanceApplier(),
            hostTheme: null);

        Assert.That(appearance.Effective.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
    }

    [Test]
    public void Before_anything_is_loaded_every_axis_follows_the_environment()
    {
        using var appearance = new ExplorerAppearance(CreatePreferences(), new FakeExplorerAppearanceApplier());

        Assert.Multiple(() =>
        {
            Assert.That(appearance.IsLoaded, Is.False);
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
            Assert.That(appearance.Contrast, Is.EqualTo(ExplorerContrastChoice.FollowSystem));
            Assert.That(appearance.Density, Is.EqualTo(ExplorerDensityChoice.FollowLayout));
            Assert.That(appearance.Effective.IsFollowingEverything, Is.True);
            Assert.That(appearance.Notice, Is.Null);
        });
    }

    [Test]
    public async Task Loading_with_nothing_stored_applies_the_out_of_the_box_appearance()
    {
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(CreatePreferences(), applier);

        await appearance.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.IsLoaded, Is.True);
            Assert.That(applier.Applied, Has.Count.EqualTo(1));
            Assert.That(applier.Last.IsFollowingEverything, Is.True);
        });
    }

    [Test]
    public async Task Loading_restores_and_applies_every_remembered_choice()
    {
        var preferences = CreatePreferences();
        await Seed(preferences, "light", "more", "compact");

        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(preferences, applier);

        await appearance.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.Light));
            Assert.That(appearance.Contrast, Is.EqualTo(ExplorerContrastChoice.More));
            Assert.That(appearance.Density, Is.EqualTo(ExplorerDensityChoice.Compact));
            Assert.That(
                applier.Last,
                Is.EqualTo(new ExplorerAppearanceState(
                    ExplorerThemeChoice.Light,
                    ExplorerContrastChoice.More,
                    ExplorerDensityChoice.Compact)));
            Assert.That(appearance.Notice, Is.Null);
        });
    }

    [Test]
    public async Task Loading_is_idempotent()
    {
        // A component hydrates in initialization and again after the first
        // render, because a prerender pass cannot reach browser storage.
        var preferences = CreatePreferences();
        await Seed(preferences, "dark", null, null);
        using var appearance = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());

        await appearance.EnsureLoadedAsync();
        await appearance.EnsureLoadedAsync();

        Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.Dark));
    }

    [Test]
    public async Task An_unknown_stored_choice_is_explained_forgotten_and_replaced_by_following()
    {
        // A name a newer build wrote, or a corrupted entry. It must not wedge the
        // console, must be said out loud once, and must not resurface after that.
        var preferences = CreatePreferences();
        await Seed(preferences, "solarized", null, null);

        using var appearance = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());
        await appearance.EnsureLoadedAsync();

        var explained = appearance.Notice;

        using var second = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());
        await second.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
            Assert.That(explained, Is.Not.Null.And.Not.Empty);
            Assert.That(
                explained,
                Does.Contain(ExplorerAppearancePreferenceKeys.Theme.Description),
                "the explanation names what could not be restored");
            Assert.That(second.Notice, Is.Null, "a forgotten value must not be re-explained");
        });
    }

    [Test]
    public async Task An_unknown_stored_choice_on_one_axis_leaves_the_others_alone()
    {
        var preferences = CreatePreferences();
        await Seed(preferences, "light", "sideways", "compact");

        using var appearance = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());
        await appearance.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.Light));
            Assert.That(appearance.Contrast, Is.EqualTo(ExplorerContrastChoice.FollowSystem));
            Assert.That(appearance.Density, Is.EqualTo(ExplorerDensityChoice.Compact));
            Assert.That(appearance.Notice, Is.Not.Null);
        });
    }

    [Test]
    public async Task Choosing_a_theme_applies_it_remembers_it_and_announces_it()
    {
        var preferences = CreatePreferences();
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(preferences, applier);
        await appearance.EnsureLoadedAsync();

        var announced = 0;
        appearance.Changed += () => announced++;

        await appearance.SetThemeAsync(ExplorerThemeChoice.Light);

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.Light));
            Assert.That(applier.Last.Theme, Is.EqualTo(ExplorerThemeChoice.Light));
            Assert.That(announced, Is.EqualTo(1));
            Assert.That(
                preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Theme, string.Empty),
                Is.EqualTo("light"));
        });
    }

    [Test]
    public async Task Choosing_a_contrast_applies_it_and_remembers_it()
    {
        var preferences = CreatePreferences();
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(preferences, applier);
        await appearance.EnsureLoadedAsync();

        await appearance.SetContrastAsync(ExplorerContrastChoice.More);

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Contrast, Is.EqualTo(ExplorerContrastChoice.More));
            Assert.That(applier.Last.Contrast, Is.EqualTo(ExplorerContrastChoice.More));
            Assert.That(
                preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Contrast, string.Empty),
                Is.EqualTo("more"));
        });
    }

    [Test]
    public async Task Choosing_a_density_applies_it_and_remembers_it()
    {
        var preferences = CreatePreferences();
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(preferences, applier);
        await appearance.EnsureLoadedAsync();

        await appearance.SetDensityAsync(ExplorerDensityChoice.Comfortable);

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Density, Is.EqualTo(ExplorerDensityChoice.Comfortable));
            Assert.That(applier.Last.Density, Is.EqualTo(ExplorerDensityChoice.Comfortable));
            Assert.That(
                preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Density, string.Empty),
                Is.EqualTo("comfortable"));
        });
    }

    [Test]
    public async Task Choosing_to_follow_the_environment_is_remembered_as_a_choice()
    {
        // Not the same as never having chosen: it has to survive a reload, and it
        // has to survive a later change to what the default is.
        var preferences = CreatePreferences();
        using var appearance = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());
        await appearance.EnsureLoadedAsync();
        await appearance.SetThemeAsync(ExplorerThemeChoice.Dark);

        await appearance.SetThemeAsync(ExplorerThemeChoice.FollowSystem);

        Assert.Multiple(() =>
        {
            Assert.That(
                preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Theme, string.Empty),
                Is.EqualTo(ExplorerAppearanceNames.FollowSystemName));
            Assert.That(appearance.Effective.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
        });
    }

    [Test]
    public async Task Setting_an_undeclared_choice_is_rejected_before_anything_is_written()
    {
        var preferences = CreatePreferences();
        using var appearance = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());
        await appearance.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => appearance.SetThemeAsync((ExplorerThemeChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => appearance.SetContrastAsync((ExplorerContrastChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => appearance.SetDensityAsync((ExplorerDensityChoice)99),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
        });
    }

    [Test]
    public void A_host_theme_resolves_follow_the_system_to_a_real_palette()
    {
        // What makes the desktop head sensible against Windows: "follow system"
        // there means the application's own theme, not whatever the embedded web
        // view reports.
        var host = new FakeExplorerHostTheme(ExplorerHostThemePreference.Light);
        using var appearance = new ExplorerAppearance(
            CreatePreferences(),
            new FakeExplorerAppearanceApplier(),
            host);

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem), "the choice is untouched");
            Assert.That(appearance.Effective.Theme, Is.EqualTo(ExplorerThemeChoice.Light), "the document is not");
        });
    }

    [Test]
    public async Task An_explicit_palette_outranks_the_host_theme()
    {
        var host = new FakeExplorerHostTheme(ExplorerHostThemePreference.Light);
        using var appearance = new ExplorerAppearance(
            CreatePreferences(),
            new FakeExplorerAppearanceApplier(),
            host);
        await appearance.EnsureLoadedAsync();

        await appearance.SetThemeAsync(ExplorerThemeChoice.Dark);

        Assert.That(appearance.Effective.Theme, Is.EqualTo(ExplorerThemeChoice.Dark));
    }

    [Test]
    public async Task A_host_theme_switch_reapplies_and_announces()
    {
        // The operating system's theme changed while the Explorer was running.
        var host = new FakeExplorerHostTheme(ExplorerHostThemePreference.Light);
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(CreatePreferences(), applier, host);
        await appearance.EnsureLoadedAsync();

        var announced = 0;
        appearance.Changed += () => announced++;

        host.MoveTo(ExplorerHostThemePreference.Dark);

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Effective.Theme, Is.EqualTo(ExplorerThemeChoice.Dark));
            Assert.That(applier.Last.Theme, Is.EqualTo(ExplorerThemeChoice.Dark));
            Assert.That(announced, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_host_that_stops_having_an_opinion_hands_the_answer_back_to_the_document()
    {
        var host = new FakeExplorerHostTheme(ExplorerHostThemePreference.Dark);
        using var appearance = new ExplorerAppearance(
            CreatePreferences(),
            new FakeExplorerAppearanceApplier(),
            host);
        await appearance.EnsureLoadedAsync();

        host.MoveTo(ExplorerHostThemePreference.Unspecified);

        Assert.That(appearance.Effective.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
    }

    [Test]
    public async Task A_reset_returns_the_appearance_to_following_the_environment()
    {
        var preferences = CreatePreferences();
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(preferences, applier);
        await appearance.EnsureLoadedAsync();
        await appearance.SetThemeAsync(ExplorerThemeChoice.Light);
        await appearance.SetDensityAsync(ExplorerDensityChoice.Compact);

        await preferences.ResetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
            Assert.That(appearance.Density, Is.EqualTo(ExplorerDensityChoice.FollowLayout));
            Assert.That(applier.Last.IsFollowingEverything, Is.True);
        });
    }

    [Test]
    public async Task Signing_in_as_somebody_else_re_reads_and_re_applies()
    {
        var scope = new FakeExplorerPreferenceScopeProvider();
        var preferences = CreatePreferences(scope: scope);
        var applier = new FakeExplorerAppearanceApplier();
        using var appearance = new ExplorerAppearance(preferences, applier);
        await appearance.EnsureLoadedAsync();
        await appearance.SetThemeAsync(ExplorerThemeChoice.Light);

        scope.MoveTo("bob", "https://cluster-a");

        Assert.Multiple(() =>
        {
            Assert.That(
                appearance.Theme,
                Is.EqualTo(ExplorerThemeChoice.FollowSystem),
                "another identity must not inherit this one's palette");
            Assert.That(applier.Last.Theme, Is.EqualTo(ExplorerThemeChoice.FollowSystem));
        });
    }

    [Test]
    public async Task A_palette_follows_the_operator_between_clusters()
    {
        // The reason the appearance keys are User-scoped rather than the
        // contract's usual UserAndCluster: a palette is a property of the person
        // and the room, not of the cluster they happen to be pointed at.
        var scope = new FakeExplorerPreferenceScopeProvider();
        var preferences = CreatePreferences(scope: scope);
        using var appearance = new ExplorerAppearance(preferences, new FakeExplorerAppearanceApplier());
        await appearance.EnsureLoadedAsync();
        await appearance.SetThemeAsync(ExplorerThemeChoice.Light);

        scope.MoveTo("alice", "https://cluster-b");

        Assert.That(appearance.Theme, Is.EqualTo(ExplorerThemeChoice.Light));
    }

    [Test]
    public async Task An_applier_that_faults_never_reaches_the_caller()
    {
        // Appearance is cosmetic. A head that cannot reach its document keeps a
        // usable shell rather than failing to load.
        var faulting = new FakeExplorerAppearanceApplier { Fault = new InvalidOperationException("no document") };
        var scope = new FakeExplorerPreferenceScopeProvider();
        using var appearance = new ExplorerAppearance(CreatePreferences(scope: scope), faulting);

        scope.MoveTo("bob", "https://cluster-a");

        Assert.That(faulting.Applied, Is.Not.Empty);
    }

    [Test]
    public async Task Disposing_stops_listening_to_the_contract_and_the_host()
    {
        var scope = new FakeExplorerPreferenceScopeProvider();
        var host = new FakeExplorerHostTheme(ExplorerHostThemePreference.Light);
        var applier = new FakeExplorerAppearanceApplier();
        var appearance = new ExplorerAppearance(CreatePreferences(scope: scope), applier, host);
        await appearance.EnsureLoadedAsync();
        var appliedBefore = applier.Applied.Count;

        appearance.Dispose();
        scope.MoveTo("bob", "https://cluster-a");
        host.MoveTo(ExplorerHostThemePreference.Dark);

        Assert.That(applier.Applied, Has.Count.EqualTo(appliedBefore));
    }

    private static ExplorerShellPreferences CreatePreferences(
        FakeExplorerPreferenceScopeProvider? scope = null)
    {
        var catalog = new ExplorerPreferenceCatalog();
        foreach (var key in ExplorerAppearancePreferenceKeys.All)
        {
            catalog.Register(key);
        }

        return new ExplorerShellPreferences(
            new FakeUiPreferenceStore(),
            catalog,
            scope ?? new FakeExplorerPreferenceScopeProvider());
    }

    private static async Task Seed(
        ExplorerShellPreferences preferences,
        string? theme,
        string? contrast,
        string? density)
    {
        await preferences.EnsureLoadedAsync();

        if (theme is not null)
        {
            await preferences.SetAsync(ExplorerAppearancePreferenceKeys.Theme, theme);
        }

        if (contrast is not null)
        {
            await preferences.SetAsync(ExplorerAppearancePreferenceKeys.Contrast, contrast);
        }

        if (density is not null)
        {
            await preferences.SetAsync(ExplorerAppearancePreferenceKeys.Density, density);
        }
    }
}
