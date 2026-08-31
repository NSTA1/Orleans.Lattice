using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The settings affordance: discoverable, keyboard-operable, and offering all
/// four choices the issue asks for across the two axes the token layer actually
/// has.
/// </summary>
/// <remarks>
/// A pure component test over stub services - no cluster, host or channel - so it
/// carries no slow category.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppearanceSettingsBunitTests : BunitContext
{
    [Test]
    public void The_affordance_says_what_it_is()
    {
        // The one job it has: be findable by somebody who has never opened the
        // Explorer. An unlabelled glyph beside "Sign out" is how the tenant
        // control became unfindable.
        Configure();

        var cut = Render<AppearanceSettings>();

        Assert.That(cut.Find(".lx-appearance-trigger").TextContent.Trim(), Is.EqualTo("Appearance"));
    }

    [Test]
    public void The_trigger_is_a_button_that_declares_the_panel_it_controls()
    {
        Configure();

        var cut = Render<AppearanceSettings>();
        var trigger = cut.Find(".lx-appearance-trigger");

        Assert.Multiple(() =>
        {
            Assert.That(trigger.TagName, Is.EqualTo("BUTTON"), "keyboard operation must be the platform's");
            Assert.That(trigger.GetAttribute("type"), Is.EqualTo("button"));
            Assert.That(trigger.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(trigger.GetAttribute("aria-controls"), Is.EqualTo(cut.Find(".lx-appearance-panel").Id));
        });
    }

    [Test]
    public void All_four_offered_choices_are_present_across_two_axes()
    {
        // The issue asks for follow-system, light, dark and high contrast. High
        // contrast is the contrast axis, not a fourth palette, because the token
        // layer layers it over whichever palette is active.
        Configure();

        var cut = Render<AppearanceSettings>();
        var labels = cut.FindAll(".lx-appearance-option-label")
            .Select(static node => node.TextContent.Trim())
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(labels, Does.Contain("Match my system"));
            Assert.That(labels, Does.Contain("Light"));
            Assert.That(labels, Does.Contain("Dark"));
            Assert.That(labels, Does.Contain("High contrast"));
        });
    }

    [Test]
    public void Every_density_preset_is_offered_alongside_the_adaptive_default()
    {
        Configure();

        var cut = Render<AppearanceSettings>();
        var labels = cut.FindAll(".lx-appearance-option-label")
            .Select(static node => node.TextContent.Trim())
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(labels, Does.Contain("Match the layout"));
            Assert.That(labels, Does.Contain("Comfortable"));
            Assert.That(labels, Does.Contain("Cosy"));
            Assert.That(labels, Does.Contain("Compact"));
        });
    }

    [Test]
    public void The_three_axes_are_labelled_groups_of_native_radios()
    {
        // Native fieldset/legend and native radios, so arrow-key navigation and
        // the group's accessible name are the platform's rather than a
        // re-implementation, and they work before any script has run.
        Configure();

        var cut = Render<AppearanceSettings>();

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("fieldset.lx-appearance-group legend").Select(static n => n.TextContent.Trim()),
                Is.EquivalentTo(new[] { "Theme", "Contrast", "Density" }));
            Assert.That(
                cut.FindAll(".lx-appearance-radio").Select(static n => n.GetAttribute("type")),
                Has.All.EqualTo("radio"));
        });
    }

    [Test]
    public void Each_axis_is_its_own_radio_group()
    {
        // Sharing a group name would make choosing a density clear the theme.
        Configure();

        var cut = Render<AppearanceSettings>();

        Assert.That(
            cut.FindAll(".lx-appearance-radio").Select(static n => n.GetAttribute("name")).Distinct().Count(),
            Is.EqualTo(3));
    }

    [Test]
    public void Two_instances_do_not_share_a_radio_group()
    {
        Configure();

        var first = Render<AppearanceSettings>(p => p.Add(c => c.Id, "one"));
        var second = Render<AppearanceSettings>(p => p.Add(c => c.Id, "two"));

        Assert.That(
            first.Find(".lx-appearance-radio").GetAttribute("name"),
            Is.Not.EqualTo(second.Find(".lx-appearance-radio").GetAttribute("name")));
    }

    [Test]
    public void With_nothing_chosen_every_axis_shows_as_following_the_environment()
    {
        Configure();

        var cut = Render<AppearanceSettings>();

        Assert.That(
            cut.FindAll(".lx-appearance-option.is-selected .lx-appearance-option-label")
                .Select(static node => node.TextContent.Trim()),
            Is.EquivalentTo(new[] { "Match my system", "Match my system", "Match the layout" }));
    }

    [Test]
    public void Clicking_the_trigger_opens_and_closes_the_panel()
    {
        Configure();

        var cut = Render<AppearanceSettings>();
        cut.Find(".lx-appearance-trigger").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lx-appearance-trigger").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.False);
        });

        cut.Find(".lx-appearance-trigger").Click();

        Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.True);
    }

    [Test]
    public void Escape_closes_an_open_panel()
    {
        Configure();

        var cut = Render<AppearanceSettings>();
        cut.Find(".lx-appearance-trigger").Click();

        cut.Find(".lx-appearance").KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = "Escape" });

        Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.True);
    }

    [Test]
    public void Escape_on_a_closed_panel_does_nothing()
    {
        Configure();

        var cut = Render<AppearanceSettings>();

        cut.Find(".lx-appearance").KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = "Escape" });

        Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.True);
    }

    [Test]
    public void Another_key_does_not_close_the_panel()
    {
        Configure();

        var cut = Render<AppearanceSettings>();
        cut.Find(".lx-appearance-trigger").Click();

        cut.Find(".lx-appearance").KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = "a" });

        Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.False);
    }

    [Test]
    public void Opening_the_panel_announces_it_to_the_host()
    {
        // So a shell can close a sibling disclosure, or persist that the panel is
        // open, without owning the trigger.
        Configure();
        var announced = new List<bool>();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.IsOpenChanged, announced.Add));
        cut.Find(".lx-appearance-trigger").Click();

        Assert.That(announced, Is.EqualTo(new[] { true }));
    }

    [Test]
    public void Choosing_a_theme_applies_it_and_shows_it_as_chosen()
    {
        var applier = Configure();

        var cut = Render<AppearanceSettings>();
        cut.Find(".lx-appearance-trigger").Click();
        ChooseOption(cut, "Light");

        Assert.Multiple(() =>
        {
            Assert.That(applier.Last.Theme, Is.EqualTo(ExplorerThemeChoice.Light));
            Assert.That(SelectedLabels(cut), Does.Contain("Light"));
        });
    }

    [Test]
    public void Choosing_high_contrast_sets_the_contrast_axis_and_leaves_the_palette_alone()
    {
        var applier = Configure();

        var cut = Render<AppearanceSettings>();
        cut.Find(".lx-appearance-trigger").Click();
        ChooseOption(cut, "Dark");
        ChooseOption(cut, "High contrast");

        Assert.Multiple(() =>
        {
            Assert.That(applier.Last.Contrast, Is.EqualTo(ExplorerContrastChoice.More));
            Assert.That(applier.Last.Theme, Is.EqualTo(ExplorerThemeChoice.Dark), "the two axes compose");
        });
    }

    [Test]
    public void Choosing_a_density_applies_it()
    {
        var applier = Configure();

        var cut = Render<AppearanceSettings>();
        cut.Find(".lx-appearance-trigger").Click();
        ChooseOption(cut, "Compact");

        Assert.That(applier.Last.Density, Is.EqualTo(ExplorerDensityChoice.Compact));
    }

    [Test]
    public void A_remembered_choice_is_shown_as_chosen_on_first_render()
    {
        var preferences = CreatePreferences();
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerAppearancePreferenceKeys.Theme, "light").GetAwaiter().GetResult();
        Configure(preferences);

        var cut = Render<AppearanceSettings>();

        Assert.That(SelectedLabels(cut), Does.Contain("Light"));
    }

    [Test]
    public void An_unusable_remembered_choice_is_explained_where_it_is_chosen()
    {
        var preferences = CreatePreferences();
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerAppearancePreferenceKeys.Theme, "solarized").GetAwaiter().GetResult();
        Configure(preferences);

        var cut = Render<AppearanceSettings>();

        Assert.Multiple(() =>
        {
            var notice = cut.Find(".lx-appearance-notice");
            Assert.That(notice.GetAttribute("role"), Is.EqualTo("status"), "it must be announced, not merely shown");
            Assert.That(notice.TextContent, Does.Contain(ExplorerAppearancePreferenceKeys.Theme.Description));
        });
    }

    [Test]
    public void With_no_trigger_the_groups_render_in_place()
    {
        // For a host that puts the control on a settings surface of its own
        // rather than in the chrome.
        Configure();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.ShowTrigger, false));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lx-appearance-trigger"), Is.Empty);
            Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.False);
            Assert.That(cut.Find(".lx-appearance").ClassName, Does.Contain("lx-appearance-inline"));
        });
    }

    [Test]
    public void A_host_class_is_appended_rather_than_replacing_the_controls_own()
    {
        Configure();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.Class, "lx-shell-appearance"));

        Assert.That(cut.Find(".lx-appearance").ClassName, Is.EqualTo("lx-appearance lx-shell-appearance"));
    }

    [Test]
    public void The_host_may_name_the_control_itself()
    {
        Configure();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.Label, "Display"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lx-appearance-trigger").TextContent.Trim(), Is.EqualTo("Display"));
            Assert.That(cut.Find(".lx-appearance-panel").GetAttribute("aria-label"), Is.EqualTo("Display"));
        });
    }

    [Test]
    public void The_host_may_open_the_panel_itself()
    {
        Configure();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.IsOpen, true));

        Assert.That(cut.Find(".lx-appearance-panel").HasAttribute("hidden"), Is.False);
    }

    [Test]
    public void A_change_made_elsewhere_is_reflected_without_re_rendering_by_hand()
    {
        var preferences = CreatePreferences();
        Configure(preferences);
        var cut = Render<AppearanceSettings>();

        Services.GetRequiredService<IExplorerAppearance>()
            .SetDensityAsync(ExplorerDensityChoice.Cosy)
            .GetAwaiter()
            .GetResult();

        Assert.That(SelectedLabels(cut), Does.Contain("Cosy"));
    }

    [Test]
    public void Renaming_the_control_updates_every_derived_id()
    {
        // The ids and group names are composed once per parameter set rather than
        // per render, so a host that changes Id must not be left with stale ones.
        Configure();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.Id, "one"));
        cut.Render(p => p.Add(c => c.Id, "two"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lx-appearance-trigger").Id, Is.EqualTo("two-trigger"));
            Assert.That(cut.Find(".lx-appearance-panel").Id, Is.EqualTo("two-panel"));
            Assert.That(cut.Find(".lx-appearance-radio").GetAttribute("name"), Is.EqualTo("two-theme"));
        });
    }

    [Test]
    public void Changing_the_host_class_updates_the_root()
    {
        Configure();

        var cut = Render<AppearanceSettings>(p => p.Add(c => c.Class, "first"));
        cut.Render(p => p.Add(c => c.Class, "second"));

        Assert.That(cut.Find(".lx-appearance").ClassName, Is.EqualTo("lx-appearance second"));
    }

    private static string[] SelectedLabels(IRenderedComponent<AppearanceSettings> cut) =>
        cut.FindAll(".lx-appearance-option.is-selected .lx-appearance-option-label")
            .Select(static node => node.TextContent.Trim())
            .ToArray();

    private static void ChooseOption(IRenderedComponent<AppearanceSettings> cut, string label)
    {
        var option = cut.FindAll(".lx-appearance-option")
            .First(node => node.TextContent.Trim() == label);

        option.QuerySelector(".lx-appearance-radio")!.Change(true);
    }

    private static ExplorerShellPreferences CreatePreferences()
    {
        var catalog = new ExplorerPreferenceCatalog();
        foreach (var key in ExplorerAppearancePreferenceKeys.All)
        {
            catalog.Register(key);
        }

        return new ExplorerShellPreferences(
            new FakeUiPreferenceStore(),
            catalog,
            new FakeExplorerPreferenceScopeProvider());
    }

    private FakeExplorerAppearanceApplier Configure(ExplorerShellPreferences? preferences = null)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        var applier = new FakeExplorerAppearanceApplier();
        Services.AddSingleton<IExplorerAppearance>(
            new ExplorerAppearance(preferences ?? CreatePreferences(), applier));

        return applier;
    }
}
