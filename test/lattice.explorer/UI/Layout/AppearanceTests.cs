using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Appearance: the stored names (compatible with what the Explorer has always
/// stored), the preference keys, the state that applies before it remembers, the
/// menu and its controls, and the palette commands that mirror every control.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppearanceTests : ShellChromeTestContext
{
    [Test]
    [TestCase("system", true, "System")]
    [TestCase("light", true, "Paper")]
    [TestCase("DARK", true, "Board")]
    [TestCase("sepia", false, "System")]
    [TestCase(null, false, "System")]
    public void Theme_names_parse(string? name, bool known, string expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellAppearanceNames.TryParseTheme(name, out var theme), Is.EqualTo(known));
            Assert.That(theme, Is.EqualTo(Enum.Parse<ShellTheme>(expected)));
        });
    }

    [Test]
    [TestCase("system", true, "System")]
    [TestCase("standard", true, "Standard")]
    [TestCase("More", true, "More")]
    [TestCase("less", false, "System")]
    public void Contrast_names_parse(string? name, bool known, string expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellAppearanceNames.TryParseContrast(name, out var contrast), Is.EqualTo(known));
            Assert.That(contrast, Is.EqualTo(Enum.Parse<ShellContrast>(expected)));
        });
    }

    [Test]
    [TestCase("comfortable", true, true)]
    [TestCase("compact", true, false)]
    [TestCase("cosy", true, true)]
    [TestCase("layout", true, true)]
    [TestCase("spacious", false, true)]
    [TestCase(null, false, true)]
    public void Density_names_parse_and_legacy_names_read_as_comfortable(string? name, bool known, bool comfortable)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellAppearanceNames.TryParseDensity(name, out var density), Is.EqualTo(known));
            Assert.That(density, Is.EqualTo(comfortable ? LtDensity.Comfortable : LtDensity.Compact));
        });
    }

    [Test]
    public void Every_choice_round_trips_through_its_stored_name()
    {
        Assert.Multiple(() =>
        {
            foreach (var theme in Enum.GetValues<ShellTheme>())
            {
                ShellAppearanceNames.TryParseTheme(ShellAppearanceNames.Name(theme), out var parsed);
                Assert.That(parsed, Is.EqualTo(theme));
            }

            foreach (var contrast in Enum.GetValues<ShellContrast>())
            {
                ShellAppearanceNames.TryParseContrast(ShellAppearanceNames.Name(contrast), out var parsed);
                Assert.That(parsed, Is.EqualTo(contrast));
            }

            foreach (var density in Enum.GetValues<LtDensity>())
            {
                ShellAppearanceNames.TryParseDensity(ShellAppearanceNames.Name(density), out var parsed);
                Assert.That(parsed, Is.EqualTo(density));
            }

            Assert.That(() => ShellAppearanceNames.Name((ShellTheme)99), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => ShellAppearanceNames.Name((ShellContrast)99), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void The_keys_are_the_documented_appearance_keys_remembered_per_user()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellAppearancePreferenceKeys.All.Select(key => key.Name),
                Is.EqualTo(new[] { "appearance.theme", "appearance.contrast", "appearance.density" }));
            Assert.That(ShellAppearancePreferenceKeys.All.All(key => key.Scope == ExplorerPreferenceScope.User), Is.True);
        });
    }

    [Test]
    public async Task Without_a_preference_contract_choices_apply_and_are_not_remembered()
    {
        var applier = new RecordingAppearanceApplier();
        using var appearance = new ShellAppearance(applier);

        await appearance.EnsureLoadedAsync();
        await appearance.SetThemeAsync(ShellTheme.Board);
        await appearance.SetContrastAsync(ShellContrast.More);
        await appearance.SetDensityAsync(LtDensity.Compact);
        await appearance.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.IsLoaded, Is.True);
            Assert.That(applier.Applied, Is.EqualTo(new[]
            {
                (ShellTheme.System, ShellContrast.System, LtDensity.Comfortable),
                (ShellTheme.Board, ShellContrast.System, LtDensity.Comfortable),
                (ShellTheme.Board, ShellContrast.More, LtDensity.Comfortable),
                (ShellTheme.Board, ShellContrast.More, LtDensity.Compact),
            }));
            Assert.That(() => new ShellAppearance(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task With_the_preference_contract_the_choices_are_restored_and_remembered()
    {
        var catalog = new ExplorerPreferenceCatalog();
        var preferences = Substitute.For<IExplorerShellPreferences>();
        preferences.IsLoaded.Returns(true);
        preferences.RestoreAsync(ShellAppearancePreferenceKeys.Theme, Arg.Any<string>(), Arg.Any<byte>(), Arg.Any<Func<string, byte, bool>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ExplorerPreferenceResolution<string>.Restored("dark")));
        preferences.RestoreAsync(ShellAppearancePreferenceKeys.Contrast, Arg.Any<string>(), Arg.Any<byte>(), Arg.Any<Func<string, byte, bool>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ExplorerPreferenceResolution<string>.Restored("more")));
        preferences.RestoreAsync(ShellAppearancePreferenceKeys.Density, Arg.Any<string>(), Arg.Any<byte>(), Arg.Any<Func<string, byte, bool>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(ExplorerPreferenceResolution<string>.Restored("cosy")));
        var applier = new RecordingAppearanceApplier();
        using var appearance = new ShellAppearance(applier, preferences, catalog);

        await appearance.EnsureLoadedAsync();
        await appearance.SetThemeAsync(ShellTheme.Paper);

        Assert.Multiple(async () =>
        {
            Assert.That(ShellAppearancePreferenceKeys.All.All(catalog.Contains), Is.True, "the keys are registered on the contract");
            Assert.That(applier.Applied[0], Is.EqualTo((ShellTheme.Board, ShellContrast.More, LtDensity.Comfortable)));
            await preferences.Received(1).SetAsync(ShellAppearancePreferenceKeys.Theme, "light", Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task Before_the_store_can_be_read_nothing_is_applied_and_a_later_load_retries()
    {
        var preferences = Substitute.For<IExplorerShellPreferences>();
        preferences.IsLoaded.Returns(false);
        var applier = new RecordingAppearanceApplier();
        using var appearance = new ShellAppearance(applier, preferences, new ExplorerPreferenceCatalog());

        await appearance.EnsureLoadedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(appearance.IsLoaded, Is.False);
            Assert.That(applier.Applied, Is.Empty);
        });
    }

    [Test]
    public void A_change_of_scope_re_reads_and_re_applies()
    {
        var preferences = Substitute.For<IExplorerShellPreferences>();
        preferences.GetOrDefault(ShellAppearancePreferenceKeys.Theme, Arg.Any<string>()).Returns("dark");
        preferences.GetOrDefault(ShellAppearancePreferenceKeys.Contrast, Arg.Any<string>()).Returns("standard");
        preferences.GetOrDefault(ShellAppearancePreferenceKeys.Density, Arg.Any<string>()).Returns("compact");
        var applier = new RecordingAppearanceApplier();
        var appearance = new ShellAppearance(applier, preferences, new ExplorerPreferenceCatalog());
        var raised = 0;
        appearance.Changed += () => raised++;

        preferences.Changed += Raise.Event<Action>();

        Assert.Multiple(() =>
        {
            Assert.That((appearance.Theme, appearance.Contrast, appearance.Density), Is.EqualTo((ShellTheme.Board, ShellContrast.Standard, LtDensity.Compact)));
            Assert.That(applier.Applied.Single(), Is.EqualTo((ShellTheme.Board, ShellContrast.Standard, LtDensity.Compact)));
            Assert.That(raised, Is.EqualTo(1));
        });

        appearance.Dispose();
        preferences.Changed += Raise.Event<Action>();
        Assert.That(raised, Is.EqualTo(1), "a disposed appearance stops listening");
    }

    [Test]
    public void The_menu_is_a_disclosure_that_Escape_closes()
    {
        var cut = Render<AppearanceMenu>();
        var toggle = cut.Find("button");
        Assert.That(toggle.GetAttribute("aria-expanded"), Is.EqualTo("false"));

        toggle.Click();

        var panel = cut.Find(".lt-shell-menu");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find("button").GetAttribute("aria-controls"), Is.EqualTo(panel.Id));
            Assert.That(panel.QuerySelectorAll("[role='group'] [role='group']"), Has.Length.EqualTo(3));
        });

        panel.KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(cut.FindAll(".lt-shell-menu"), Is.Empty);
    }

    [Test]
    public void Escape_in_the_menu_survives_a_refused_focus()
    {
        RefuseEveryFocus();
        var cut = Render<AppearanceMenu>();
        cut.Find("button").Click();

        cut.Find(".lt-shell-menu").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        // Still answering: it closed, and it opens again.
        Assert.That(cut.FindAll(".lt-shell-menu"), Is.Empty);
        cut.Find("button").Click();
        Assert.That(cut.FindAll(".lt-shell-menu"), Has.Count.EqualTo(1));
    }

    // The browser refuses a focus whose element a later render removed. Both routes a
    // chrome focus can take are refused: Blazor's own, and the chrome module's.
    private void RefuseEveryFocus()
    {
        var refused = new JSException("Unable to focus an invalid element.");
        JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetException(refused);
        JSInterop.SetupModule(ShellChromeAssets.ModuleSpecifier).SetupVoid("focusElement", _ => true).SetException(refused);
    }

    [Test]
    public void The_menu_names_the_material_in_force()
    {
        var cut = Render<AppearanceMenu>();
        cut.Find("button").Click();

        cut.Find("[data-lt-command='appearance.theme.paper']").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").TextContent.Trim(), Is.EqualTo("Paper"));
            Assert.That(cut.Find("button").GetAttribute("aria-label"), Is.EqualTo("Appearance: Paper"));
        });
    }

    [Test]
    public void Each_choice_is_a_pressed_toggle_that_applies_at_once()
    {
        var cut = Render<AppearanceControls>();

        cut.Find("[data-lt-command='appearance.theme.board']").Click();
        cut.Find("[data-lt-command='appearance.contrast.more']").Click();
        cut.Find("[data-lt-command='appearance.density.compact']").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[aria-pressed='true']").Select(button => button.GetAttribute("data-lt-command")),
                Is.EqualTo(new[] { "appearance.theme.board", "appearance.contrast.more", "appearance.density.compact" }));
            Assert.That(Applier.Applied.Last(), Is.EqualTo((ShellTheme.Board, ShellContrast.More, LtDensity.Compact)));
            Assert.That(cut.FindAll("[role='group'][aria-labelledby]"), Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void Every_chrome_command_has_a_visible_control()
    {
        AddArea(new FakeArea("data", "Data"));
        var location = new ExplorerLocation(
            ExplorerAddress.Home,
            [new ExplorerAreaEntry(new FakeArea("data", "Data"), AreaAvailability.Visible)],
            EntriesLoaded: true,
            TenancyActive: false);
        var commands = ChromeCommands.Build(location, Services.GetRequiredService<ShellAppearance>());

        var spine = Render<DirectorySpine>(parameters => parameters.AddCascadingValue(location));
        var menu = Render<AppearanceMenu>();
        ExplorerCommandControls.AssertVisibleControl(menu, new ExplorerCommand(ChromeCommands.AppearanceMenuId, "Open the appearance menu"));
        menu.Find("button").Click();

        Assert.That(commands, Has.Count.EqualTo(10));
        foreach (var command in commands)
        {
            if (command.Id.StartsWith("go.", StringComparison.Ordinal))
            {
                ExplorerCommandControls.AssertVisibleControl(spine, command);
            }
            else
            {
                ExplorerCommandControls.AssertVisibleControl(menu, command);
            }
        }
    }

    [Test]
    public async Task The_appearance_commands_set_what_their_controls_set()
    {
        var appearance = Services.GetRequiredService<ShellAppearance>();
        var commands = ChromeCommands.Build(ExplorerLocation.Initial, appearance).ToDictionary(command => command.Id);

        await commands["appearance.theme.board"].InvokeAsync!(CancellationToken.None);
        await commands["appearance.contrast.standard"].InvokeAsync!(CancellationToken.None);
        await commands["appearance.density.compact"].InvokeAsync!(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That((appearance.Theme, appearance.Contrast, appearance.Density), Is.EqualTo((ShellTheme.Board, ShellContrast.Standard, LtDensity.Compact)));
            Assert.That(commands["go.home"].Target, Is.EqualTo(ExplorerAddress.Home));
            Assert.That(() => ChromeCommands.Build(null!, appearance), Throws.ArgumentNullException);
            Assert.That(() => ChromeCommands.Build(ExplorerLocation.Initial, null!), Throws.ArgumentNullException);
        });
    }
}
