using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U5;

/// <summary>
/// The appearance controls as U5 drives them (#5387): the header's appearance menu, or the
/// compact <b>Menu</b> sheet; what the document is drawn in; which choices the controls show
/// pressed; and what the blocking first-paint script put on the document before the circuit.
/// </summary>
/// <param name="shell">The chrome of the page to drive.</param>
public sealed class AppearancePage(ShellPage shell)
{
    private const string FirstPaintScript =
        """
        (() => {
          let written = false;
          try {
            const setItem = Storage.prototype.setItem;
            Storage.prototype.setItem = function (name, value) {
              if (name === 'orleans.lattice.explorer.appearance.v2') { written = true; }
              return setItem.apply(this, arguments);
            };
          } catch (e) { }
          document.addEventListener('DOMContentLoaded', () => {
            const root = document.documentElement;
            window.__ltU5FirstPaint = {
              theme: root.getAttribute('data-bs-theme'),
              contrast: root.getAttribute('data-lt-contrast'),
              density: root.getAttribute('data-lt-density'),
              circuitApplied: written,
            };
          }, { once: true });
        })();
        """;

    private bool _firstPaintWatched;

    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell ?? throw new ArgumentNullException(nameof(shell));

    /// <summary>The header's appearance menu button (expanded chrome only).</summary>
    public ILocator MenuToggle => Shell.Page.Locator("button[data-lt-command=\"appearance.menu\"]");

    /// <summary>The control carrying <paramref name="command"/>, wherever the controls are drawn.</summary>
    /// <param name="command">An <see cref="AppearanceCommands"/> id.</param>
    public ILocator Control(string command) => Shell.Page.Locator($"button[data-lt-command=\"{command}\"]");

    /// <summary>
    /// Asserts the document is drawn in <paramref name="appearance"/>: <c>data-bs-theme</c> holds
    /// its material, <c>data-lt-contrast</c> its contrast (absent for System), and
    /// <c>data-lt-density</c> <c>compact</c> or nothing.
    /// </summary>
    /// <param name="appearance">The appearance.</param>
    /// <param name="systemPrefersDark">Whether the browser reports a dark operating-system scheme.</param>
    public async Task ExpectAppliedAsync(ConsoleAppearance appearance, bool systemPrefersDark = false)
    {
        ArgumentNullException.ThrowIfNull(appearance);
        await Expect(Shell.Root).ToHaveAttributeAsync("data-bs-theme", appearance.Material(systemPrefersDark));
        if (appearance.Contrast == "system")
        {
            await Expect(Shell.Root).Not.ToHaveAttributeAsync("data-lt-contrast", ConsoleAppearance.AnyValue());
        }
        else
        {
            await Expect(Shell.Root).ToHaveAttributeAsync("data-lt-contrast", appearance.Contrast);
        }

        if (appearance.Density == "compact")
        {
            await Expect(Shell.Root).ToHaveAttributeAsync("data-lt-density", "compact");
        }
        else
        {
            await Expect(Shell.Root).Not.ToHaveAttributeAsync("data-lt-density", ConsoleAppearance.AnyValue());
        }
    }

    /// <summary>Makes every choice of <paramref name="appearance"/> through the appearance controls.</summary>
    /// <param name="appearance">The appearance.</param>
    public Task ChooseAsync(ConsoleAppearance appearance)
    {
        ArgumentNullException.ThrowIfNull(appearance);
        return Shell.ChooseAppearanceAsync(
            ConsoleAppearance.ThemeCommandOf(appearance.Theme),
            ConsoleAppearance.ContrastCommandOf(appearance.Contrast),
            ConsoleAppearance.DensityCommandOf(appearance.Density));
    }

    /// <summary>
    /// Opens the appearance controls - the header's appearance menu, or the <b>Menu</b> sheet
    /// when compact - and waits for every choice to be drawn.
    /// </summary>
    public async Task OpenAsync()
    {
        if (await Shell.IsCompactAsync())
        {
            await Shell.OpenMenuAsync();
        }
        else if (await MenuToggle.GetAttributeAsync("aria-expanded") != "true")
        {
            await MenuToggle.ClickAsync();
            await Expect(Shell.Page.GetByRole(AriaRole.Group, new() { Name = "Appearance" })).ToBeVisibleAsync();
        }

        await Expect(Control(AppearanceCommands.SystemTheme)).ToBeVisibleAsync();
    }

    /// <summary>Closes the appearance controls, if open.</summary>
    public async Task CloseAsync()
    {
        if (await MenuToggle.IsVisibleAsync() && await MenuToggle.GetAttributeAsync("aria-expanded") == "true")
        {
            await MenuToggle.ClickAsync();
            await Expect(MenuToggle).ToHaveAttributeAsync("aria-expanded", "false");
        }

        await Shell.CloseMenuAsync();
    }

    /// <summary>
    /// Reads, from the open appearance controls, the one choice of each kind drawn pressed, and
    /// asserts it is the only one pressed.
    /// </summary>
    /// <returns>The appearance the controls show as chosen.</returns>
    public async Task<ConsoleAppearance> ReadChosenAsync()
    {
        await OpenAsync();
        var theme = await PressedAsync(ConsoleAppearance.Themes, ConsoleAppearance.ThemeCommandOf, "theme");
        var contrast = await PressedAsync(ConsoleAppearance.Contrasts, ConsoleAppearance.ContrastCommandOf, "contrast");
        var density = await PressedAsync(ConsoleAppearance.Densities, ConsoleAppearance.DensityCommandOf, "density");
        return new ConsoleAppearance(theme, contrast, density);
    }

    /// <summary>
    /// Reloads the page and returns what the document carried at <c>DOMContentLoaded</c>: the
    /// attributes the blocking first-paint script set from the remembered record, and whether
    /// the circuit had applied an appearance yet (it must not have).
    /// </summary>
    public async Task<FirstPaint> ReloadAndReadFirstPaintAsync()
    {
        if (!_firstPaintWatched)
        {
            await Shell.Page.AddInitScriptAsync(FirstPaintScript);
            _firstPaintWatched = true;
        }

        await Shell.Page.ReloadAsync();
        await JourneyBrowser.WaitForInteractiveAsync(Shell.Page);
        return await Shell.Page.EvaluateAsync<FirstPaint>("() => window.__ltU5FirstPaint");
    }

    private async Task<string> PressedAsync(IReadOnlyList<string> choices, Func<string, string> commandOf, string kind)
    {
        var pressed = new List<string>();
        foreach (var choice in choices)
        {
            if (await Control(commandOf(choice)).GetAttributeAsync("aria-pressed") == "true")
            {
                pressed.Add(choice);
            }
        }

        Assert.That(pressed, Has.Count.EqualTo(1), $"exactly one {kind} is drawn pressed in the appearance controls");
        return pressed[0];
    }

    /// <summary>What the document carried before the circuit started.</summary>
    public sealed class FirstPaint
    {
        /// <summary>The <c>data-bs-theme</c> attribute.</summary>
        public string? Theme { get; set; }

        /// <summary>The <c>data-lt-contrast</c> attribute.</summary>
        public string? Contrast { get; set; }

        /// <summary>The <c>data-lt-density</c> attribute.</summary>
        public string? Density { get; set; }

        /// <summary>Whether the circuit had already written the appearance.</summary>
        public bool CircuitApplied { get; set; }

        /// <inheritdoc />
        public override string ToString() => $"theme={Theme ?? "(none)"} contrast={Contrast ?? "(none)"} density={Density ?? "(none)"} circuitApplied={CircuitApplied}";
    }
}
