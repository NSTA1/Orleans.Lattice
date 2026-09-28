using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Layout.Appearance;

namespace Orleans.Lattice.Explorer.Shell.Layout;

/// <summary>
/// The header's appearance menu: a disclosure button, named for the material in
/// force, that shows the <see cref="AppearanceControls"/>.
/// </summary>
/// <remarks>
/// The button reports <c>aria-expanded</c>; Escape closes the panel and returns
/// focus to the button. The panel floats on a hairline rather than a shadow.
/// </remarks>
public partial class AppearanceMenu : IDisposable
{
    private readonly string _panelId = LtIds.Next("lt-shell-appearance-menu");
    private ElementReference _toggle;
    private bool _open;

    [Inject]
    internal ShellAppearance Appearance { get; set; } = default!;

    private string Label => Appearance.Theme switch
    {
        ShellTheme.Paper => "Paper",
        ShellTheme.Board => "Board",
        _ => "Appearance",
    };

    private string? AccessibleName => Appearance.Theme == ShellTheme.System ? null : "Appearance: " + Label;

    /// <summary>Stops listening to the appearance state.</summary>
    public void Dispose() => Appearance.Changed -= OnAppearanceChanged;

    /// <inheritdoc />
    protected override void OnInitialized() => Appearance.Changed += OnAppearanceChanged;

    private void Toggle() => _open = !_open;

    private async Task OnKeyDownAsync(KeyboardEventArgs args)
    {
        if (args.Key == "Escape")
        {
            _open = false;
            await _toggle.FocusAsync();
        }
    }

    private void OnAppearanceChanged() => _ = InvokeAsync(StateHasChanged);
}
