using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Layout.Appearance;

namespace Orleans.Lattice.Explorer.Shell.Layout;

/// <summary>
/// The appearance choices - material, contrast and density - as three labelled
/// groups of toggle buttons, applied at once and remembered per user.
/// </summary>
public partial class AppearanceControls : IDisposable
{
    private static readonly (ShellTheme Value, string Text)[] Themes =
    [
        (ShellTheme.System, "System"),
        (ShellTheme.Paper, "Paper"),
        (ShellTheme.Board, "Board"),
    ];

    private static readonly (ShellContrast Value, string Text)[] Contrasts =
    [
        (ShellContrast.System, "System"),
        (ShellContrast.Standard, "Standard"),
        (ShellContrast.More, "More"),
    ];

    private static readonly (LtDensity Value, string Text)[] Densities =
    [
        (LtDensity.Comfortable, "Comfortable"),
        (LtDensity.Compact, "Compact"),
    ];

    private readonly string _id = LtIds.Next("lt-shell-appearance");

    [Inject]
    internal ShellAppearance Appearance { get; set; } = default!;

    /// <summary>Stops listening to the appearance state.</summary>
    public void Dispose() => Appearance.Changed -= OnAppearanceChanged;

    /// <inheritdoc />
    protected override void OnInitialized() => Appearance.Changed += OnAppearanceChanged;

    private void OnAppearanceChanged() => _ = InvokeAsync(StateHasChanged);
}
