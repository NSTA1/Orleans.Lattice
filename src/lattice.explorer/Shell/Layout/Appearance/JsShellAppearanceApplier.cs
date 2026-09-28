using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Layout.Appearance;

/// <summary>Applies an appearance through the chrome's JavaScript module.</summary>
internal sealed class JsShellAppearanceApplier : IShellAppearanceApplier
{
    private readonly ShellChromeInterop _interop;

    /// <summary>Creates the applier.</summary>
    /// <param name="interop">The chrome's JavaScript module.</param>
    public JsShellAppearanceApplier(ShellChromeInterop interop)
    {
        ArgumentNullException.ThrowIfNull(interop);
        _interop = interop;
    }

    /// <inheritdoc />
    public ValueTask ApplyAsync(ShellTheme theme, ShellContrast contrast, LtDensity density, CancellationToken cancellationToken = default) =>
        _interop.ApplyAppearanceAsync(
            ShellAppearanceNames.Name(theme),
            ShellAppearanceNames.Name(contrast),
            ShellAppearanceNames.Name(density));
}
