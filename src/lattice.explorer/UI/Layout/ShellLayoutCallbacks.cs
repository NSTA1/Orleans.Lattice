using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The .NET end of the chrome module's callbacks: the global shortcut that opens
/// the address line, and the width band the Shell root has been measured in.
/// </summary>
/// <remarks>
/// A separate internal type so the layout itself exposes no public
/// script-callable members.
/// </remarks>
internal sealed class ShellLayoutCallbacks
{
    private readonly Func<Task> _openAddressLine;
    private readonly Func<int, Task> _viewportBand;

    /// <summary>Creates the callbacks.</summary>
    /// <param name="openAddressLine">Opens the address line.</param>
    /// <param name="viewportBand">Receives the band index: 0 compact, 1 medium, 2 expanded.</param>
    public ShellLayoutCallbacks(Func<Task> openAddressLine, Func<int, Task> viewportBand)
    {
        ArgumentNullException.ThrowIfNull(openAddressLine);
        ArgumentNullException.ThrowIfNull(viewportBand);

        _openAddressLine = openAddressLine;
        _viewportBand = viewportBand;
    }

    /// <summary>Called when <c>/</c> or Ctrl+K is pressed.</summary>
    [JSInvokable]
    public Task OpenAddressLine() => _openAddressLine();

    /// <summary>Called when the Shell root moves into another width band.</summary>
    /// <param name="band">The band: 0 compact, 1 medium, 2 expanded.</param>
    [JSInvokable]
    public Task OnViewportBand(int band) => _viewportBand(band);
}
