using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Pages;

/// <summary>
/// The not-found page: the address that did not resolve, and the nearest
/// ancestor of it that exists for this caller.
/// </summary>
public partial class NotFoundPage
{
    private ExplorerAddress? _ancestor;

    private string RequestedText => Navigator.Current?.Format() ?? Navigator.CurrentRelativePath;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync() =>
        _ancestor = await Navigator.GetNearestValidAncestorAsync(Navigator.Current);
}
