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

    /// <summary>The router renders this page for any address nothing answers, and the layout for an area the caller may not see.</summary>
    private protected override bool AnswersEveryAddress => true;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync() =>
        _ancestor = await Navigator.GetNearestValidAncestorAsync(Navigator.Current);
}
