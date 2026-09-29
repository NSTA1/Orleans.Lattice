using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.Routing;
using Orleans.Lattice.Explorer.Shell.Areas.Data;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// Stands in for the layout and router around a Data page: it reads the current
/// address from the navigation manager, cascades it as the layout does, picks the
/// directory or the tree workspace as the router would, and re-renders on every
/// navigation - keeping the same page instance, as the router does, while the
/// page type is unchanged.
/// </summary>
internal sealed class DataPageHost : ComponentBase, IDisposable
{
    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    /// <summary>Whether to cascade the compact width band.</summary>
    [Parameter]
    public bool Compact { get; set; }

    /// <summary>Whether tenancy is active, as the layout reports it.</summary>
    [Parameter]
    public bool TenancyActive { get; set; }

    /// <inheritdoc />
    public void Dispose() => Navigation.LocationChanged -= OnLocationChanged;

    /// <inheritdoc />
    protected override void OnInitialized() => Navigation.LocationChanged += OnLocationChanged;

    /// <inheritdoc />
    protected override void BuildRenderTree(RenderTreeBuilder builder)
    {
        var address = ExplorerAddress.TryFromUri(Navigation.Uri, Navigation.BaseUri, out var parsed) ? parsed : ExplorerAddress.Home;
        var location = new ExplorerLocation(address, [], EntriesLoaded: true, TenancyActive);
        var page = address.TreeId is null ? typeof(DataDirectoryPage) : typeof(DataTreePage);

        builder.OpenComponent<CascadingValue<ExplorerLocation>>(0);
        builder.AddComponentParameter(1, "Value", location);
        builder.AddComponentParameter(2, "ChildContent", (RenderFragment)(inner =>
        {
            inner.OpenComponent<CascadingValue<LtBreakpoint>>(0);
            inner.AddComponentParameter(1, "Name", LtBreakpointCascade.Name);
            inner.AddComponentParameter(2, "Value", Compact ? LtBreakpoint.Compact : LtBreakpoint.Expanded);
            inner.AddComponentParameter(3, "ChildContent", (RenderFragment)(body =>
            {
                body.OpenComponent(0, page);
                body.CloseComponent();
            }));
            inner.CloseComponent();
        }));
        builder.CloseComponent();
    }

    private void OnLocationChanged(object? sender, LocationChangedEventArgs args) => _ = InvokeAsync(StateHasChanged);
}
