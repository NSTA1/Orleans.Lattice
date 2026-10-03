using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// The base of every delegated tenant Access view: the tenant it is for, the
/// circuit's tenant access catalogue, and one <see cref="ComponentLifetime"/> for
/// the reads it starts. A view is rendered by the page that answers its address,
/// only once the posture probe has reported the tenant's access administration
/// delegated to the caller.
/// </summary>
public abstract class TenantAccessView : ComponentBase, IDisposable
{
    /// <summary>The tenant whose access the view administers.</summary>
    [Parameter]
    [EditorRequired]
    public string Tenant { get; set; } = string.Empty;

    /// <summary>The circuit's tenant access catalogue: the facades, and the memoised posture, groups and rules.</summary>
    [Inject]
    internal TenantAccessCatalog Access { get; set; } = default!;

    /// <summary>The navigator, for links and moves within the tenant.</summary>
    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    /// <summary>The cancellation the view's reads take: cancelled when it is left, never disposed.</summary>
    internal ComponentLifetime Lifetime { get; } = new();

    /// <inheritdoc />
    public void Dispose()
    {
        Lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    /// <summary>The link to <paramref name="address"/>, canonical for the caller's tenancy.</summary>
    /// <param name="address">A tenant-rooted Access address.</param>
    /// <returns>The href.</returns>
    private protected string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();
}
