using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The base of every routable Explorer page: it declares the route parameters
/// the address grammar uses, and gives the page its canonical
/// <see cref="Address"/>.
/// </summary>
/// <remarks>
/// <para>
/// A page declares its routes in lower case, beginning with a literal segment,
/// both plain and tenant-rooted, with optional parameters rather than a
/// catch-all, and inherits this class so it needs no route parameters of its own:
/// </para>
/// <code>
/// @page "/data"
/// @page "/data/{p1}/{p2?}/{p3?}"
/// @page "/t/{tenant}/data"
/// @page "/t/{tenant}/data/{p1}/{p2?}/{p3?}"
/// @inherits ExplorerPage
/// </code>
/// <para>
/// The route parameters exist only so the router can bind; a page reads where it
/// is from <see cref="Address"/>, which the layout has already made canonical for
/// the caller's tenancy, and never from the parameters.
/// </para>
/// </remarks>
public abstract class ExplorerPage : ComponentBase
{
    /// <summary>The <c>{tenant}</c> route parameter. Read <c>Address.Tenant</c> instead.</summary>
    [Parameter]
    public string? Tenant { get; set; }

    /// <summary>The <c>{p1}</c> route parameter. Read <c>Address.Path</c> instead.</summary>
    [Parameter]
    public string? P1 { get; set; }

    /// <summary>The <c>{p2}</c> route parameter. Read <c>Address.Path</c> instead.</summary>
    [Parameter]
    public string? P2 { get; set; }

    /// <summary>The <c>{p3}</c> route parameter. Read <c>Address.Path</c> instead.</summary>
    [Parameter]
    public string? P3 { get; set; }

    /// <summary>The <c>{p4}</c> route parameter. Read <c>Address.Path</c> instead.</summary>
    [Parameter]
    public string? P4 { get; set; }

    /// <summary>The <c>{p5}</c> route parameter. Read <c>Address.Path</c> instead.</summary>
    [Parameter]
    public string? P5 { get; set; }

    /// <summary>The <c>{p6}</c> route parameter. Read <c>Address.Path</c> instead.</summary>
    [Parameter]
    public string? P6 { get; set; }

    /// <summary>Where the user is, cascaded by the layout.</summary>
    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    /// <summary>The navigator, for moving to another address.</summary>
    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    /// <summary>
    /// The page's canonical address: the layout's, or, outside the layout, the
    /// browser's URL read directly.
    /// </summary>
    internal ExplorerAddress Address => Location?.Address ?? Navigator.Current ?? ExplorerAddress.Home;

    /// <summary>
    /// Whether the page accepts a location at any address rather than only at one
    /// of its own routes. Only a page the head renders for every address - the
    /// not-found page - says yes.
    /// </summary>
    private protected virtual bool AnswersEveryAddress => false;

    /// <summary>
    /// Sets the page's parameters, unless the cascaded location is at an address
    /// none of the page's routes answers: then the page is left exactly as it was,
    /// and neither reads nor acts on that address.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The layout's location reaches the page through a cascade, and a navigation
    /// updates that cascade before the page being left is torn down, so the
    /// outgoing page is handed the next page's address. A page that read it would
    /// load, or declare not found, an address that is not its own - "my tenant"
    /// reading <c>/access</c> as a tenant with no id declared <c>/access</c> not
    /// found. The page entered is likewise rendered once with the previous page's
    /// location before the layout has resolved the new one; it waits for its own.
    /// </para>
    /// <para>
    /// Either way the page's own address arrives with the layout's next location,
    /// and the page takes it then. A page rendered outside the layout, with no
    /// location, is not affected.
    /// </para>
    /// </remarks>
    /// <param name="parameters">The parameters, including the cascaded location.</param>
    public override Task SetParametersAsync(ParameterView parameters)
    {
        if (!AnswersEveryAddress
            && parameters.TryGetValue<ExplorerLocation>(nameof(Location), out var location)
            && location is not null
            && !ExplorerPageRoutes.For(GetType()).Answers(location.Address))
        {
            return Task.CompletedTask;
        }

        return base.SetParametersAsync(parameters);
    }
}
