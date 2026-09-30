using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// Renders the page for one caller: one sign-in, at one endpoint, asserting one
/// tenant. The layout keys it on that caller (<c>ShellCallerKey</c>), so a sign-in,
/// a sign-out, a new connection or moving to another tenant disposes the page and
/// builds a fresh one, rather than handing the same component instance - and every
/// answer it holds in its fields - to the next caller or new route parameters.
/// </summary>
internal sealed class ShellTenantBoundary : ComponentBase
{
    /// <summary>The page.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <inheritdoc />
    protected override void BuildRenderTree(RenderTreeBuilder builder) => builder.AddContent(0, ChildContent);
}
