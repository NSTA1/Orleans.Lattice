using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A key/value list - the properties of one object - set as booktabs rows: a
/// term in secondary ink, its value in ink, and a hairline between rows.
/// </summary>
public partial class LtDefinitionList
{
    /// <summary>The rows: <see cref="LtDefinition"/> components.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }
}
