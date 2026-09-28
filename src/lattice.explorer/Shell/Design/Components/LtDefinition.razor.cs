using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>One term and its value in an <see cref="LtDefinitionList"/>.</summary>
public partial class LtDefinition
{
    /// <summary>The term, such as "Shards".</summary>
    [Parameter, EditorRequired]
    public string Term { get; set; } = string.Empty;

    /// <summary>The value.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>Whether the value is data - an id, a key, a count - and is set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; }

    private string ValueClass => Mono ? "lt-dl__value lt-dl__value--mono" : "lt-dl__value";
}
