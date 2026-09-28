using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;

namespace Orleans.Lattice.Explorer.Shell.Design.Slots;

/// <summary>
/// Renders every component contributed to one chrome slot, in
/// <see cref="IShellSlot.Order"/> order. An empty slot renders nothing at all,
/// not an empty wrapper, so the layout owns every element around it.
/// </summary>
/// <remarks>
/// Written in C# rather than Razor so it can stay <see langword="internal"/>:
/// the Razor compiler always emits a public class.
/// </remarks>
internal sealed class ShellSlotOutlet : ComponentBase
{
    private IShellSlot[] _contributions = [];

    /// <summary>The slot to render: one of <see cref="ShellSlotNames"/>.</summary>
    [Parameter, EditorRequired]
    public string Name { get; set; } = string.Empty;

    [Inject]
    private IEnumerable<IShellSlot> Slots { get; set; } = [];

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        ShellSlotNames.EnsureKnown(Name, nameof(Name));

        _contributions = Slots
            .Where(slot => string.Equals(slot.Name, Name, StringComparison.Ordinal))
            .OrderBy(slot => slot.Order)
            .ThenBy(slot => slot.ComponentType.FullName, StringComparer.Ordinal)
            .ToArray();
    }

    /// <inheritdoc />
    protected override void BuildRenderTree(RenderTreeBuilder builder)
    {
        foreach (var contribution in _contributions)
        {
            builder.OpenComponent(0, contribution.ComponentType);
            builder.SetKey(contribution.ComponentType);
            builder.CloseComponent();
        }
    }
}
