using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// One component a feature has contributed to a region of the shell's banner.
/// </summary>
/// <remarks>
/// The shell renders it through <c>DynamicComponent</c>, so it never references
/// the contributing type and gains no dependency on the package that supplied
/// it. The component receives no parameters: a chrome contribution is
/// self-contained and takes what it needs from the container.
/// </remarks>
public sealed class ExplorerChromeSlot
{
    /// <summary>Declares a chrome contribution.</summary>
    /// <param name="placement">The banner region to render into.</param>
    /// <param name="componentType">
    /// The component to render. Must implement <see cref="IComponent"/>.
    /// </param>
    /// <param name="order">
    /// The ordering hint within the placement, ascending. Contributions with
    /// equal hints keep registration order.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="componentType"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="componentType"/> does not implement <see cref="IComponent"/>.</exception>
    public ExplorerChromeSlot(ExplorerChromeSlotPlacement placement, Type componentType, int order = 0)
    {
        ArgumentNullException.ThrowIfNull(componentType);

        if (!typeof(IComponent).IsAssignableFrom(componentType))
        {
            throw new ArgumentException(
                $"A chrome slot must name a component; '{componentType.FullName}' does not implement IComponent.",
                nameof(componentType));
        }

        Placement = placement;
        ComponentType = componentType;
        Order = order;
    }

    /// <summary>The banner region this contribution renders into.</summary>
    public ExplorerChromeSlotPlacement Placement { get; }

    /// <summary>The component type the shell renders.</summary>
    public Type ComponentType { get; }

    /// <summary>The ordering hint within the placement, ascending.</summary>
    public int Order { get; }
}
