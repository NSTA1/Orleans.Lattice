using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Slots;

/// <summary>A contribution of <typeparamref name="TComponent"/> to one chrome slot.</summary>
/// <typeparam name="TComponent">The component rendered into the slot.</typeparam>
internal sealed class ShellSlot<TComponent> : IShellSlot
    where TComponent : IComponent
{
    /// <summary>Creates a contribution to the slot named <paramref name="name"/>.</summary>
    /// <param name="name">One of <see cref="ShellSlotNames"/>.</param>
    /// <param name="order">The position within the slot; lower renders first.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not a declared slot.</exception>
    public ShellSlot(string name, int order = 0)
    {
        ShellSlotNames.EnsureKnown(name, nameof(name));
        Name = name;
        Order = order;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public int Order { get; }

    /// <inheritdoc />
    public Type ComponentType => typeof(TComponent);
}
