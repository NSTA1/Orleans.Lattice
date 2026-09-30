namespace Orleans.Lattice.Explorer.UI.Design.Slots;

/// <summary>
/// One contribution to a named chrome slot: a component the Shell layout renders
/// at a fixed place in the chrome without knowing which item supplied it.
/// </summary>
/// <remarks>
/// This is how the navigation chrome (S1, issue #3815) and the session chrome
/// (S2, issue #3816) meet without either referencing the other's files: S1's
/// layout places a <see cref="ShellSlotOutlet"/> for each name in
/// <see cref="ShellSlotNames"/>, and S2 registers the components that fill them
/// with <see cref="ShellSlotServiceCollectionExtensions.AddShellSlot{TComponent}"/>.
/// A contribution is a parameterless component; it reads what it needs from
/// dependency injection or a cascading value, never from the layout.
/// </remarks>
internal interface IShellSlot
{
    /// <summary>The slot this contribution fills: one of <see cref="ShellSlotNames"/>.</summary>
    string Name { get; }

    /// <summary>
    /// The contribution's position within its slot. Lower renders first; ties are
    /// broken by the component's full type name, so the order is stable.
    /// </summary>
    int Order { get; }

    /// <summary>The component type rendered into the slot.</summary>
    Type ComponentType { get; }
}
