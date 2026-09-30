namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>The visual weight of an <see cref="LtButton"/>.</summary>
public enum LtButtonVariant
{
    /// <summary>The primary action: an ink outline on the page, filled with the marker while pressed.</summary>
    Outlined,

    /// <summary>A secondary action: a strong-rule outline and secondary ink.</summary>
    Quiet,

    /// <summary>An action that destroys data: the outline and label take the danger role.</summary>
    Destructive,
}
