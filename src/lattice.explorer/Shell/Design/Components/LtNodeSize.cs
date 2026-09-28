namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>The size of an <see cref="LtNode"/>.</summary>
public enum LtNodeSize
{
    /// <summary>A spine or chain node: 7px, or 9px for the join.</summary>
    Small,

    /// <summary>A section or empty-state node: 11px, or 13px for the join.</summary>
    Large,
}
