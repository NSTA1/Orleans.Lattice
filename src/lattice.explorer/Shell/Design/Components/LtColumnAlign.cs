namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>How an <see cref="LtColumn{TItem}"/>'s cells align.</summary>
public enum LtColumnAlign
{
    /// <summary>Text and identifiers: aligned to the start of the line.</summary>
    Start,

    /// <summary>Numbers: aligned to the end of the line, in tabular figures, so digits line up.</summary>
    End,
}
