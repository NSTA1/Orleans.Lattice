namespace Orleans.Lattice.Explorer.Shell.Design.Tokens;

/// <summary>The three named widths the Shell lays out for.</summary>
internal enum LtBreakpoint
{
    /// <summary>Narrower than <see cref="LtBreakpoints.MediumMinimumWidth"/>: the spine becomes a panel.</summary>
    Compact,

    /// <summary>From <see cref="LtBreakpoints.MediumMinimumWidth"/> up to <see cref="LtBreakpoints.ExpandedMinimumWidth"/>.</summary>
    Medium,

    /// <summary><see cref="LtBreakpoints.ExpandedMinimumWidth"/> and wider.</summary>
    Expanded,
}
