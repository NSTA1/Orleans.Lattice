namespace Orleans.Lattice.Explorer.Shell.Design.Tokens;

/// <summary>The two Operate densities: how tightly rows and controls are set.</summary>
internal enum LtDensity
{
    /// <summary>The default: 36px rows and controls.</summary>
    Comfortable,

    /// <summary>28px rows and controls, still above the 24px WCAG 2.2 minimum target size.</summary>
    Compact,
}
