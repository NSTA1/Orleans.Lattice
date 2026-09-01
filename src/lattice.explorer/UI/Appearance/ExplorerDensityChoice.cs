using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// How much space the operator has asked the Explorer to spend per unit of
/// information. Adds one member to <see cref="LatticeDensity"/>: the choice not
/// to choose.
/// </summary>
/// <remarks>
/// <see cref="FollowLayout"/> is the default and is what preserves the adaptive
/// behaviour the shell shipped with - the density each adaptive root derives
/// from the breakpoint it is in. An explicit density is stamped on the document
/// instead, and every adaptive root below it defers.
/// </remarks>
public enum ExplorerDensityChoice
{
    /// <summary>
    /// Let the layout decide: each adaptive root keeps the density it derives
    /// from its breakpoint. The default.
    /// </summary>
    FollowLayout = 0,

    /// <summary>Always <see cref="LatticeDensity.Comfortable"/>.</summary>
    Comfortable = 1,

    /// <summary>Always <see cref="LatticeDensity.Cosy"/>.</summary>
    Cosy = 2,

    /// <summary>Always <see cref="LatticeDensity.Compact"/>.</summary>
    Compact = 3,
}
