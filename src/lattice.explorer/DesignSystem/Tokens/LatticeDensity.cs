namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// The Explorer's density scale: how much padding and row height a surface
/// spends per unit of information. Density is a token like any other, so a
/// surface opts into a density by name and the token layer supplies the
/// concrete spacing.
/// </summary>
/// <remarks>
/// The breakpoint layer already picks a default density per breakpoint
/// (comfortable at compact, where touch targets dominate; compact at expanded,
/// where information density dominates). A surface only sets a density
/// explicitly when it wants to override that default.
/// </remarks>
public enum LatticeDensity
{
    /// <summary>
    /// Roomy: the largest padding and row height. The default at
    /// <see cref="LatticeBreakpoint.Compact"/>, where every row is also a touch
    /// target.
    /// </summary>
    Comfortable = 0,

    /// <summary>
    /// The Explorer's standard density, matching the current desktop surfaces.
    /// </summary>
    Cosy = 1,

    /// <summary>
    /// The tightest density, for dense technical tables where a reader is
    /// scanning many rows at once.
    /// </summary>
    Compact = 2,
}

/// <summary>
/// Maps <see cref="LatticeDensity"/> onto the stable names the token layer keys
/// off. Every member allocates nothing.
/// </summary>
public static class LatticeDensities
{
    /// <summary>The stable name of <see cref="LatticeDensity.Comfortable"/>.</summary>
    public const string ComfortableName = "comfortable";

    /// <summary>The stable name of <see cref="LatticeDensity.Cosy"/>.</summary>
    public const string CosyName = "cosy";

    /// <summary>The stable name of <see cref="LatticeDensity.Compact"/>.</summary>
    public const string CompactName = "compact";

    private static readonly LatticeDensity[] AllOrdered =
    [
        LatticeDensity.Comfortable,
        LatticeDensity.Cosy,
        LatticeDensity.Compact,
    ];

    /// <summary>Every density, ordered roomiest first.</summary>
    public static IReadOnlyList<LatticeDensity> All => AllOrdered;

    /// <summary>
    /// The stable lowercase name of <paramref name="density"/>, as used by the
    /// <c>data-lx-density</c> attribute and the token-layer selectors. Returns
    /// an interned literal, so this allocates nothing.
    /// </summary>
    /// <param name="density">The density to name.</param>
    /// <returns>The density's stable name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="density"/> is not a declared density.
    /// </exception>
    public static string Name(LatticeDensity density) => density switch
    {
        LatticeDensity.Comfortable => ComfortableName,
        LatticeDensity.Cosy => CosyName,
        LatticeDensity.Compact => CompactName,
        _ => throw new ArgumentOutOfRangeException(nameof(density), density, "Unknown density."),
    };

    /// <summary>
    /// Parses a stable density name produced by <see cref="Name"/>. Matching is
    /// ordinal and case-insensitive.
    /// </summary>
    /// <param name="name">The density name to parse.</param>
    /// <param name="density">The parsed density when parsing succeeds.</param>
    /// <returns><see langword="true"/> when <paramref name="name"/> is a known density name.</returns>
    public static bool TryParseName(string? name, out LatticeDensity density)
    {
        if (string.Equals(name, ComfortableName, StringComparison.OrdinalIgnoreCase))
        {
            density = LatticeDensity.Comfortable;
            return true;
        }

        if (string.Equals(name, CosyName, StringComparison.OrdinalIgnoreCase))
        {
            density = LatticeDensity.Cosy;
            return true;
        }

        if (string.Equals(name, CompactName, StringComparison.OrdinalIgnoreCase))
        {
            density = LatticeDensity.Compact;
            return true;
        }

        density = LatticeDensity.Cosy;
        return false;
    }
}
