namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The components features have contributed to each region of the shell's
/// banner, resolved once and grouped by placement.
/// </summary>
public interface IExplorerChromeSlotCatalog
{
    /// <summary>
    /// The contributions for <paramref name="placement"/>, in ascending
    /// <see cref="ExplorerChromeSlot.Order"/> then registration order. Empty
    /// when nothing has been contributed there.
    /// </summary>
    /// <remarks>
    /// Called on the shell's render path, so an implementation groups at
    /// construction and returns a cached list rather than filtering per call.
    /// </remarks>
    /// <param name="placement">The banner region to enumerate.</param>
    IReadOnlyList<ExplorerChromeSlot> ForPlacement(ExplorerChromeSlotPlacement placement);
}
