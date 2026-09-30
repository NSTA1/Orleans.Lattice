using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Layout.Appearance;

/// <summary>Puts an appearance on the document: the seam between the appearance state and the browser.</summary>
internal interface IShellAppearanceApplier
{
    /// <summary>
    /// Applies the appearance to the document element. Best effort: an applier
    /// that cannot reach a document (a prerender, a test) does nothing and does not throw.
    /// </summary>
    /// <param name="theme">The material.</param>
    /// <param name="contrast">The contrast overlay.</param>
    /// <param name="density">The density.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    ValueTask ApplyAsync(ShellTheme theme, ShellContrast contrast, LtDensity density, CancellationToken cancellationToken = default);
}
