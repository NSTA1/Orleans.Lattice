namespace Orleans.Lattice.Explorer.Plugins.Selection;

/// <summary>
/// Resolves the view registered for a nested per-selection surface id, so a
/// hosting surface can render it without referencing the package that
/// contributed it.
/// </summary>
public interface ISelectionNestedSurfaceRegistry
{
    /// <summary>
    /// The view registered under <paramref name="surfaceId"/>, or
    /// <see langword="null"/> when the contributing package was not registered.
    /// A hosting surface treats <see langword="null"/> as "offer no affordance",
    /// which is what makes withholding the package a complete opt-out.
    /// </summary>
    /// <param name="surfaceId">The stable nested-surface id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="surfaceId"/> is <see langword="null"/>.</exception>
    Type? Find(string surfaceId);
}
