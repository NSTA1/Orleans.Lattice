namespace Orleans.Lattice.Schema;

/// <summary>
/// Resolves a registered <see cref="ILatticeValueTransform"/> by its stable id.
/// A consumer that has persisted only a transform id uses this to recover the
/// live implementation at evaluation time.
/// </summary>
public interface ILatticeValueTransformRegistry
{
    /// <summary>
    /// Attempts to resolve the transform registered under <paramref name="id"/>.
    /// </summary>
    /// <param name="id">The stable transform id.</param>
    /// <param name="transform">The resolved transform when found; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when a transform is registered under <paramref name="id"/>; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is <c>null</c>.</exception>
    bool TryGet(string id, out ILatticeValueTransform? transform);

    /// <summary>
    /// Resolves the transform registered under <paramref name="id"/>.
    /// </summary>
    /// <param name="id">The stable transform id.</param>
    /// <returns>The registered transform.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is <c>null</c>.</exception>
    /// <exception cref="KeyNotFoundException">No transform is registered under <paramref name="id"/>.</exception>
    ILatticeValueTransform Get(string id);
}
