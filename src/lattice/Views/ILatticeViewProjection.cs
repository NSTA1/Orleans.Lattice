namespace Orleans.Lattice;

/// <summary>
/// Lowers a source <see cref="LatticeMutation"/> into the set of
/// <see cref="ViewWrite"/>s that maintain a materialised view. The maintainer
/// invokes <see cref="Project"/> for every user mutation it reads off the source
/// tree's write-ahead log and applies the resulting writes (after coalescing)
/// to the <c>view-{name}</c> tree.
/// <para>
/// A projection must be a <b>pure, deterministic</b> function of its input
/// mutation: the same mutation must always yield the same writes, independent of
/// call order or host, so every cluster derives an identical view from converged
/// source state. Implementations are resolved as services (never serialized), so
/// they may capture delegates and configuration.
/// </para>
/// </summary>
public interface ILatticeViewProjection
{
    /// <summary>
    /// A stable identifier for the projection's logic. Stamped into the view
    /// maintainer's durable checkpoint; a mismatch on startup signals that the
    /// projection changed and the view must be rebuilt. Convenience
    /// implementations derive this as a structural hash of their configuration.
    /// </summary>
    string ProjectionVersion { get; }

    /// <summary>
    /// Returns the view writes that <paramref name="mutation"/> produces, or an
    /// empty sequence when the mutation does not affect the view (filtered out).
    /// </summary>
    /// <param name="mutation">The committed source mutation to project.</param>
    IEnumerable<ViewWrite> Project(LatticeMutation mutation);
}
