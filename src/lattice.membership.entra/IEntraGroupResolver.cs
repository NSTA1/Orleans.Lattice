namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// Resolves the full set of group ids for a caller whose Entra token overflowed
/// its <c>groups</c> claim. This is the pluggable overage seam: the default
/// implementation is token-only and dependency-free, while the
/// <c>Orleans.Lattice.Membership.Entra.Graph</c> package provides a Microsoft
/// Graph-backed implementation. Registered implementations are consulted only
/// when <see cref="EntraGroupResolutionMode.ResolveOnOverage"/> is configured and
/// a token actually overflowed, so an unregistered resolver costs nothing.
/// </summary>
public interface IEntraGroupResolver
{
    /// <summary>
    /// Resolves the caller's full transitive group ids. Implementations must not
    /// throw for an ordinary "no groups" result; they return an empty collection
    /// instead.
    /// </summary>
    /// <param name="context">The resolution request identifying the caller. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved group ids; empty when the caller belongs to no groups.</returns>
    ValueTask<IReadOnlyCollection<string>> ResolveGroupsAsync(
        EntraGroupResolutionContext context,
        CancellationToken cancellationToken = default);
}
