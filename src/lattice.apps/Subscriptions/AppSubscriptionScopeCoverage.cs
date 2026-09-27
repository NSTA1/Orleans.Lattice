using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Decides whether an install ceiling's operator-approved exception scopes cover an
/// out-of-namespace subscription scope.
/// </summary>
/// <remarks>
/// This mirrors the private <c>AppRoleCompiler.IsCovered</c> exactly, so a subscription and a
/// role scope are judged by one coverage rule: both scopes are tenant-local (pre-composition),
/// they must name the same tree (ordinal), a tree exception covers any scope, a prefix exception
/// covers a key or prefix scope starting with it, and a key exception covers only the identical
/// key scope. <see cref="LatticeScope.ClusterWideTreeId"/> is not a wildcard. Keep the two in step.
/// </remarks>
internal static class AppSubscriptionScopeCoverage
{
    /// <summary>Returns <c>true</c> when any of <paramref name="exceptions"/> covers <paramref name="requested"/>.</summary>
    /// <param name="requested">The tenant-local scope the subscription observes.</param>
    /// <param name="exceptions">The ceiling's approved exception scopes; <c>null</c> entries are ignored.</param>
    /// <returns><c>true</c> when the scope is covered.</returns>
    internal static bool IsCovered(LatticeScope requested, IReadOnlyList<LatticeScope> exceptions)
    {
        foreach (var exception in exceptions)
        {
            if (exception is null || !string.Equals(exception.TreeId, requested.TreeId, StringComparison.Ordinal))
                continue;
            var covered = exception.Kind switch
            {
                LatticeScopeKind.Tree => true,
                LatticeScopeKind.Prefix => requested.Kind != LatticeScopeKind.Tree
                    && requested.KeyOrPrefix!.StartsWith(exception.KeyOrPrefix!, StringComparison.Ordinal),
                LatticeScopeKind.Key => requested.Kind == LatticeScopeKind.Key
                    && string.Equals(requested.KeyOrPrefix, exception.KeyOrPrefix, StringComparison.Ordinal),
                _ => false,
            };
            if (covered)
                return true;
        }

        return false;
    }
}
