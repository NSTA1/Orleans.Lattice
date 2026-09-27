using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The fail-closed classification of which tenant-local tree ids an app may ever be granted or
/// may ever observe outside its own structural namespace, shared by the role compiler, the
/// subscription compiler and the manifest validator so the three can never disagree.
/// </summary>
/// <remarks>
/// <para>
/// An out-of-namespace scope (an adopted tree or another app's tree) needs an operator-approved
/// ceiling exception, but no exception can make one of these ids grantable: the cluster-wide
/// capability sentinel <see cref="LatticeScope.ClusterWideTreeId"/> (a rule on it is a
/// cluster-wide or all-trees grant, not a tree grant), the reserved core namespace
/// <c>_lattice_</c>, the system-data namespace <c>sys-</c> (the policy, tenant and app registries
/// among it), and an already tenant-qualified <c>t/</c> id (which tenant composition passes
/// through uncomposed, so it would name another tenant's tree). The compilers are the single
/// seam every app grant and observation funnels through, so the guard is enforced there
/// whatever the manifest validator or the caller-supplied ceiling admitted.
/// </para>
/// <para>
/// Allocation-free: ordinal prefix tests over the id.
/// </para>
/// </remarks>
internal static class AppTreeIds
{
    /// <summary>
    /// Returns <c>true</c> when <paramref name="localTreeId"/> is an ordinary data tree an
    /// approved exception may grant or observe; <c>false</c> for an empty id, the cluster-wide
    /// sentinel, and the reserved, system-data and tenant-qualified namespaces.
    /// </summary>
    /// <param name="localTreeId">A tenant-local (pre-composition) tree id.</param>
    /// <returns>Whether the id may be granted through an exception.</returns>
    internal static bool IsGrantable(string? localTreeId) =>
        !string.IsNullOrEmpty(localTreeId)
        && !string.Equals(localTreeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal)
        && !localTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        && !localTreeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
        && !localTreeId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal);

    /// <summary>
    /// Returns <c>true</c> when <paramref name="adoptedTreeId"/> may be declared as an adopted
    /// physical tree: grantable (see <see cref="IsGrantable"/>), outside the structural app
    /// namespace <c>a/</c>, at most <see cref="AppManifestLimits.MaxTextLength"/> characters, and
    /// free of leading or trailing white space and of control characters, so the id an operator
    /// approves is exactly the id the grant names.
    /// </summary>
    /// <param name="adoptedTreeId">The declared adopted tree id.</param>
    /// <returns>Whether the id is a valid adoption target.</returns>
    internal static bool IsAdoptable(string? adoptedTreeId)
    {
        if (!IsGrantable(adoptedTreeId)
            || adoptedTreeId!.Length > AppManifestLimits.MaxTextLength
            || adoptedTreeId.StartsWith(LatticeConstants.AppTreePrefix, StringComparison.Ordinal)
            || char.IsWhiteSpace(adoptedTreeId[0])
            || char.IsWhiteSpace(adoptedTreeId[^1]))
        {
            return false;
        }

        foreach (var c in adoptedTreeId)
        {
            if (char.IsControl(c))
            {
                return false;
            }
        }

        return true;
    }
}
