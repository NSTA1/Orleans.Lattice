using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

/// <summary>
/// The single place that composes, classifies, and parses materialised-view tree
/// ids, mirroring how <see cref="LatticeTenantTrees"/> centralises the reserved
/// <c>t/</c> tenant namespace. A view tree is named
/// <c>{tenantSegment}view-{name}</c>, where the tenant segment is empty for a
/// cluster-global (default-tenant) view and <c>t/{tenant}/</c> for a view a
/// tenant owns, so a view's id carries both its kind and its owner.
/// </summary>
/// <remarks>
/// <para>
/// Centralising this is what keeps <em>classification</em> correct after
/// <em>composition</em>. The view prefix is a leading-prefix marker, and scoping
/// a name into <c>t/{tenant}/</c> moves that marker off the front of the string.
/// Every ad-hoc <c>treeId.StartsWith(ViewTreePrefix)</c> therefore turns silently
/// negative for a tenant's view - retiring, among others, the guards that stop a
/// caller writing to a view tree directly. Routing every decision through
/// <see cref="IsViewTree"/> (which classifies the tenant-local name) makes the
/// answer independent of whether the id has been composed.
/// </para>
/// <para>
/// Every operation is a no-op string-wise for a non-tenant id, so a cluster with
/// tenancy off sees exactly the ids it saw before: <c>view-{name}</c> in,
/// <c>view-{name}</c> out, with the same string reference wherever possible.
/// </para>
/// </remarks>
internal static class LatticeViewTrees
{
    /// <summary>
    /// The reserved prefix (<c>view-</c>) that marks the tenant-local portion of
    /// every materialised-view tree id.
    /// </summary>
    public const string SegmentPrefix = LatticeConstants.ViewTreePrefix;

    /// <summary>
    /// Separates a view tree id from its explicit generation suffix
    /// (<c>#g{N}</c>), which a shadow-swap rebuild appends to address a
    /// non-active generation.
    /// </summary>
    public const char GenerationSeparator = '#';

    /// <summary>
    /// Composes the view tree id for <paramref name="viewName"/>:
    /// <c>orders</c> yields <c>view-orders</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This deliberately does <b>not</b> yet lift a tenant segment out of the
    /// name. A view name is currently validated only for null/empty, so a caller
    /// can create one literally named <c>t/globex/orders</c>; composing that
    /// tenant-first would yield <c>t/globex/view-orders</c> and plant the tree in
    /// another tenant's structural namespace. Tenant-aware composition therefore
    /// has to arrive together with view-name validation that reserves the
    /// <see cref="LatticeTenantTrees.SegmentPrefix"/>, not before it.
    /// </para>
    /// <para>
    /// Until then this reproduces the legacy interpolation byte for byte, so
    /// routing every call site through this helper changes no id anywhere.
    /// </para>
    /// </remarks>
    /// <param name="viewName">The view name. Must not be <c>null</c> or empty.</param>
    /// <returns>The view tree id.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    public static string ComposeTreeId(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        return string.Concat(SegmentPrefix, viewName);
    }

    /// <summary>
    /// Composes the view tree id for a specific shadow-swap generation.
    /// Generation <c>0</c> (and below) maps to the stable, unsuffixed id for
    /// backward compatibility; a higher generation appends <c>#g{N}</c>.
    /// </summary>
    /// <param name="viewName">The (possibly tenant-qualified) view name. Must not be <c>null</c> or empty.</param>
    /// <param name="generation">The generation number.</param>
    /// <returns>The generation's view tree id.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    public static string ComposeTreeId(string viewName, long generation)
    {
        var stable = ComposeTreeId(viewName);
        return generation <= 0
            ? stable
            : string.Concat(stable, GenerationSeparator.ToString(), "g", generation.ToString());
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> names a
    /// materialised-view tree, whether or not it has been tenant-composed. This
    /// is the classification every view guard and view-aware read path must use
    /// in place of a raw leading-prefix test.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the id names a view tree; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool IsViewTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // Fast path: an uncomposed view id (every id on a tenancy-off cluster)
        // answers on the leading prefix alone, with no parse and no allocation.
        if (treeId.StartsWith(SegmentPrefix, StringComparison.Ordinal))
        {
            return true;
        }

        return LatticeTenantTrees.IsTenantScoped(treeId)
            && LatticeTenantTrees.LocalName(treeId).StartsWith(SegmentPrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Recovers the view name - the maintainer grain key, tenant segment
    /// included - from a view tree id, stripping the view prefix and any
    /// <c>#g{N}</c> generation suffix. The exact inverse of
    /// <see cref="ComposeTreeId(string)"/>: <c>view-orders</c> yields
    /// <c>orders</c> and <c>t/acme/view-orders#g2</c> yields <c>t/acme/orders</c>.
    /// </summary>
    /// <remarks>
    /// Returns <see cref="string.Empty"/> when <paramref name="treeId"/> carries
    /// no recoverable name, so a caller can distinguish an unusable id from a
    /// real one rather than dialing a maintainer keyed by an empty string.
    /// </remarks>
    /// <param name="treeId">The view tree id. Must not be <c>null</c>.</param>
    /// <returns>The view name, or <see cref="string.Empty"/> when none is recoverable.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static string ViewNameFromTreeId(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        if (!IsViewTree(treeId))
        {
            return string.Empty;
        }

        var tenantScoped = LatticeTenantTrees.TryGetTenant(treeId, out var tenant);
        var local = tenantScoped ? LatticeTenantTrees.LocalName(treeId) : treeId;

        var name = local.AsSpan(SegmentPrefix.Length);
        var separator = name.IndexOf(GenerationSeparator);
        if (separator >= 0)
        {
            name = name[..separator];
        }

        if (name.IsEmpty)
        {
            return string.Empty;
        }

        return tenantScoped
            ? LatticeTenantTrees.Compose(tenant, new string(name))
            : new string(name);
    }
}
