using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Reserved tree and structural-name conventions the activation pipeline uses.
/// </summary>
internal static class AppActivationTreeNames
{
    /// <summary>
    /// The reserved tree holding one <see cref="AppActivationStatus"/> per tenant app, keyed like
    /// the registry. Under the <c>sys-app-</c> prefix, so it is control-plane read isolated,
    /// catalog-hidden, and system-origin-write only.
    /// </summary>
    internal const string StatusTree = LatticeConstants.AppRegistryTreePrefix + "activation";

    /// <summary>Composes the tenant-local structural tree name <c>a/{slug}/{tree}</c>.</summary>
    internal static string LocalStructuralTree(AppSlug slug, string tree) =>
        string.Concat(LatticeConstants.AppTreePrefix, slug.Value, "/", tree);

    /// <summary>Composes the effective (tenant-composed) structural tree id for a declared tree.</summary>
    internal static string StructuralTree(TenantId tenant, AppSlug slug, string tree) =>
        LatticeTenantResolution.ComposeEffectiveTreeId(tenant, LocalStructuralTree(slug, tree));

    /// <summary>Whether <paramref name="treeId"/> belongs to <paramref name="tenant"/>'s tree namespace.</summary>
    internal static bool BelongsToTenant(string treeId, TenantId tenant) =>
        LatticeTenantTrees.TryGetTenant(treeId, out var owner) ? owner == tenant : tenant.IsDefault;

    /// <summary>
    /// Splits a tree id of the shape <c>a/{slug}/{tree}</c> (optionally tenant-composed as
    /// <c>t/{tenant}/a/{slug}/{tree}</c>) into its app slug and local tree name.
    /// </summary>
    internal static bool TrySplitStructuralTree(string? treeId, out ReadOnlySpan<char> slug, out ReadOnlySpan<char> tree)
    {
        slug = default;
        tree = default;
        if (string.IsNullOrEmpty(treeId))
        {
            return false;
        }

        var local = LatticeTenantTrees.TryGetTenant(treeId, out _)
            ? LatticeTenantTrees.LocalName(treeId.AsSpan())
            : treeId.AsSpan();
        if (!local.StartsWith(LatticeConstants.AppTreePrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var rest = local[LatticeConstants.AppTreePrefix.Length..];
        var separator = rest.IndexOf('/');
        if (separator <= 0 || separator == rest.Length - 1)
        {
            return false;
        }

        slug = rest[..separator];
        tree = rest[(separator + 1)..];
        return true;
    }
}
