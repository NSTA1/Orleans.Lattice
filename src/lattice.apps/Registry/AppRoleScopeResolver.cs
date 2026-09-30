using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Resolves a manifest role's scope templates to effective tree scopes exactly as
/// <see cref="AppRoleCompiler"/> does, so a role gate - and the bridge plan built from it - names exactly the
/// trees the compiled rules name.
/// </summary>
/// <remarks>
/// A template resolves, in the tenant-local vocabulary, to <c>a/{otherApp}/{tree}</c> when it names another
/// app; otherwise to the declaration's <see cref="AppTreeDeclaration.AdoptedTreeId"/> when set; otherwise to
/// the structural <c>a/{app}/{tree}</c>. The result is then composed with the tenant through the core
/// tenant-resolution seam. A template whose composition fails closed is dropped, so it can never make a role
/// easier to hold.
/// </remarks>
internal static class AppRoleScopeResolver
{
    /// <summary>Resolves every scope template of <paramref name="role"/> for <paramref name="tenant"/>.</summary>
    /// <param name="slug">The declaring app.</param>
    /// <param name="role">The role whose scopes to resolve.</param>
    /// <param name="trees">The app's tree declarations, for adopted tree ids.</param>
    /// <param name="tenant">The install's tenant.</param>
    /// <returns>The effective scopes, in declaration order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="role"/> or <paramref name="trees"/> is null.</exception>
    public static LatticeScope[] Resolve(AppSlug slug, AppRoleDeclaration role, AppTreeDeclaration[] trees, TenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(role);
        ArgumentNullException.ThrowIfNull(trees);

        var templates = role.Scopes ?? [];
        var scopes = new List<LatticeScope>(templates.Length);
        foreach (var template in templates)
        {
            if (template is null)
                continue;

            var local = new LatticeScope(template.Kind, ResolveLocalTreeId(slug, template, trees), template.KeyOrPrefix);
            string effective;
            try
            {
                effective = LatticeTenantResolution.ComposeEffectiveTreeId(tenant, local.TreeId);
            }
            catch (LatticeTenantAccessDeniedException)
            {
                continue;
            }

            scopes.Add(ReferenceEquals(effective, local.TreeId) ? local : local with { TreeId = effective });
        }

        return scopes.ToArray();
    }

    private static string ResolveLocalTreeId(AppSlug slug, AppScopeTemplate template, AppTreeDeclaration[] trees)
    {
        if (template.App is { } app && app != slug)
            return string.Concat(LatticeConstants.AppTreePrefix, app.Value, "/", template.Tree);

        foreach (var tree in trees)
        {
            if (tree is not null
                && tree.AdoptedTreeId is { } physical
                && string.Equals(tree.Name, template.Tree, StringComparison.Ordinal))
            {
                return physical;
            }
        }

        return string.Concat(LatticeConstants.AppTreePrefix, slug.Value, "/", template.Tree);
    }
}
