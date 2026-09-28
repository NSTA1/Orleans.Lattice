using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Resolves a manifest's change-feed subscriptions for one install and checks each against the
/// install's capability ceiling. A pure function with no I/O; the subscription runtime calls it when
/// it activates an enabled app, and activation code can call it to fail an app early.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tree resolution</b> matches <see cref="AppRoleCompiler"/>: a subscription naming another app
/// resolves to <c>a/{otherApp}/{tree}</c>; otherwise to the declaration's
/// <see cref="AppTreeDeclaration.AdoptedTreeId"/> when set; otherwise to the structural
/// <c>a/{app}/{tree}</c>. The resolved id is composed with the install's tenant (tenant is the outer
/// axis), so observation never crosses tenants; cross-tenant observation stays with the existing
/// cross-tenant grant machinery.
/// </para>
/// <para>
/// <b>Consent.</b> A structural own-tree subscription needs no exception, because the scope is
/// inside <c>a/{app}/</c> by construction. A cross-app subscription (and, as for roles, an adopted
/// tree) is out-of-namespace and must be covered by an
/// <see cref="AppCapabilityCeiling.ApprovedExceptionScopes"/> entry, judged by the same tenant-local
/// coverage rule the role compiler uses. The observed scope is the tree, or a prefix scope when
/// <see cref="AppSubscriptionDeclaration.KeyPrefix"/> is set. As for roles, no exception can
/// approve observing the cluster-wide sentinel, a reserved <c>_lattice_</c> or system-data
/// <c>sys-</c> tree, or a tenant-qualified <c>t/</c> id. Any denial fails the app's whole
/// subscription activation; nothing is partially activated.
/// </para>
/// <para>
/// <b>Cross-app owner.</b> A cross-app subscription additionally requires the observed app to be the
/// installed owner of the observed tree in the same tenant, as recorded by the tree ownership ledger
/// and passed in as an <see cref="AppTreeOwnerSnapshot"/>; otherwise it is denied.
/// </para>
/// </remarks>
public static class AppSubscriptionCompiler
{
    /// <summary>Compiles <paramref name="manifest"/>'s subscriptions for one install.</summary>
    /// <param name="manifest">A manifest that passed <see cref="AppManifestValidator.Validate"/>; it is not re-validated.</param>
    /// <param name="tenant">The install's tenant; <see cref="TenantId.Default"/> when tenancy is off.</param>
    /// <param name="ceiling">The install's pinned capability ceiling.</param>
    /// <param name="owners">
    /// The installed owners of the trees the manifest observes across app boundaries, normally from
    /// the tree ownership ledger; <c>null</c> means no tree has an installed owner, so every cross-app
    /// subscription is denied.
    /// </param>
    /// <returns>The resolved subscriptions, or every denial.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> or <paramref name="ceiling"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised "no tenant" value, the manifest has no valid slug,
    /// or a subscription is <c>null</c>.
    /// </exception>
    public static AppSubscriptionCompilation Compile(
        AppManifest manifest,
        TenantId tenant,
        AppCapabilityCeiling ceiling,
        AppTreeOwnerSnapshot? owners = null)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(ceiling);
        owners ??= AppTreeOwnerSnapshot.None;
        if (tenant.Value is null)
            throw new ArgumentException("An install must be attributed to a tenant.", nameof(tenant));
        var slug = manifest.Identity?.Slug ?? default;
        if (slug.Value is null)
            throw new ArgumentException("The manifest has no valid app slug.", nameof(manifest));

        var declared = manifest.Subscriptions ?? Array.Empty<AppSubscriptionDeclaration>();
        if (declared.Length == 0)
            return new(Array.Empty<AppSubscriptionContext>(), Array.Empty<AppSubscriptionDenial>());

        var exceptions = ceiling.ApprovedExceptionScopes ?? Array.Empty<LatticeScope>();
        Dictionary<string, string>? adopted = null;
        foreach (var tree in manifest.Trees ?? Array.Empty<AppTreeDeclaration>())
            if (tree?.AdoptedTreeId is { } physical)
                (adopted ??= new(StringComparer.Ordinal)).TryAdd(tree.Name, physical);

        var subscriptions = new List<AppSubscriptionContext>(declared.Length);
        List<AppSubscriptionDenial>? denials = null;
        foreach (var subscription in declared)
        {
            if (subscription is null)
                throw new ArgumentException("A subscription declaration cannot be null.", nameof(manifest));

            string localTreeId;
            AppSlug observed;
            bool structural;
            if (subscription.App is { } other && other != slug)
            {
                localTreeId = string.Concat(LatticeConstants.AppTreePrefix, other.Value, "/", subscription.Tree);
                observed = other;
                structural = false;
            }
            else if (adopted is not null && adopted.TryGetValue(subscription.Tree, out var physical))
            {
                localTreeId = physical;
                observed = slug;
                structural = false;
            }
            else
            {
                localTreeId = string.Concat(LatticeConstants.AppTreePrefix, slug.Value, "/", subscription.Tree);
                observed = slug;
                structural = true;
            }

            if (!structural)
            {
                var scope = subscription.KeyPrefix is null
                    ? new LatticeScope(LatticeScopeKind.Tree, localTreeId)
                    : new LatticeScope(LatticeScopeKind.Prefix, localTreeId, subscription.KeyPrefix);
                if (!AppTreeIds.IsGrantable(localTreeId) || !AppSubscriptionScopeCoverage.IsCovered(scope, exceptions))
                {
                    (denials ??= []).Add(new(subscription.Name, observed, scope, DenialMessage(slug, subscription, observed, scope)));
                    continue;
                }
            }

            var treeId = LatticeTenantResolution.ComposeEffectiveTreeId(tenant, localTreeId);
            if (observed != slug && !owners.IsOwnedBy(treeId, observed))
            {
                var scope = subscription.KeyPrefix is null
                    ? new LatticeScope(LatticeScopeKind.Tree, localTreeId)
                    : new LatticeScope(LatticeScopeKind.Prefix, localTreeId, subscription.KeyPrefix);
                (denials ??= []).Add(new(subscription.Name, observed, scope,
                    $"Subscription '{subscription.Name}' of app '{slug}' observes app '{observed}', which is not installed as the owner of tree '{subscription.Tree}' in this tenant."));
                continue;
            }

            subscriptions.Add(new(tenant, slug, subscription.Name, observed, subscription.Tree, localTreeId, treeId, subscription.KeyPrefix));
        }

        return denials is null
            ? new(subscriptions, Array.Empty<AppSubscriptionDenial>())
            : new(Array.Empty<AppSubscriptionContext>(), denials);
    }

    private static string DenialMessage(AppSlug slug, AppSubscriptionDeclaration subscription, AppSlug observed, LatticeScope scope)
    {
        var target = scope.KeyOrPrefix is null ? $"tree '{scope.TreeId}'" : $"prefix '{scope.KeyOrPrefix}' of tree '{scope.TreeId}'";
        return observed != slug
            ? $"Subscription '{subscription.Name}' of app '{slug}' observes app '{observed}' ({target}), which no approved exception scope in the install ceiling covers."
            : $"Subscription '{subscription.Name}' of app '{slug}' observes its adopted {target}, which no approved exception scope in the install ceiling covers.";
    }
}
