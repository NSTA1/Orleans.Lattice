using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Shared builders for the change-feed subscription tests.</summary>
internal static class SubscriptionTestData
{
    public static readonly AppSlug Notes = AppSlug.Parse("notes");

    public static readonly AppSlug Billing = AppSlug.Parse("billing");

    public static readonly AppVersion V1 = AppVersion.Parse("1.0.0");

    public static readonly TenantId Acme = TenantId.Parse("acme");

    public static AppSubscriptionDeclaration Subscription(string name, string tree, AppSlug? app = null, string? keyPrefix = null) =>
        new() { Name = name, Tree = tree, App = app, KeyPrefix = keyPrefix };

    public static AppTreeDeclaration Tree(string name, string? adopted = null) => new() { Name = name, AdoptedTreeId = adopted };

    public static AppManifest Manifest(AppSlug slug, AppTreeDeclaration[] trees, params AppSubscriptionDeclaration[] subscriptions)
    {
        var manifest = new AppManifest
        {
            Identity = new() { Slug = slug, Version = V1 },
            Trees = trees,
            Roles = [],
            Subscriptions = subscriptions,
            McpTools = [],
        };
        var validation = AppManifestValidator.Validate(manifest);
        Assert.That(validation.IsValid, Is.True, () => string.Join("; ", validation.Errors));
        return manifest;
    }

    public static AppManifest Manifest(params AppSubscriptionDeclaration[] subscriptions) =>
        Manifest(Notes, [Tree("docs"), Tree("audit")], subscriptions);

    public static AppCapabilityCeiling Ceiling(params LatticeScope[] exceptions) =>
        AppCapabilityCeiling.Structural(LatticeOperation.Read) with { ApprovedExceptionScopes = exceptions };

    public static LatticeScope TreeException(string treeId) => new(LatticeScopeKind.Tree, treeId);

    public static AppRegistryRecord Record(
        AppSlug slug,
        AppRegistryLifecycleState state = AppRegistryLifecycleState.Enabled,
        TenantId? tenant = null,
        AppCapabilityCeiling? ceiling = null,
        long revision = 1) =>
        AppRegistryTestData.Record(state, tenant, slug, V1) with { Ceiling = ceiling ?? Ceiling(), Revision = revision };

    public static LatticeMutation Set(string treeId, string key) =>
        new() { TreeId = treeId, Kind = MutationKind.Set, Key = key, Value = [1] };
}
