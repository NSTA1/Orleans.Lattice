using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An in-memory wiring of <see cref="AppActivationEngine"/> over a real <see cref="AppRegistry"/>
/// on an in-memory store, with fake source, rule store, tree provisioner, and status store, so
/// activation is exercised end to end without a cluster.
/// </summary>
internal sealed class ActivationHarness
{
    public static readonly AppSlug Slug = AppRegistryTestData.Slug;
    public static readonly AppVersion V1 = AppRegistryTestData.V1;
    public static readonly AppVersion V2 = AppRegistryTestData.V2;
    public static readonly LatticeOperation ReadWrite = LatticeOperation.Read | LatticeOperation.Write;

    public ActivationHarness(bool withMembership = true, bool withPolicyStore = true, ILatticeMembershipContext? membership = null)
    {
        Registry = AppRegistryTestData.CreateRegistry(RegistryStore);
        Engine = new AppActivationEngine(
            Registry,
            Source,
            Status,
            Trees,
            NullLogger<AppActivationEngine>.Instance,
            withPolicyStore ? Rules : null,
            membership ?? (withMembership ? new FixedMembershipContext() : null),
            Time);
    }

    public InMemoryAppRegistryStore RegistryStore { get; } = new();

    public AppRegistry Registry { get; }

    public ActivationAppSource Source { get; } = new();

    public InMemoryPolicyStore Rules { get; } = new();

    public RecordingTreeProvisioner Trees { get; } = new();

    public InMemoryActivationStatusStore Status { get; } = new();

    public ManualTimeProvider Time { get; } = new(AppRegistryTestData.Start);

    public AppActivationEngine Engine { get; }

    public Task<AppActivationOutcome> RunAsync(AppActivationOperation operation, TenantId? tenant = null, AppSlug? slug = null) =>
        Engine.ExecuteAsync(operation, tenant ?? TenantId.Default, slug ?? Slug, CancellationToken.None);

    public async Task<AppRegistryRecord> InstallAsync(
        AppManifest manifest,
        TenantId? tenant = null,
        AppCapabilityCeiling? ceiling = null,
        IReadOnlyList<AppRoleBinding>? bindings = null)
    {
        Source.Publish(manifest);
        var result = await Registry.InstallAsync(new AppRegistryInstallRequest
        {
            Tenant = tenant ?? TenantId.Default,
            Identity = manifest.Identity,
            Ceiling = ceiling ?? AppCapabilityCeiling.Structural(ReadWrite),
            RoleBindings = bindings ?? new[] { AppRoleBinding.Create("reader", "readers") },
        });
        Assert.That(result.Succeeded, Is.True, result.Message);
        return result.Record!;
    }

    public async Task<AppRegistryRecord> UpgradeAsync(
        AppManifest manifest,
        TenantId? tenant = null,
        AppCapabilityCeiling? ceiling = null,
        IReadOnlyList<AppRoleBinding>? bindings = null)
    {
        Source.Publish(manifest);
        var result = await Registry.UpgradeAsync(new AppRegistryInstallRequest
        {
            Tenant = tenant ?? TenantId.Default,
            Identity = manifest.Identity,
            Ceiling = ceiling ?? AppCapabilityCeiling.Structural(ReadWrite),
            RoleBindings = bindings ?? new[] { AppRoleBinding.Create("reader", "readers") },
        });
        Assert.That(result.Succeeded, Is.True, result.Message);
        return result.Record!;
    }

    public static AppManifest Manifest(
        AppVersion? version = null,
        AppSlug? slug = null,
        AppTreeDeclaration[]? trees = null,
        AppRoleDeclaration[]? roles = null) => new()
    {
        Identity = new AppIdentity { Slug = slug ?? Slug, Version = version ?? V1 },
        Trees = trees ?? new[] { Tree("records") },
        Roles = roles ?? new[] { Role("reader", LatticeOperation.Read, "records") },
        Subscriptions = Array.Empty<AppSubscriptionDeclaration>(),
        McpTools = Array.Empty<AppMcpToolDeclaration>(),
    };

    public static AppTreeDeclaration Tree(string name, string? adopted = null, int? virtualShards = null, TimeSpan? softDelete = null) =>
        new() { Name = name, AdoptedTreeId = adopted, VirtualShardCount = virtualShards, SoftDeleteDuration = softDelete };

    public static AppRoleDeclaration Role(string name, LatticeOperation operations, params string[] trees) => new()
    {
        Name = name,
        Operations = operations,
        Scopes = trees.Select(tree => new AppScopeTemplate { Tree = tree }).ToArray(),
    };

    public string[] OwnedRuleIds(TenantId? tenant = null) => Rules.Rules
        .Where(rule => LatticeAppRuleIds.IsAppOwned(rule.RuleId)
            && AppActivationTreeNames.BelongsToTenant(rule.Scope.TreeId, tenant ?? TenantId.Default))
        .Select(rule => rule.RuleId)
        .ToArray();
}
